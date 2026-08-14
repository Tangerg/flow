# Contributing

Focused issues and pull requests are welcome. Before changing behavior or an
exported API, read the package boundaries in the
[project README](./README.md#choose-the-smallest-package).

## Requirements

- Go 1.26 or newer.
- `golangci-lint` v2 (CI currently pins v2.12.2).
- `actionlint` (CI currently pins v1.7.10).
- `govulncheck` (CI currently pins v1.6.0).
- Node.js 22 or newer when changing Markdown documentation.
- A clean module graph with no committed `replace` directives.
- Tests written with the standard `testing` package.

## Development workflow

Format and run the fast local checks while iterating:

```sh
gofmt -w .
go test ./...
go vet ./...
```

Before opening a pull request, run the complete gate used by CI:

```sh
test -z "$(gofmt -l .)"
go mod tidy -diff
go test -race -coverprofile=coverage.out ./...
coverage="$(go tool cover -func=coverage.out | awk '/^total:/ { print $3 }')"
awk -v coverage="${coverage%\%}" 'BEGIN { exit coverage < 95.0 }'
go vet ./...
golangci-lint run ./...
actionlint
govulncheck ./...
npx --yes markdownlint-cli2@0.23.2
```

The coverage floor protects behavior without making every defensive statement
a public design constraint. Keep meaningful coverage as high as the tests
naturally support, but do not add white-box tests or distort production control
flow solely to preserve an exact percentage.

Changes to the learning path should also run:

```sh
go test ./example -run Example -v
```

## Design boundaries

- Keep `flow.Node` a single-method interface.
- Put general control-flow primitives in `flow`.
- Put derived combinators and decorators in `flowx`.
- Put named state and runtime definitions in `workflow`.
- Keep the expression language optional in `workflow/expr`.
- Keep definition rendering optional and derived in `workflow/diagram`.
- Keep one named-port shape for workflow data edges; unary nodes use
  `workflow.OneInput`.
- Give a concept the same name in the JSON `Spec` and in the Go config that
  builds the same step, so the two forms map mechanically. The types differ —
  `Spec.Body` is a nested `Spec` while `LoopConfig.Body` is a `Step`, and
  `Spec.Condition` is a registered name while `LoopConfig.Condition` is the node
  — but the field name should not. Naming a field after its type, as in
  `Inputs Inputs`, is preferred over inventing a synonym.
- Express a value the engine computes from a Store as `flow.Node[Store, T]`
  rather than a bespoke function type. `Step`, `Resolver`, and `Condition` are
  aliases for that shape, so they compose with the typed helpers, share one
  validation path, and need no second execution protocol. Do not add a
  parameter that the execution context already carries: a repeated boundary
  publishes its index through the scope, which `Scope` reports.
- Keep one construction shape per category of workflow step. A composite that
  contains other steps and names itself takes exactly one `Config` struct that
  owns every field, including its ID and body: `Branch`, `Loop`, `Iteration`,
  and `Subgraph`. Do not split a composite's inputs between positional
  parameters and a config, and do not alias a `flow` config as a workflow one,
  which prevents the workflow form from carrying its own fields. `Sequence`
  stays variadic because it has no settings, and a step with no children keeps
  positional parameters: `Leaf`, `Await`, `Interrupt`, `Route`. Delegate the
  meaning of a shared setting to the `flow` config that defines it rather than
  restating the rule.
- Let `Store` own every decision about its own representation. A composite must
  not read `Store.depth` or compare it against `storeOverlayLimit`; it calls the
  named `Store` method for what it is about to do. `Store.bounded` is the single
  place the overlay limit is enforced, so every path that extends an overlay
  ends there rather than restating the threshold. A composite that hands one
  Store to concurrent derivers — parallel branches, iteration elements, graph
  nodes — passes it through `Store.sharedBase` first, or each deriver flattens
  the snapshot separately and the fan-out costs one copy per deriver.
  `BenchmarkParallelBaseScaling`, `BenchmarkIterationBaseScaling`, and
  `BenchmarkGraphRunBaseScaling` vary the input overlay length so a new fan-out
  site that skips this shows up as an allocation cliff at the limit. Those measure
  cost; `TestStore_sharesOneBaseAcrossConcurrentDerivers` measures correctness, and
  it is what a flattening that decided to remember its result would fail — the race
  detector reports it writing into the snapshot every deriver is reading.
- Measure nesting from something the walk already keeps, or pass it down. The
  `jsondoc` reader reads `len(path)`, because entering a container is what
  appends a segment; `specValidator` and the `Spec` encoder each descend into a
  child holding `depth+1`; `expr`'s compiler takes it as a parameter, the way its
  own reference flattening already did. A field raised on the way in and put back
  on the way out states one fact twice, and only the second copy can be left
  raised — which quietly turns a limit on nesting into a limit on how much a
  document may hold. That failure needs a wide input to see, so a test that nests
  a single chain cannot: `TestCodec_boundsNestingNotBreadth`,
  `TestSpec_boundsNestingNotBreadth`, and `TestParse_boundsNestingNotBreadth`
  supply one per boundary.
- Adding a configurable step kind is six edits in two languages, and none of them
  is optional: the `Kind` constant, an entry in `specKindFields`, a validator
  case, a compiler case, the `definitionKind` the built step reports, and a branch
  in `jsonschema/spec.schema.json`. Three of the six are held to each other by
  `TestSpecFieldMatricesAgreeWithTheSpecStruct`, which compares the matrix, the
  schema, and the `Spec` struct field by field and kind by kind; the other three
  fail as an unknown kind rather than silently. A code-built boundary that is not
  a Spec shape — `Await`, `Interrupt`, a graph, an opaque Step — needs only the
  kind `Describe` reports, which is why those constants form a second block.
- One arity, one path. A composite runs its children through one implementation
  however many there are: `Parallel` had a single-branch path that re-derived the
  suspension classification, the merge, and the index a real failure is reported
  under, and the round that deleted it found that path one deletion away from
  labelling a wait as branch 0 failing. It bought two allocations, because
  `flow.Map` already runs a single element on the calling goroutine rather than
  through an errgroup — the layer below had the optimisation, and the layer above
  had a second copy of the meaning. An empty composite may still return early:
  doing nothing is a base case, not a second implementation.
- Let a cancellation outrank what a child reported, at every boundary, and expect
  to write it out. Some forty sites ask `context.Cause(ctx)` right after a child
  returns, because work that failed while its context was ending must not be
  reported as failing on its own terms. That repetition is not a missing helper:
  each site supplies its own fallback Store, and what a *suspension* means differs
  between them — a loop promotes the waiting body's writes, its stop decision
  re-identifies the wait under the loop's own ID, a subgraph passes it through
  untouched, and an iteration has already turned its elements' waits into values.
  Factoring the first line would hide the line that differs. `loopExecution.admit`
  is the one place the pattern is named, because both halves of one iteration do
  share a policy. Each boundary is held to the rule where it decides:
  `TestBranch_parentCancellationWinsAtEveryCallBoundary`,
  `TestSubgraph_parentCancellationWinsAtEveryBoundary`,
  `TestIteration_parentCancellationWinsAtEveryCallBoundary`, and
  `TestParallelBranches_resamplesParentCancellation` walk the checks one at a time
  with `ctxtest.CancelAtCheck`.
- Admission is the one cancellation check that does factor. Claiming an execution
  identity can wait on another goroutine, so `admitBoundary` and
  `admitScopedStep` sample the cause before returning, and their callers pair that
  one error with their own untouched Store — there is no fallback or suspension
  difference to hide, which is what separates this from the forty above. Expect no
  test to distinguish it: whatever runs next samples the cause too, so deleting
  either sample changes nothing observable. It stays because the boundary that
  decides admission is where "no work begins under a cancelled context" belongs,
  rather than in each caller's next act — a new composite would inherit the rule by
  calling the helper and lose it by hand.
- Two checks of one rule must be held to one verdict. `ValidateSpec` walks a Spec
  before anything is built and definition validation walks the built Steps;
  neither can be derived from the other, so duplicate identities, the ID a loop
  reserves inside its body, and the nesting limit are each stated twice. Nothing
  but a test keeps the two statements together, and a rule that drifted would make
  a workflow's legality depend on which form its author wrote it in:
  `TestTheTwoValidatorsRefuseTheSameDefects` asks both about one defect at a time.
  The sentinel is the agreement and the message deliberately is not — a Spec
  locates a defect by wire path, a definition by step identity — except where both
  can phrase it the same way, which
  `TestAProjectionDefectReadsTheSameWhicheverCheckFindsIt` holds to the full text.
- Ask for what the callee builds from. `leafCompiler` takes a `leafNode` — the
  registered node type and the `NodeSpec` a factory receives — so the nested-Spec
  and flat-Graph paths each convert to it rather than presenting a composite
  definition whose other members the callee has to ignore. The graph path used to
  fabricate a whole `Spec`, `Kind: KindLeaf` included, for a callee that never
  read a kind: a member nothing reads is a member nothing can be wrong about, and
  it is indistinguishable from one that has quietly stopped being read. Naming
  the real input also leaves one place to copy the caller's mutable values,
  `NodeSpec.clone`, instead of one per caller.
- Report the outcome in the event, not only in the return. Every boundary event
  names its step and carries what that boundary produced: `EventCompleted` and
  `EventSkipped` carry the Store, `EventSuspended` the wait, `EventFailed` the
  failure — including the two admission failures, which return before anything
  starts. An observer has no second source for any of it, and for a step that
  writes nothing or ran nothing the event is the whole account:
  `TestAwait_reportsTheWaitItRaisedAndTheStoreItLetThrough`,
  `TestInterrupt_eventsReportSuspensionThenReplay`, and
  `TestEvents_distinguishValidationReplayAndAdmission` read each one from the
  event rather than from the returned Store or error.
- Install a run before running children. Every composite calls `ensureRun`, which
  supplies one when the caller has none, because a `Step` may be invoked directly
  rather than through `Run`. The run owns the identities claimed so far, and
  claiming against no run silently succeeds, so a composite that skipped this
  would let one step ID run twice in a scope and notice nothing.
  `TestEveryCompositeRunDirectlyStillFormsOneRun` invokes each composite outside
  `Run`, with a body that reaches one leaf twice through a `flow` combinator:
  a nested workflow composite would install a run of its own and answer in place
  of the one under test, and two visible children sharing an ID are rejected by
  definition validation before anything runs.
- End a derived context where it was derived. Every `context.WithCancel` here
  belongs to a boundary — `Run`, a graph run, a leaf's emission session, and
  `flow.Race` — and each ends its own before returning, so work that outlived its
  boundary stops and a long-lived parent does not accumulate children it will
  never cancel. Each is asked where only its own cancel can answer, since a
  boundary above closes everything under it on the way out:
  `TestEveryBoundaryClosesTheContextItDerived` runs the graph on its own and asks
  the leaf's from the following step, and `TestRace_closesTheContextItDerived`
  fails every node, because a winner cancels the losers itself.
- State a wire member set once where you can. `expr` encodes and decodes through
  the same struct tags, so its members cannot drift. `workflow` states some sets
  twice on purpose: an explicit member list, or the embedded JSON Schema, rejects
  unknown, duplicate, and case-folded members, which `encoding/json` cannot. A
  second statement therefore has to be pinned, not trusted — a field added to one
  side alone yields a value that marshals to a document it then rejects. Add a
  type with a second statement to `TestWireTypesRoundTripEveryPopulatedField`,
  and keep a kind-discriminated one such as `Spec` in
  `TestSpecFieldMatricesAgreeWithTheSpecStruct`. Both derive what they expect by
  reflecting over the struct, so neither becomes another list to maintain.
- Name the test, and keep the name resolving. A comment that cites a test is how
  a pinned rule proves it is pinned: the reader checks the test instead of taking
  the comment's word. A citation that no longer resolves reads exactly like one
  that does, so a rename quietly turns a pinned rule back into a trusted one.
  `TestCitedTestsResolve` walks every comment and every document and fails on a
  name no test defines — three of eleven citations were already broken when it was
  written, one of them naming a benchmark that had never existed. A citation can
  also resolve to the wrong test: a comment that opens with a test's name must be
  attached to that test, because a new test inserted into an existing comment block
  inherits its opening lines and leaves the old test undocumented.
  `TestTestCommentsNameTheirOwnTest` checks the attachment that
  `TestCitedTestsResolve` cannot see. The documentation names the API the same way,
  and nothing compiles the fifty-odd Go snippets in the README and the tutorials, so
  `TestDocumentedAPINamesResolve` checks that every package-qualified name they use
  still exists.
- Name the package exactly once in an error. Each package reaches that
  differently, and the difference is forced rather than chosen: `flow`'s
  sentinels carry `flow:` because most of them reach a caller with nothing
  wrapping them, so the locations it adds — `IndexError`, a switch case — state
  only where. `workflow` and `expr` are the mirror image, because a `StepError`,
  `GraphError`, `SpecError`, or `expr.Error` always supplies the name, so the
  sentinels they wrap state only the condition. An error assembled from several
  independent ones adds nothing of its own, the way `errors.Join` does not: a
  fan-out of three suspensions names the package three times, not four.
  `TestErrorsNameThePackageAtMostOnce` in `flow` and `expr`,
  `TestSurfacedErrorsNamePackageExactlyOnce`, and
  `TestAJoinedSuspensionNamesThePackageOncePerWait` hold each package to it.
- Decode through `jsondoc.DecodeInto`. Every exported `UnmarshalJSON` here makes
  the same promise — a nil receiver reported rather than a panic, the whole
  document decoded, and the destination replaced only after complete success —
  and eight of them once implemented it independently, where any one could have
  assigned before checking its error. Supply a `decode` returning the value and
  a `wrap` giving the boundary its own diagnostic; the definition types need the
  second because a `GraphError` carries a field the plain boundaries have no
  place for. A type whose text is identity also needs `MarshalJSON`, because
  `encoding/json` replaces invalid UTF-8 by design and a wire type must refuse
  rather than rename itself: `TestEveryIdentityBearingTypeRefusesToRenameItself`.
  The rule is checked rather than trusted, because it has drifted twice and the
  half that matters is invisible — a decoder that assigns a partial value and then
  fails returns the same error as one that does not.
  `TestEveryUnmarshalJSONDecodesThroughOneBoundary` reads the bodies, and a type
  that genuinely cannot route through it says so in `wireDecodeExceptions`: only
  `Journal` does, because it owns a mutex and continues its own revision rather
  than being replaced.
- Say what a caller owns when an exported result is a slice or a map. Every one
  in this repo is built fresh and a signature cannot say so, so each says it in
  one clause the way `Journal.Keys` does. Whether the values inside are the
  caller's too needs saying separately — `Store.Changes` returns a fresh slice of
  borrowed values. A `MarshalJSON` needs no clause, since `json.Marshaler`
  already hands its bytes over. A new result that stays silent reads as
  uncertainty rather than as the convention it is; naming the ones that comply
  would be a list to maintain, and it drifted before this sentence replaced it.
- Let the type name itself once. A read that requires a type states it in the
  signature, in the assertion, and in the message it produces when something else
  arrived, and the three drift apart quietly: a wanted type spelled as prose is a
  claim nothing checks. `expr.evalAs` derives its message from its type argument,
  and `replayDecision` does the same for the two composites that journal a
  decision instead of an output — `Branch` wants a case name, `Loop` wants whether
  it stopped, and neither spells that anywhere but the type it asks for.
  `TestAReplayedDecisionMustCarryTheTypeItsCompositeRecorded` holds both to it.
- Open a doc comment with the name it documents, unexported declarations
  included. `godoclint`'s `start-with-name` checks exported symbols by default,
  which is the half a reader is most likely to catch anyway; all three that had
  drifted were internal — `baseCells` documented as "base", a `validate`
  documented as the type it belongs to, a vocabulary type documented as the type
  it describes. `start-with-name/include-unexported` closes it, and
  `TestTestCommentsNameTheirOwnTest` holds a test's comment to the same rule.
- Delete a single-call wrapper whose body is one call. Its name has to say more
  than the call it hides, and eight of them said less: `guaranteedStepListOutputs`
  named `unionOutputs(steps, guaranteedOutputs)`, which already reads as "every
  step runs, so union what they produce", and `validateIterationOutput` hid the
  `locate` that is the entire difference between how a definition and a `Spec`
  report one condition. A wrapper does earn its place when a caller should not see
  what it binds — but `compileNamed`'s two callers were the opposite case, because
  the arguments they bound are exactly what distinguishes a condition table from a
  resolver table.
- Register a new package in `moduleImports` before importing it. That table is the
  module's dependency direction written out, and it states what each layer permits
  rather than what it happens to use, so the edge that inverts the module is
  refused by name: a sibling reaching sideways, the primitive layer borrowing a
  private helper, an internal package depending on the layer it exists to serve.
  Every other axiom in [AGENTS.md](./AGENTS.md) is about what a package means and
  is held by tests of its behavior; this one is a property of the import graph that
  no behavior can reveal, so `TestPackageDependenciesPointOneWay` reads the imports
  and `TestModuleImportsCoversEveryPackage` keeps the table from permitting a
  package it forgot to mention.
- Say what a rendering may claim. `diagram` draws whatever graph it is handed,
  because an editor renders a document before it is valid and neither renderer
  consults a `Registry`. It labelled a gate by testing for `TriggerAny` and calling
  everything else "all", which reads as a rule the document did not ask for on any
  trigger this package has not heard of. Every trigger but the zero value spells
  itself, so only that one needs a name here —
  `TestASCII_namesTheTriggerADocumentCarries`.
- Own state or be a function. `graphPlanner`, `nodeConnector`, and `specValidator`
  are types because a traversal mutates them; `switchCompiler` had one immutable
  field and a method that ignored its receiver, so the shape promised state that
  was not there. `Switch` is the compiler it always was. A method that never names
  its receiver says the same thing more quietly, and two did: `collectBranches`
  classifies the branches of an `Unwrap() []error`, none of which is the tree it
  was hung on, and `decodeScope` reads a member of the record in front of it and
  nothing of the decoder's own state.
- Let the type own an order it promises. Three exported results are documented as
  sorted references, and the order lived in `Ref.compare` for two of them while
  `expr` wrote it out again for the third — which is the one a caller is told to
  diff against `Graph.Inputs`, and a diff of two sorted lists is nonsense unless
  both were sorted the same way. `Ref.Compare` is exported for that reason, not for
  repository-local convenience. Its test spells the expected order out rather than
  deriving it from the comparator, because asking two producers to agree passes
  however the comparator orders them — the drift it exists to prevent.
- Point a doc link at something that exists. A `[Name]` that resolves becomes a
  link in godoc and one that does not is rendered as bracketed text, so a rename
  leaves 574 of them degrading silently. `TestGoDocLinksResolve` checks the ones it
  can tell from prose — a qualified name from this module, a name starting with a
  capital, and a lowercase name below a declared type — because a comment writing
  `scope[index]` is the same shape as a link to an unexported name, and only the
  type in front of the dot distinguishes them.
- Publish one ordered walk, and pin the order where a diagnostic spends it.
  `Inputs` is a map, so name order is the only reason a check reports the same
  first offending binding twice — and it published that one order three times: as
  `PortNames`, as `Refs`, and as an internal neutral spelling that existed only
  because "port" is the wrong word for a subgraph seed. Six callers wanting a name
  and its reference re-indexed the map to recover the half their walk had dropped.
  `Inputs.All` yields the pair and names neither boundary, so each caller still
  supplies its own vocabulary. Determinism bought this way is invisible in the
  sorted spelling and breaks no accepted definition, only rejected ones, so it is
  pinned where it is spent — a factory reading one port or none, a schema's
  declared and undeclared ports, subgraph seeds, branch cases, and the steps
  inside them: `TestInputsWalkInNameOrderSoTheFirstOffenderIsAlwaysTheSame` and
  `TestADefinitionWithSeveralDefectsIsRefusedForTheSameOne`. Both were written
  against the map-order mutant that the whole suite had survived.
- Pin the half of a concurrency window a test can choose, and say who chooses the
  rest. Removing a lock is a data race only if something puts two accesses in flight
  together, so a critical section no test exercises concurrently reads as protected
  while being untested. Of this package's twelve exclusive sections, eight race under
  `-race` the moment their lock goes. The other four are the emission lease's, and
  every streaming test either waited for its yields before returning or sequenced
  them with channels, so a close had never run while a yield was arriving. Admission
  is the half a test can enter on purpose — a producer that returns while its
  goroutines are still yielding — and
  `TestStreamFunc_aYieldRacingTheCloseReportsWhatItDid` asserts only what timing may
  not decide: a yield reports truthfully whether its value was delivered, nothing
  arrives after `Run` returns, delivery stays serialized, indexes stay a gapless
  prefix. The other three need two leaked yields to overlap, and `close` runs on the
  goroutine that ran the producer, so dropping their locks races only sometimes —
  `emissionLease` records that beside them rather than leaving the next reader to
  hunt for the missing test.
- State a value type's algebra even where nothing observes it. Thirty-three
  defensive copies keep this module's constructors and accessors owning what they
  hand out, and twenty-seven fail a test the moment one goes — the
  `_owns…Structure` family. Of the six that do not, five are a second copy of a
  clone that already happened on every path to a caller, and `outputGuarantee.union`
  is the one whose clone carries meaning: it is a method on a struct passed by value,
  so `a.union(b)` has to leave `a` alone, and `unionOutputs` folding the result back
  over its accumulator is the only reason nothing notices today.
  `TestOutputGuaranteeCombinatorsLeaveTheirOperandsAlone` says it instead of leaving
  it for a second caller to discover.
- Match a category on every route that reports it. `%w` and `%v` render the same
  bytes, and a sentinel handed to a structured error is indistinguishable from any
  other error of the same text, so a wrap is a promise only where something matches
  through it. Of 159 format wraps here, 48 change nothing when downgraded — mostly
  an inner `%w` over a cause carrying no sentinel — and the ones worth reading are
  the sentinel-carrying routes. Two were unheld. `ErrUnknownNodeType` is reported
  from three places and only the `Spec` validator's was matched, while
  `errors_test.go` builds a `GraphError` around that sentinel by hand to check how a
  location prints, which says nothing about the path that reports it:
  `TestUnknownNodeTypeIsMatchableOnEveryRouteThatReportsIt` covers the routes
  instead. `RefError`'s doc promises `ErrNotFound` or `ErrTypeMismatch`, and the
  stored-nil branch was the half nothing matched — the category is what makes
  `FirstOf` stop there rather than skip to a later reference, so
  `TestAStoredNilIsATypeErrorNotAnAbsence` pins the consequence and not just the
  sentinel. A branch no public route can reach says so where it is, the way
  `leafCompiler.compile` does.
- Pin a wire member name where nothing else will. Renaming any of this module's 64
  JSON members fails a test — except ten, and each of the ten says something. Two
  are `specJSONDecoder`'s `steps` and `body`: renaming them decodes valid documents
  identically, because `encoding/json` then reaches the embedded `Spec` field of the
  same name. Those raw shadows exist for exactly one thing, locating the single
  nested failure the embedded schema cannot express — an integer it accepts as an
  integer and Go cannot represent — so
  `TestSpecDecodingLocatesTheChildAJSONSchemaCannotReach` names that class rather
  than the mechanism. The other eight belong to `Description` and `NodeSchema`,
  which nothing in this module encodes: a rename there is silent in the repository
  and breaking for every editor, which is what both types are documented to serve.
  `TestEditorFacingTypesPublishTheWireNamesTheyCarry` spells their bytes out,
  including which members an absent value may omit.
- Spell an enumerated value that leaves the package, and let the source decide the
  list. A caller compares against `KindLeaf` or `EventFailed` rather than against
  `"leaf"` or `"failed"`, and so does almost every test here, which makes both
  invariant to the spelling: respelling twenty-two of this package's ninety-four
  string constants failed nothing at all. Which ones were held was incidental —
  `TypeString` and `TypeNumber` only because a golden encoding happened to write
  the words out, and the four `ValueType`s beside them not at all. A `Kind` names a
  step in a `Spec` document, an `EventKind` and a `StepOp` reach an application that
  traces or persists a run, a `ValueType` is a `NodeSchema` member an editor
  encodes, and the registration kinds are a `RegistrationError` field, so
  `TestEveryPublishedVocabularySpellsItselfOut` reads the constants out of the
  package source: a new member has to be spelled before it ships, and a spelling
  left behind by a removed one fails rather than reading as coverage. The name kinds
  in `errors.go` stay out of it, because a fragment of a sentence is prose and the
  contract here is the sentinel and the structured location.
- Let measurement decide whether a degenerate shape gets a path of its own, and
  write down which way it went. Two of them read identically in the source and were
  not. `Store.cells` had a branch for a Store with no overlay that copied the
  snapshot loop so it could skip building a tracking set; it made that function the
  module's most complex at 25, it made the iterator's early-stop contract three
  statements instead of two, and the set it saved costs nothing — a zero-hint map
  that does not escape allocates no table, and `BenchmarkStoreChangesScaling` and
  `BenchmarkStoreJSONScaling` report identical allocations without it. It is gone.
  `definitionValidator` keeps its first claimed ID in a field rather than a set, and
  collapsing that into the simpler "always use the set" costs two allocations per
  validated boundary — `BenchmarkSequenceRunScaling/512` goes from 1612 to 2636 —
  while passing every other test in this repository. It stays, and
  `TestValidatingOneBoundaryAllocatesNoIdentitySet` is what refuses the
  simplification now, because a comment claiming an allocation is not a guard.
- Split a function that holds two subjects, and leave one that holds a single
  irreducible subject alone. Probing at `gocognit` 12 and `gocyclo` 10 — far under
  the gate's 30 — surfaced fourteen, and six of them were two things wearing one
  name. `Get` located the same failed read at four returns, so `typedRead` says the
  reference and the wanted type once and `nilAssignable` names the kinds a stored
  nil reads back as. `suspensionTree.collect` mixed what one error node means with
  how a tree is walked, and `waitAt` is the first of those. `specJSONEncoder.encode`
  was one block while the decoder beside it was already three, so `encodeSteps`,
  `encodeCases`, and `encodeBody` now mirror `decodeSteps`, `decodeCases`, and
  `decodeBody`. `NodeSchema.validate` is the three declarations its own doc lists.
  `compileCall` is dispatch again with `compileHas` and `compileLen` owning their
  builtins, and `compileBinary` names its two closures — `shortCircuit` and
  `binaryOperator.eval` — which also retires a `//nolint:exhaustive` covering a
  switch with one case.
  The eight left are each one subject: a scheduler loop, an error-tree walk, a
  shadowing walk over two maps, an escape scanner, a number parser, a straight-line
  factory pipeline, and two dispatches over the kind vocabulary — where
  `Spec.requireKindFields` *is* the field matrix. Splitting those would hide the
  thing they exist to show.
- Let a declared identity travel as one value until the layer that consumes its
  halves. Sweeping for parameter pairs that appear together in three or more
  signatures put `(scope []ScopeFrame, id string)` at the top with eleven, and that
  pair already has a name: `JournalKey` is what a boundary invocation is known by,
  and the exported `Journal.Record` and `Journal.Forget` carry it. Below them it was
  taken apart at every step — `Journal.record` and `runState.claim` each rebuilt the
  key from a pair their own caller had just split, `scopedSet` stored it in the same
  trie either way, and `suspensionList.identify` took the two in the opposite order.
  `boundaryKey(ctx, id)` builds it once where a boundary knows both halves, and the
  pair survives only in `journalNode`, where the scope is a path to descend and the
  ID a member to act on; `journalNode.record` says so where it happens. The weaker
  clumps stay: a registration's kind and name exist to build a `RegistrationError`,
  and a binary operator's two operands are two things. A pair sweep sees only pairs,
  so it missed `withEmission`, which took the same identity in the opposite order
  with two other arguments between sightings; a separate look at signatures with
  four or more non-context parameters is what surfaced it, and `emissionSession`
  now holds the key instead of its two halves.
- Make an ordering that matters impossible rather than documented. Every call of
  `suspensionList.identify` was immediately followed by `err`, and its own comment
  said why they had to be — assigning an ID changes the sort key, so ordering the
  list before the names are in place orders it by identities it does not have yet.
  A comment is not a guard, and three callers each had to remember. `errAt` is the
  one operation, and there is nothing left to call in the wrong order. A sweep for
  calls on the same value in adjacent statements finds no other pair.
- Say a two-route rule the same way on both routes. A skeleton sweep — function
  bodies with identifiers and literals erased — pairs off much of this module by
  design: Graph against Spec, condition against resolver, iteration against
  subgraph. That symmetry is the point, so a match is evidence only where one side
  names the rule and the other inlines it. `validateIteration` and
  `validateSubgraph` inlined the projection check a built definition already names,
  where a `*OutputCondition` reports why a body cannot satisfy its output and
  `locate` attaches where it belongs. The Spec side has both now, and the two
  routes read alike. `nodeSet` and `definitionIDs` keep their identical
  constructors: one answers membership, the other is a claim set cloned along a
  path, and a shared generic constructor would cost more machinery than the five
  lines it saves. `unparam`, `wastedassign`, and `prealloc` find nothing here;
  `gocritic`'s full tag set finds one type declared after its own methods.
- Prefer standard Go contracts, explicit context propagation, and errors that
  work with `errors.Is` and `errors.As`.
- Keep distributed scheduling, durable timers, and exactly-once execution out
  of scope.

New abstraction is justified only when it removes repeated policy without
hiding control flow. Prefer one clear shape per purpose and useful zero values.

## Public API changes

Any exported change must include:

- A package comment or symbol comment that defines behavior and edge cases.
- An external-package test or executable example showing caller usage.
- Error semantics, including stable sentinels or structured errors when callers
  need to branch.
- Cancellation and concurrency semantics where applicable.
- A migration entry in [CHANGELOG.md](./CHANGELOG.md) if existing callers must
  change.

Adding a method to an exported interface is breaking. Raising the `go` directive
also raises every dependent's toolchain floor. Treat both as compatibility
decisions, not routine cleanup.

## Documentation changes

- Keep the root README focused on package choice, first use, and capability
  discovery.
- Put progressive teaching in [`docs/tutorials`](./docs/tutorials/README.md).
- Keep runnable code in [`example`](./example/README.md).
- Keep user-visible release notes and released compatibility history in the
  changelog; pre-release implementation archaeology belongs in Git history.
- Use package comments and examples for API reference.

When documentation contains code, prefer a runnable example as its source of
truth and link to it.

## Pull requests

Keep commits reviewable and avoid mixing unrelated cleanup with behavioral
changes. Explain:

- the problem and user-visible outcome;
- API and behavioral trade-offs;
- error and cancellation behavior;
- benchmark evidence for performance claims;
- migration steps for a compatibility break.

Maintainers should use the [release checklist](./docs/releasing.md) before
tagging.
