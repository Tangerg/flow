package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/Tangerg/flow/internal/jsondoc"
	"github.com/Tangerg/flow/internal/jsonnum"
)

// graphJSON and specJSON are the strict DSL's typed decoding boundary. They
// retain the exported definitions as plain Go data while reconciling one
// representational difference: JSON Schema integers include spellings such as
// 1.0 and 1e0, whereas encoding/json accepts only integer tokens for an int.
type graphJSON struct {
	graph Graph
}

type specJSON struct {
	spec Spec
}

// specJSONDecoder owns the recursive raw members while one Spec is decoded.
// Keeping child conversion on this receiver leaves specJSON as a small wire
// adapter and keeps application Config bytes out of the normalization path.
//
// A child failure is located by wrapping on the way out — "steps[0]: case ..." —
// because json.Unmarshaler cannot be handed a position on the way in. That reads
// differently from the JSON Pointer the schema reports, and only for the failures
// the schema cannot see, which is an integer beyond int64. Threading a path down
// would mean bypassing the Unmarshaler seam and turning these into SpecError
// values, which the outer wrap in Spec.UnmarshalJSON would then name twice.
type specJSONDecoder struct {
	fields        specJSONFields
	steps         []json.RawMessage
	cases         map[string]json.RawMessage
	body          json.RawMessage
	concurrency   json.RawMessage
	maxIterations json.RawMessage
}

type graphJSONEncoder struct {
	graph Graph
}

// specJSONOutput suppresses Spec.MarshalJSON recursively while retaining the
// exported wire fields through specJSONFields. Explicit recursive members win
// over the embedded aliases, just as they do in specJSONDecoder.
type specJSONOutput struct {
	*specJSONFields
	Steps []specJSONOutput          `json:"steps,omitempty"`
	Cases map[string]specJSONOutput `json:"cases,omitempty"`
	Body  *specJSONOutput           `json:"body,omitempty"`
}

// specJSONEncoder owns the depth and cycle state of one marshal. It builds a
// method-free wire tree in the same pass that proves text can cross JSON without
// encoding/json replacing bytes. The spec being encoded is an argument rather
// than state: only the outermost call needs the root, so carrying it would give
// every child a copy of a field none of them reads.
type specJSONEncoder struct {
	active map[*Spec]struct{}
	depth  int
}

// graphJSONFields and specJSONFields prevent the wrapper methods above from
// recursing while retaining the exported wire fields.
type (
	graphJSONFields Graph
	specJSONFields  Spec
)

func decodeGraphDocument(data []byte) (Graph, error) {
	var document graphJSON
	if err := schemaLoader(loadGraphSchema).decode(jsonDocument(data), &document); err != nil {
		return Graph{}, err
	}
	return document.graph, nil
}

func decodeSpecDocument(data []byte) (Spec, error) {
	var document specJSON
	if err := schemaLoader(loadSpecSchema).decode(jsonDocument(data), &document); err != nil {
		return Spec{}, err
	}
	return document.spec, nil
}

func (g graphJSONEncoder) marshal() ([]byte, error) {
	for index, node := range g.graph.Nodes {
		field, err := node.validateJSONText()
		if err != nil {
			return nil, locateNode(index, node).fieldError(field, err)
		}
	}
	encoded, err := marshalJSON(graphJSONFields(g.graph))
	if err != nil {
		return nil, graphJSONError(err)
	}
	return encoded, nil
}

func (n GraphNode) validateJSONText() (string, error) {
	// Each member carries both halves of its vocabulary: the serialized field a
	// GraphError points at, and the concept name the message states. Reusing the
	// field for both would repeat it -- "field type: type is not valid UTF-8" --
	// and would describe the same member differently from the definition check.
	for _, member := range [...]struct {
		field string
		kind  string
		value string
	}{
		{field: fieldID, kind: nameStepID, value: n.ID},
		{field: fieldType, kind: nameNodeType, value: n.Type},
		{field: fieldTrigger, kind: nameTrigger, value: string(n.Trigger)},
	} {
		if err := validateText(member.kind, member.value); err != nil {
			return member.field, err
		}
	}
	if err := n.Inputs.validatePortJSONText(); err != nil {
		return fieldInputs, err
	}
	if len(n.Config) > 0 {
		if err := jsonDocument(n.Config).validate(); err != nil {
			return fieldConfig, err
		}
	}
	for index, dependency := range n.DependsOn {
		if err := validateText(nameDependency, dependency); err != nil {
			return fieldDependsOn, fmt.Errorf("dependency %d: %w", index, err)
		}
	}
	for index, gate := range n.When {
		if err := gate.validateJSONText(); err != nil {
			return fieldWhen, fmt.Errorf("gate %d: %w", index, err)
		}
	}
	return "", nil
}

func (g Gate) validateJSONText() error {
	if err := validateText(nameGateSource, g.NodeID); err != nil {
		return err
	}
	return validateText(nameGateOutlet, g.Outlet)
}

func (s *specJSONEncoder) marshal(root Spec) ([]byte, error) {
	output, err := s.encode(root)
	if err != nil {
		return nil, err
	}
	// The logical Spec-depth check bounds construction and detects pointer
	// cycles. The complete wire document has additional object and array
	// containers, and a valid Config gains enclosing containers here, so only
	// validating the assembled bytes proves that this package can read its own
	// output.
	encoded, err := marshalJSON(output)
	if err != nil {
		return nil, root.fieldError(fieldJSON, err)
	}
	return encoded, nil
}

func (s *specJSONEncoder) encode(spec Spec) (specJSONOutput, error) {
	if err := spec.checkDepth(s.depth); err != nil {
		return specJSONOutput{}, err
	}
	if field, err := spec.validateJSONText(); err != nil {
		return specJSONOutput{}, spec.fieldError(field, err)
	}

	fields := specJSONFields(spec)
	output := specJSONOutput{specJSONFields: &fields}
	// The wire tags decide whether a child member appears: an empty container and
	// an absent one encode alike. The guards below only skip allocating a container
	// that no child will fill.
	if len(spec.Steps) > 0 {
		output.Steps = make([]specJSONOutput, len(spec.Steps))
		for index, child := range spec.Steps {
			encoded, err := s.encodeChild(child)
			if err != nil {
				return specJSONOutput{}, locateSpecError(err, fieldSteps, strconv.Itoa(index))
			}
			output.Steps[index] = encoded
		}
	}
	if len(spec.Cases) > 0 {
		output.Cases = make(map[string]specJSONOutput, len(spec.Cases))
		for _, name := range slices.Sorted(maps.Keys(spec.Cases)) {
			encoded, err := s.encodeChild(spec.Cases[name])
			if err != nil {
				return specJSONOutput{}, locateSpecError(err, fieldCases, name)
			}
			output.Cases[name] = encoded
		}
	}
	if spec.Body != nil {
		if _, cyclic := s.active[spec.Body]; cyclic {
			return specJSONOutput{}, spec.fieldError(
				fieldBody,
				errors.New("cyclic spec body"),
			)
		}
		s.active[spec.Body] = struct{}{}
		encoded, err := s.encodeChild(*spec.Body)
		delete(s.active, spec.Body)
		if err != nil {
			return specJSONOutput{}, locateSpecError(err, fieldBody)
		}
		output.Body = &encoded
	}
	return output, nil
}

// encodeChild encodes spec one level down. The child is a new encoder rather
// than a depth this one raises and puts back, so no return path can leave a
// sibling starting deeper than it is -- which would bound how many specs a
// document may hold rather than how deeply they nest, see
// TestSpec_boundsNestingNotBreadth. [specValidator.child] walks the same
// tree the same way, and the two must agree because they share [Spec.checkDepth].
// The cycle set stays shared: it tracks the walk, not the level.
func (s *specJSONEncoder) encodeChild(spec Spec) (specJSONOutput, error) {
	child := specJSONEncoder{active: s.active, depth: s.depth + 1}
	return child.encode(spec)
}

func (s Spec) validateJSONText() (string, error) {
	for _, member := range [...]struct {
		field string
		kind  string
		value string
	}{
		{field: fieldKind, kind: nameKind, value: string(s.Kind)},
		{field: fieldID, kind: nameStepID, value: s.ID},
		{field: fieldType, kind: nameNodeType, value: s.Type},
		{field: fieldResolver, kind: nameResolver, value: s.Resolver},
		{field: fieldCondition, kind: nameCondition, value: s.Condition},
	} {
		if err := validateText(member.kind, member.value); err != nil {
			return member.field, err
		}
	}
	if len(s.Config) > 0 {
		if err := jsonDocument(s.Config).validate(); err != nil {
			return fieldConfig, err
		}
	}
	if err := s.Input.validateJSONText(); err != nil {
		return fieldInput, err
	}
	var inputsErr error
	if s.Kind == KindSubgraph {
		inputsErr = s.Inputs.validateSeedJSONText()
	} else {
		inputsErr = s.Inputs.validatePortJSONText()
	}
	if inputsErr != nil {
		return fieldInputs, inputsErr
	}
	if err := s.BodyOutput.validateJSONText(); err != nil {
		return fieldBodyOutput, err
	}
	// A case name is an object member on the wire, so it carries identity that
	// encoding/json would replace rather than reject. Sorted, so a Spec with more
	// than one invalid name always reports the same one.
	for _, name := range slices.Sorted(maps.Keys(s.Cases)) {
		if err := validateText(nameBranchCase, name); err != nil {
			return fieldCases, err
		}
	}
	return "", nil
}

func (g *graphJSON) UnmarshalJSON(data []byte) error {
	var next graphJSONFields
	document := struct {
		*graphJSONFields
		Concurrency json.RawMessage `json:"concurrency"`
	}{graphJSONFields: &next}
	if err := json.Unmarshal(data, &document); err != nil {
		return err
	}
	concurrency, err := decodeDefinitionInt("concurrency", document.Concurrency)
	if err != nil {
		return err
	}
	next.Concurrency = concurrency
	g.graph = Graph(next)
	return nil
}

func (s *specJSON) UnmarshalJSON(data []byte) error {
	var decoder specJSONDecoder
	if err := decoder.decode(data); err != nil {
		return err
	}
	s.spec = Spec(decoder.fields)
	return nil
}

func (s *specJSONDecoder) decode(data []byte) error {
	document := struct {
		*specJSONFields
		Steps         []json.RawMessage          `json:"steps"`
		Cases         map[string]json.RawMessage `json:"cases"`
		Body          json.RawMessage            `json:"body"`
		Concurrency   json.RawMessage            `json:"concurrency"`
		MaxIterations json.RawMessage            `json:"maxIterations"`
	}{specJSONFields: &s.fields}
	if err := json.Unmarshal(data, &document); err != nil {
		return err
	}
	s.steps = document.Steps
	s.cases = document.Cases
	s.body = document.Body
	s.concurrency = document.Concurrency
	s.maxIterations = document.MaxIterations

	concurrency, err := decodeDefinitionInt("concurrency", s.concurrency)
	if err != nil {
		return err
	}
	maxIterations, err := decodeDefinitionInt("maxIterations", s.maxIterations)
	if err != nil {
		return err
	}
	s.fields.Concurrency = concurrency
	s.fields.MaxIterations = maxIterations
	if err := s.decodeSteps(); err != nil {
		return err
	}
	if err := s.decodeCases(); err != nil {
		return err
	}
	return s.decodeBody()
}

func (s *specJSONDecoder) decodeSteps() error {
	if s.steps != nil {
		s.fields.Steps = make([]Spec, len(s.steps))
		for index, child := range s.steps {
			decoded, err := decodeSpecJSON(child)
			if err != nil {
				return fmt.Errorf("steps[%d]: %w", index, err)
			}
			s.fields.Steps[index] = decoded
		}
	}
	return nil
}

func (s *specJSONDecoder) decodeCases() error {
	if s.cases != nil {
		s.fields.Cases = make(map[string]Spec, len(s.cases))
		for _, name := range slices.Sorted(maps.Keys(s.cases)) {
			child := s.cases[name]
			decoded, err := decodeSpecJSON(child)
			if err != nil {
				return fmt.Errorf("case %q: %w", name, err)
			}
			s.fields.Cases[name] = decoded
		}
	}
	return nil
}

func (s *specJSONDecoder) decodeBody() error {
	if len(s.body) > 0 && !bytes.Equal(s.body, []byte("null")) {
		body, err := decodeSpecJSON(s.body)
		if err != nil {
			return fmt.Errorf("body: %w", err)
		}
		s.fields.Body = &body
	}
	return nil
}

func decodeSpecJSON(data []byte) (Spec, error) {
	var document specJSON
	if err := json.Unmarshal(data, &document); err != nil {
		return Spec{}, err
	}
	return document.spec, nil
}

// decodeDefinitionInt maps JSON Schema's mathematical integer domain onto a
// Go int without a float64 round trip. An absent or null member produces the
// zero value, matching encoding/json on a freshly allocated destination.
func decodeDefinitionInt(field string, data json.RawMessage) (int, error) {
	converted, err := decodeInt(data)
	if err != nil {
		return 0, fmt.Errorf("json field %s: %w", field, err)
	}
	return converted, nil
}

// decodeInt states only what is wrong with the member; decodeDefinitionInt
// names which member it was.
func decodeInt(data json.RawMessage) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	value, err := jsonDocument(data).value()
	if err != nil {
		return 0, err
	}
	if value == nil {
		return 0, nil
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf("expected integer, got %s", jsondoc.Kind(value))
	}
	integer, err := jsonnum.ParseInteger(number.String())
	// jsonDocument already proved the JSON number grammar, so ParseInteger can
	// report only a fractional value or an out-of-range magnitude here.
	if errors.Is(err, jsonnum.ErrFractional) {
		return 0, fmt.Errorf("%s is not an integer", number)
	}
	// A magnitude jsonnum rejects and one strconv.Atoi rejects are the same
	// condition seen at two widths: the value does not fit this platform's int.
	if err == nil {
		if converted, convertErr := strconv.Atoi(integer.String()); convertErr == nil {
			return converted, nil
		}
	}
	return 0, fmt.Errorf("integer %s overflows int", number)
}
