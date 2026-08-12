package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"

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

// specJSONEncoder owns recursive path, depth, and cycle state for one marshal.
// It builds a method-free wire tree in the same pass that proves text can cross
// JSON without encoding/json replacing bytes.
type specJSONEncoder struct {
	root   Spec
	active map[*Spec]struct{}
	depth  int
}

// graphJSONFields and specJSONFields prevent the wrapper methods above from
// recursing while retaining the exported wire fields.
type (
	graphJSONFields Graph
	specJSONFields  Spec
)

func decodeGraphDocument(data jsonDocument) (Graph, error) {
	var document graphJSON
	if err := schemaLoader(loadGraphSchema).decode(data, &document); err != nil {
		return Graph{}, err
	}
	return document.graph, nil
}

func decodeSpecDocument(data jsonDocument) (Spec, error) {
	var document specJSON
	if err := schemaLoader(loadSpecSchema).decode(data, &document); err != nil {
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
		return nil, &GraphError{Field: fieldJSON, Err: err}
	}
	return encoded, nil
}

func (n GraphNode) validateJSONText() (string, error) {
	for _, field := range [...]struct {
		name  string
		value string
	}{
		{name: fieldID, value: n.ID},
		{name: fieldType, value: n.Type},
		{name: fieldTrigger, value: string(n.Trigger)},
	} {
		if err := validateText(field.name, field.value); err != nil {
			return field.name, err
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
		if err := validateText("dependency ID", dependency); err != nil {
			return fieldDependsOn, fmt.Errorf("dependency %d: %w", index, err)
		}
	}
	for index, gate := range n.When {
		if err := validateText("gate source node ID", gate.NodeID); err != nil {
			return fieldWhen, fmt.Errorf("gate %d: %w", index, err)
		}
		if err := validateText("gate outlet", gate.Outlet); err != nil {
			return fieldWhen, fmt.Errorf("gate %d: %w", index, err)
		}
	}
	return "", nil
}

func (s *specJSONEncoder) marshal() ([]byte, error) {
	output, err := s.encode(s.root)
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
		return nil, s.root.fieldError(fieldJSON, err)
	}
	return encoded, nil
}

func (s *specJSONEncoder) encode(spec Spec) (specJSONOutput, error) {
	if s.depth > MaxNestingDepth {
		return specJSONOutput{}, spec.depthError(s.depth)
	}
	if field, err := spec.validateJSONText(); err != nil {
		return specJSONOutput{}, spec.fieldError(field, err)
	}

	fields := specJSONFields(spec)
	output := specJSONOutput{specJSONFields: &fields}
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
			if err := validateText("branch case name", name); err != nil {
				return specJSONOutput{}, spec.fieldError(fieldCases, err)
			}
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

func (s *specJSONEncoder) encodeChild(spec Spec) (specJSONOutput, error) {
	s.depth++
	defer func() { s.depth-- }()
	return s.encode(spec)
}

func (s Spec) validateJSONText() (string, error) {
	for _, field := range [...]struct {
		name  string
		value string
	}{
		{name: fieldKind, value: string(s.Kind)},
		{name: fieldID, value: s.ID},
		{name: fieldType, value: s.Type},
		{name: fieldResolver, value: s.Resolver},
		{name: fieldCondition, value: s.Condition},
	} {
		if err := validateText(field.name, field.value); err != nil {
			return field.name, err
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
	if len(data) == 0 {
		return 0, nil
	}
	value, err := jsonDocument(data).value()
	if err != nil {
		return 0, fmt.Errorf("json field %s: %w", field, err)
	}
	if value == nil {
		return 0, nil
	}
	number, ok := value.(json.Number)
	if !ok {
		return 0, fmt.Errorf(
			"json field %s: expected integer, got %s",
			field,
			(jsonValue{raw: value}).kind(),
		)
	}
	integer, err := jsonnum.ParseInteger(number.String())
	// jsonDocument already proved the JSON number grammar, so ParseInteger can
	// report only a fractional value or an out-of-range magnitude here.
	if errors.Is(err, jsonnum.ErrFractional) {
		return 0, fmt.Errorf("json field %s: %s is not an integer", field, number)
	}
	if err != nil {
		return 0, fmt.Errorf("json field %s: integer %s overflows int", field, number)
	}
	decimal := strconv.FormatUint(integer.Magnitude, 10)
	if integer.Negative {
		decimal = "-" + decimal
	}
	converted, err := strconv.Atoi(decimal)
	if err != nil {
		return 0, fmt.Errorf("json field %s: integer %s overflows int", field, number)
	}
	return converted, nil
}
