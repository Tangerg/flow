package workflow

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestDefinitionJSONDecodesLimitsRecursivelyAndPreservesRawConfig(t *testing.T) {
	graphData := []byte(`{
		"nodes":[{"id":"node","type":"kind","config": { "n": 1.0, "text": "1e0" }}],
		"concurrency":1e1
	}`)
	var graphDocument graphJSONInput
	if err := json.Unmarshal(graphData, &graphDocument); err != nil {
		t.Fatalf("Unmarshal Graph: %v", err)
	}
	if graphDocument.graph.Concurrency != 10 {
		t.Fatalf("Graph.Concurrency = %d; want 10", graphDocument.graph.Concurrency)
	}
	wantConfig := json.RawMessage(`{ "n": 1.0, "text": "1e0" }`)
	if !reflect.DeepEqual(graphDocument.graph.Nodes[0].Config, wantConfig) {
		t.Fatalf(
			"Config = %s; want original %s",
			graphDocument.graph.Nodes[0].Config,
			wantConfig,
		)
	}

	specData := []byte(`{
		"kind":"branch", "id":"branch", "resolver":"pick",
		"cases":{
			"one":{"kind":"parallel","steps":[{"kind":"sequence","steps":[]}],"concurrency":1e1},
			"two":{"kind":"loop","id":"loop","condition":"done","maxIterations":2.0,"body":null}
		}
	}`)
	var specDocument specJSONInput
	if err := json.Unmarshal(specData, &specDocument); err != nil {
		t.Fatalf("Unmarshal Spec: %v", err)
	}
	one := specDocument.spec.Cases["one"]
	two := specDocument.spec.Cases["two"]
	if one.Concurrency != 10 || len(one.Steps) != 1 ||
		two.MaxIterations != 2 || two.Body != nil {
		t.Fatalf("decoded cases = %+v; want recursive limits and null body", specDocument.spec.Cases)
	}
}

func TestDefinitionJSONRejectsInvalidEngineIntegersWithoutReplacingResult(t *testing.T) {
	graphDocument := graphJSONInput{graph: Graph{Concurrency: 7}}
	err := json.Unmarshal([]byte(`{"nodes":[],"concurrency":1.5}`), &graphDocument)
	if err == nil || !strings.Contains(err.Error(), "concurrency") {
		t.Fatalf("Unmarshal error = %v; want concurrency error", err)
	}
	if graphDocument.graph.Concurrency != 7 {
		t.Fatalf("Graph changed on error: %+v", graphDocument.graph)
	}

	for name, data := range map[string]string{
		"steps": `{"kind":"sequence","steps":[{"kind":"parallel","concurrency":1.5}]}`,
		"cases": `{"kind":"branch","cases":{"bad":{"kind":"loop","maxIterations":1.5}}}`,
		"body":  `{"kind":"loop","body":{"kind":"parallel","concurrency":1.5}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var document specJSONInput
			if err := json.Unmarshal([]byte(data), &document); err == nil {
				t.Fatal("Unmarshal succeeded; want nested integer error")
			}
		})
	}
}

func TestDefinitionJSONReportsInvalidCasesDeterministically(t *testing.T) {
	data := []byte(`{
		"kind":"branch", "id":"branch", "resolver":"pick",
		"cases":{
			"z":{"kind":"parallel","steps":[],"concurrency":1e1000},
			"a":{"kind":"parallel","steps":[],"concurrency":1e1000}
		}
	}`)
	var first string
	for range 100 {
		_, err := NewRegistry().CompileSpecJSON(data)
		if err == nil || !strings.Contains(err.Error(), `case "a"`) {
			t.Fatalf("CompileSpecJSON error = %v; want first case a", err)
		}
		if first == "" {
			first = err.Error()
		} else if err.Error() != first {
			t.Fatalf("error changed from %q to %q", first, err)
		}
	}
}

func TestDefinitionJSONPreservesDecoderTypeErrors(t *testing.T) {
	var graphDocument graphJSONInput
	if err := json.Unmarshal([]byte(`[]`), &graphDocument); err == nil {
		t.Fatal("Graph unmarshal succeeded; want type error")
	}
	var specDocument specJSONInput
	if err := json.Unmarshal([]byte(`[]`), &specDocument); err == nil {
		t.Fatal("Spec unmarshal succeeded; want type error")
	}
}

// A body member is read when it holds anything but null, which a body of a single
// byte is the shortest way to say: it is a JSON value, so it must be read and
// rejected for what it is rather than skipped for being too short and reported as
// a body that was never there.
func TestDefinitionJSONReadsABodyOfEveryLength(t *testing.T) {
	for name, document := range map[string]string{
		"one byte":   `{"kind":"loop","id":"l","condition":"c","body":1}`,
		"many bytes": `{"kind":"loop","id":"l","condition":"c","body":{"kind":"leaf"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			var spec Spec
			err := json.Unmarshal([]byte(document), &spec)
			if err == nil || !strings.Contains(err.Error(), "/body") {
				t.Fatalf("Unmarshal error = %v; want one located at /body", err)
			}
		})
	}
}

func TestDecodeDefinitionInt(t *testing.T) {
	const signBit = strconv.IntSize - 1
	platformMax := uint64(1)<<signBit - 1
	tests := map[string]struct {
		data    string
		want    int
		wantErr string
	}{
		"absent":  {},
		"null":    {data: "null"},
		"integer": {data: "-2", want: -2},
		"decimal": {data: "2.0", want: 2},
		"exponent": {
			data: "1e1",
			want: 10,
		},
		"fraction": {data: "1.5", wantErr: "is not an integer"},
		"string":   {data: `"1"`, wantErr: "expected integer, got string"},
		"overflow": {data: "1e1000", wantErr: "overflows int"},
		"bounded huge exponent": {
			data:    "1e1000000000",
			wantErr: "overflows int",
		},
		"exponent overflow": {
			data:    "1e999999999999999999999",
			wantErr: "overflows int",
		},
		"malformed": {
			data:    "1e",
			wantErr: "unexpected EOF",
		},
		"minimum": {
			data: "-" + strconv.FormatUint(platformMax+1, 10),
			want: -1 << signBit,
		},
		"negative platform overflow": {
			data:    "-" + strconv.FormatUint(platformMax+2, 10),
			wantErr: "overflows int",
		},
		"positive platform overflow": {
			data:    strconv.FormatUint(platformMax+1, 10),
			wantErr: "overflows int",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := decodeDefinitionInt("limit", json.RawMessage(test.data))
			if test.wantErr == "" {
				if err != nil || got != test.want {
					t.Fatalf("decodeDefinitionInt = %d, %v; want %d, nil", got, err, test.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("decodeDefinitionInt error = %v; want %q", err, test.wantErr)
			}
		})
	}
}

// TestSpecDecodingLocatesTheChildAJSONSchemaCannotReach names the one nested
// failure the embedded schema cannot express, and which the decoder therefore has
// to locate itself: a JSON integer the schema accepts as an integer and Go cannot
// represent. Everything else a nested Spec can get wrong is refused by the schema,
// which already points at the child with a JSON Pointer.
//
// That single class is the whole reason specJSONDecoder shadows steps, cases, and
// body with raw members rather than letting the embedded Spec fields decode them:
// renaming any of those three tags decodes valid documents identically, because
// encoding/json then reaches the embedded field of the same name, and only the
// location of this failure changes.
func TestSpecDecodingLocatesTheChildAJSONSchemaCannotReach(t *testing.T) {
	const unrepresentable = `{"kind":"parallel","concurrency":1e400}`
	tests := map[string]struct {
		document string
		want     string
	}{
		"a sequence child": {
			document: `{"kind":"sequence","steps":[{"kind":"sequence"},` + unrepresentable + `]}`,
			want:     `steps[1]: json field concurrency:`,
		},
		"a branch case": {
			document: `{"kind":"branch","id":"b","resolver":"r","cases":` +
				`{"a":{"kind":"sequence"},"z":` + unrepresentable + `}}`,
			want: `case "z": json field concurrency:`,
		},
		"a loop body": {
			document: `{"kind":"loop","id":"l","condition":"c","body":` + unrepresentable + `}`,
			want:     `body: json field concurrency:`,
		},
		// The root has no child position to report, so the same condition arrives
		// with nothing in front of it.
		"the root itself": {
			document: unrepresentable,
			want:     `field json: json field concurrency:`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var spec Spec
			err := json.Unmarshal([]byte(test.document), &spec)
			if err == nil {
				t.Fatal("decoded an integer Go cannot represent")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v; want substring %q", err, test.want)
			}
		})
	}
}
