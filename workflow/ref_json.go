package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
)

var _ json.Unmarshaler = (*Ref)(nil)

// A Ref is decoded inside definitions and inside persisted waits. Only the
// definition path is covered by a JSON Schema, so the member contract is stated
// here rather than left to encoding/json, whose case folding would let
// "NODEID" silently satisfy nodeID.
const (
	refFieldNodeID = "nodeID"
	refFieldPath   = "path"
)

// UnmarshalJSON atomically replaces a Ref from its strict canonical object. Both
// members are required, and unknown, duplicate, or noncanonical names are
// rejected. The reference is not validated here: an unset Ref is meaningful in
// several definition fields, and each owner reports its own field path.
func (r *Ref) UnmarshalJSON(data []byte) error {
	if r == nil {
		return errors.New("workflow: unmarshal ref: nil ref")
	}
	raw, err := jsonDocument(data).object()
	if err != nil {
		return fmt.Errorf("workflow: unmarshal ref: %w", err)
	}

	object := jsonObject(raw)
	if err := object.allow(refFieldNodeID, refFieldPath); err != nil {
		return fmt.Errorf("workflow: unmarshal ref: %w", err)
	}
	if err := object.require("ref", refFieldNodeID, refFieldPath); err != nil {
		return fmt.Errorf("workflow: unmarshal ref: %w", err)
	}
	nodeID, ok := object[refFieldNodeID].(string)
	if !ok {
		return errors.New("workflow: unmarshal ref: nodeID must be a string")
	}
	path, ok := object[refFieldPath].(string)
	if !ok {
		return errors.New("workflow: unmarshal ref: path must be a string")
	}
	*r = Ref{NodeID: nodeID, Path: path}
	return nil
}
