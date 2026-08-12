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
	next, err := decodeRef(data)
	if err != nil {
		return fmt.Errorf("workflow: unmarshal ref: %w", err)
	}
	*r = next
	return nil
}

// decodeRef reads the strict canonical object. Each return names only its own
// condition: UnmarshalJSON owns the context, so stating the prefix once there
// keeps it from being repeated at every failure, and returning a value makes the
// atomic replacement above the only way the receiver changes.
func decodeRef(data []byte) (Ref, error) {
	raw, err := jsonDocument(data).object()
	if err != nil {
		return Ref{}, err
	}

	object := jsonObject(raw)
	if err = object.allow(refFieldNodeID, refFieldPath); err != nil {
		return Ref{}, err
	}
	if err = object.require("ref", refFieldNodeID, refFieldPath); err != nil {
		return Ref{}, err
	}
	nodeID, err := object.stringMember(refFieldNodeID)
	if err != nil {
		return Ref{}, err
	}
	path, err := object.stringMember(refFieldPath)
	if err != nil {
		return Ref{}, err
	}
	return Ref{NodeID: nodeID, Path: path}, nil
}
