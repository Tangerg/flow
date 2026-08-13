package workflow

import (
	"encoding/json"
	"fmt"
)

var (
	_ json.Marshaler   = Ref{}
	_ json.Unmarshaler = (*Ref)(nil)
)

// refJSON keeps MarshalJSON from recursing while retaining the wire members.
type refJSON Ref

// A Ref is decoded inside definitions and inside persisted waits. Only the
// definition path is covered by a JSON Schema, so the member contract is stated
// here rather than left to encoding/json, whose case folding would let
// "NODEID" silently satisfy nodeID.
const (
	refFieldNodeID = "nodeID"
	refFieldPath   = "path"
)

// MarshalJSON encodes a Ref without letting encoding/json change what it names.
// Every identity-bearing type in this package validates its text before
// encoding, because encoding/json replaces invalid UTF-8 by design. A Ref needs
// that on its own account, not only inside a definition: [Graph.Inputs] and
// [Graph.MissingInputs] hand callers bare references, and an editor that
// serializes one would otherwise persist a different reference than it read.
func (r Ref) MarshalJSON() ([]byte, error) {
	if err := r.validateJSONText(); err != nil {
		return nil, fmt.Errorf("workflow: marshal ref: %w", err)
	}
	return marshalJSON(refJSON(r))
}

// UnmarshalJSON atomically replaces a Ref from its strict canonical object. Both
// members are required, and unknown, duplicate, or noncanonical names are
// rejected. The reference is not validated here: an unset Ref is meaningful in
// several definition fields, and each owner reports its own field path.
func (r *Ref) UnmarshalJSON(data []byte) error {
	return decodeJSONInto(r, data, decodeRef, unmarshalError("ref"))
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
