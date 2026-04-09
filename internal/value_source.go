package workflow

import "context"

type ValueSource interface {
	Resolve(ctx context.Context, store ValueStore) (Value, error)
}

type ValueSourcePath struct {
}

type ValueSourceConst struct {
	value Value
}

func (v *ValueSourceConst) Resolve(ctx context.Context, store ValueStore) (Value, error) {
	return v.value, nil
}
