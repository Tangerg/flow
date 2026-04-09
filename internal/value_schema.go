package workflow

import (
	"context"
	"errors"
)

type ValueSchema struct {
	Name        string           `json:"name"`
	Description string           `json:"description"`
	Source      ValueSource      `json:"source"`
	Validators  []ValueValidator `json:"validators"`
}

func (v *ValueSchema) Resolve(ctx context.Context, store ValueStore) (Value, error) {
	if v == nil {
		return nil, errors.New("nil ValueSchema")
	}
	if v.Source == nil {
		return nil, errors.New("nil ValueSource")
	}
	value, err := v.Source.Resolve(ctx, store)
	if err != nil {
		return nil, err
	}
	for _, validator := range v.Validators {
		err = validator.Validate(value)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}
