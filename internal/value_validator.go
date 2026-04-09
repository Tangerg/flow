package workflow

type ValueValidator interface {
	Validate(value Value) error
}
