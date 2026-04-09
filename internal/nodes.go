package workflow

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"

	"github.com/Tangerg/flow"
	"github.com/samber/lo"
)

type NodeType string

func (n NodeType) String() string {
	return string(n)
}

type Node interface {
	ID() string
	Type() NodeType
	Metadata() map[string]any
	Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error]
}

const (
	NodeTypeBranch    NodeType = "BranchNode"
	NodeTypeIteration NodeType = "IterationNode"
	NodeTypeLoop      NodeType = "LoopNode"
	NodeTypeParallel  NodeType = "ParallelNode"
	NodeTypeSequence  NodeType = "SequenceNode"
)

// ==================== BranchNode ====================

type BranchNodeConfig struct {
	ID             string
	Branches       []Node
	BranchResolver func(context.Context, ValueStore) string
}

func (cfg *BranchNodeConfig) validate() error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if cfg.ID == "" {
		return errors.New("empty ID")
	}
	if len(cfg.Branches) == 0 {
		return errors.New("empty branches")
	}
	uniqNodes := lo.UniqBy(cfg.Branches, func(node Node) string {
		return node.ID()
	})
	if len(uniqNodes) < len(cfg.Branches) {
		return errors.New("duplicate node IDs in branches")
	}
	if cfg.BranchResolver == nil && len(cfg.Branches) > 1 {
		return errors.New("empty branch resolver")
	}
	return nil
}

var _ Node = (*BranchNode)(nil)

type BranchNode struct {
	config   BranchNodeConfig
	metadata map[string]any
}

func NewBranchNode(cfg BranchNodeConfig) (*BranchNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &BranchNode{
		config: cfg,
		metadata: map[string]any{
			"id":   cfg.ID,
			"type": NodeTypeBranch,
			"branches": lo.Map(cfg.Branches, func(node Node, _ int) map[string]any {
				return map[string]any{
					"branch": node.ID(),
					"node":   node.Metadata(),
				}
			}),
		},
	}, nil
}

func (b *BranchNode) ID() string               { return b.config.ID }
func (b *BranchNode) Type() NodeType           { return NodeTypeBranch }
func (b *BranchNode) Metadata() map[string]any { return maps.Clone(b.metadata) }

func (b *BranchNode) Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		b.run(ctx, store, yield)
	}
}

func (b *BranchNode) run(ctx context.Context, store ValueStore, yield func(Event, error) bool) {
	branchs := map[string]func(context.Context, ValueStore) (any, error){}

	for _, node := range b.config.Branches {
		branchs[node.ID()] = func(ctx context.Context, pool ValueStore) (any, error) {
			for event, err := range node.Run(ctx, pool) {
				if !yield(event, err) {
					return nil, err
				}
			}
			return nil, nil
		}
	}

	branch, err := flow.
		NewBranchBuilder[ValueStore, any]().
		WithBranchResolver(b.config.BranchResolver).
		WithBranches(branchs).
		Build()
	if err != nil {
		yield(nil, err)
		return
	}

	_, _ = branch.Run(ctx, store)
}

// ==================== IterationNode ====================

type IterationNodeConfig struct {
	ID               string
	Node             Node
	ContinueOnError  bool
	ConcurrencyLimit int
}

func (cfg *IterationNodeConfig) validate() error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if cfg.ID == "" {
		return errors.New("empty ID")
	}
	if cfg.Node == nil {
		return errors.New("nil node")
	}
	return nil
}

var _ Node = (*IterationNode)(nil)

type IterationNode struct {
	config   IterationNodeConfig
	metadata map[string]any
}

func NewIterationNode(cfg IterationNodeConfig) (*IterationNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &IterationNode{
		config: cfg,
		metadata: map[string]any{
			"id":                cfg.ID,
			"type":              NodeTypeIteration,
			"node":              cfg.Node.Metadata(),
			"concurrency_limit": cfg.ConcurrencyLimit,
			"continue_on_error": cfg.ContinueOnError,
		},
	}, nil
}

func (i *IterationNode) ID() string               { return i.config.ID }
func (i *IterationNode) Type() NodeType           { return NodeTypeIteration }
func (i *IterationNode) Metadata() map[string]any { return maps.Clone(i.metadata) }

func (i *IterationNode) Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		i.run(ctx, store, yield)
	}
}

func (i *IterationNode) run(ctx context.Context, store ValueStore, yield func(Event, error) bool) {
	value, err := store.Get(i.ID(), "input")
	if err != nil {
		yield(nil, fmt.Errorf("failed to get input: %w", err))
		return
	}

	arrValue, ok := value.(*ArrayValue)
	if !ok {
		yield(nil, fmt.Errorf("input is not an array, got %s", value.Type()))
		return
	}

	innerNodeID := i.config.Node.ID()
	processor := func(ctx context.Context, index int, value Value) (Value, error) {
		clonedstore := store.Clone()
		_ = clonedstore.Set(innerNodeID, "index", NewNumberValue(float64(index)))
		_ = clonedstore.Set(innerNodeID, "input", value)

		for event, err := range i.config.Node.Run(ctx, clonedstore) {
			if !yield(event, err) {
				return nil, err
			}
		}

		return clonedstore.Get(i.config.Node.ID(), "output")
	}

	iteration, err := flow.
		NewIterationBuilder[Value, Value]().
		WithConcurrencyLimit(i.config.ConcurrencyLimit).
		WithContinueOnError(i.config.ContinueOnError).
		WithProcessor(processor).
		Build()
	if err != nil {
		yield(nil, err)
		return
	}

	rawArray, ok := arrValue.Raw().([]Value)
	if !ok {
		yield(nil, fmt.Errorf("internal error: array raw type mismatch"))
		return
	}

	results, err := iteration.Run(ctx, rawArray)
	if err != nil {
		yield(nil, err)
		return
	}

	outputValues := make([]Value, 0, len(results))
	for _, result := range results {
		outputValues = append(outputValues, result.Value)
	}

	_ = store.Set(i.ID(), "output", NewArrayValue(outputValues...))
}

// ==================== LoopNode ====================

type LoopNodeConfig struct {
	ID            string
	Node          Node
	MaxIterations int
	Terminator    func(context.Context, int, ValueStore) bool
}

func (cfg *LoopNodeConfig) validate() error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if cfg.ID == "" {
		return errors.New("empty ID")
	}
	if cfg.Node == nil {
		return errors.New("nil node")
	}
	if cfg.MaxIterations < 0 {
		return errors.New("invalid MaxIterations")
	}
	if cfg.MaxIterations == 0 {
		cfg.MaxIterations = 10
	}
	if cfg.Terminator == nil {
		cfg.Terminator = func(context.Context, int, ValueStore) bool { return true }
	}
	return nil
}

var _ Node = (*LoopNode)(nil)

type LoopNode struct {
	config   LoopNodeConfig
	metadata map[string]any
}

func NewLoopNode(cfg LoopNodeConfig) (*LoopNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &LoopNode{
		config: cfg,
		metadata: map[string]any{
			"id":             cfg.ID,
			"type":           NodeTypeLoop,
			"max_iterations": cfg.MaxIterations,
			"node":           cfg.Node.Metadata(),
		},
	}, nil
}

func (l *LoopNode) ID() string               { return l.config.ID }
func (l *LoopNode) Type() NodeType           { return NodeTypeLoop }
func (l *LoopNode) Metadata() map[string]any { return maps.Clone(l.metadata) }

func (l *LoopNode) Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		l.run(ctx, store, yield)
	}
}

func (l *LoopNode) run(ctx context.Context, store ValueStore, yield func(Event, error) bool) {
	processor := func(ctx context.Context, iteration int, variables ValueStore) (ValueStore, bool, error) {
		for event, err := range l.config.Node.Run(ctx, variables) {
			if !yield(event, err) {
				return variables, true, err
			}
		}
		return variables, l.config.Terminator(ctx, iteration, variables), nil
	}

	loop, err := flow.
		NewLoopBuilder[ValueStore]().
		WithMaxIterations(l.config.MaxIterations).
		WithProcessor(processor).
		Build()
	if err != nil {
		yield(nil, err)
		return
	}

	_, _ = loop.Run(ctx, store)
}

// ==================== ParallelNode ====================

type ParallelNodeConfig struct {
	ID               string
	Nodes            []Node
	ContinueOnError  bool
	ConcurrencyLimit int
}

func (cfg *ParallelNodeConfig) validate() error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if cfg.ID == "" {
		return errors.New("empty ID")
	}
	if len(cfg.Nodes) == 0 {
		return errors.New("parallel must contain at least one node")
	}
	return nil
}

var _ Node = (*ParallelNode)(nil)

type ParallelNode struct {
	config   ParallelNodeConfig
	metadata map[string]any
}

func NewParallelNode(cfg ParallelNodeConfig) (*ParallelNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &ParallelNode{
		config: cfg,
		metadata: map[string]any{
			"id":                cfg.ID,
			"type":              NodeTypeParallel,
			"continue_on_error": cfg.ContinueOnError,
			"concurrency_limit": cfg.ConcurrencyLimit,
			"count":             len(cfg.Nodes),
			"nodes": lo.Map(cfg.Nodes, func(node Node, _ int) map[string]any {
				return node.Metadata()
			}),
		},
	}, nil
}

func (p *ParallelNode) ID() string               { return p.config.ID }
func (p *ParallelNode) Type() NodeType           { return NodeTypeParallel }
func (p *ParallelNode) Metadata() map[string]any { return maps.Clone(p.metadata) }

func (p *ParallelNode) Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		p.run(ctx, store, yield)
	}
}

func (p *ParallelNode) run(ctx context.Context, store ValueStore, yield func(Event, error) bool) {
	processors := make([]func(context.Context, ValueStore) (any, error), 0, len(p.config.Nodes))
	for _, node := range p.config.Nodes {
		processor := func(ctx context.Context, store ValueStore) (any, error) {
			for event, err := range node.Run(ctx, store) {
				if !yield(event, err) {
					return nil, err
				}
			}
			return nil, nil
		}
		processors = append(processors, processor)
	}

	parallel, err := flow.
		NewParallelBuilder[ValueStore, any]().
		WithConcurrencyLimit(p.config.ConcurrencyLimit).
		WithContinueOnError(p.config.ContinueOnError).
		WithProcessors(processors).
		Build()
	if err != nil {
		yield(nil, err)
		return
	}

	_, _ = parallel.Run(ctx, store)
}

// ==================== SequenceNode ====================

type SequenceNodeConfig struct {
	ID    string
	Nodes []Node
}

func (cfg *SequenceNodeConfig) validate() error {
	if cfg == nil {
		return errors.New("nil config")
	}
	if cfg.ID == "" {
		return errors.New("empty ID")
	}
	if len(cfg.Nodes) == 0 {
		return errors.New("sequence node must have at least one node")
	}
	return nil
}

var _ Node = (*SequenceNode)(nil)

type SequenceNode struct {
	config   SequenceNodeConfig
	metadata map[string]any
}

func NewSequenceNode(cfg SequenceNodeConfig) (*SequenceNode, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &SequenceNode{
		config: cfg,
		metadata: map[string]any{
			"id":    cfg.ID,
			"type":  NodeTypeSequence,
			"count": len(cfg.Nodes),
			"nodes": lo.Map(cfg.Nodes, func(node Node, _ int) map[string]any {
				return node.Metadata()
			}),
		},
	}, nil
}

func (s *SequenceNode) ID() string               { return s.config.ID }
func (s *SequenceNode) Type() NodeType           { return NodeTypeSequence }
func (s *SequenceNode) Metadata() map[string]any { return maps.Clone(s.metadata) }

func (s *SequenceNode) Run(ctx context.Context, store ValueStore) iter.Seq2[Event, error] {
	return func(yield func(Event, error) bool) {
		for _, node := range s.config.Nodes {
			for event, err := range node.Run(ctx, store) {
				if !yield(event, err) {
					return
				}
			}
		}
	}
}
