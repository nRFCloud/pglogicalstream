package benthos

import (
	"context"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type PeekBatch struct {
	batcher *service.Batcher
	count   int
	policy  *service.BatchPolicy
}

func NewPeekBatch(policy *service.BatchPolicy, resources *service.Resources) (*PeekBatch, error) {
	batcher, err := policy.NewBatcher(resources)
	if err != nil {
		return nil, err
	}

	return &PeekBatch{
		batcher: batcher,
		count:   0,
		policy:  policy,
	}, nil
}

func (p *PeekBatch) Count() int {
	return p.count
}

func (p *PeekBatch) Room() int {
	return p.policy.Count - p.Count()
}

func (p *PeekBatch) IsFull() bool {
	return p.Room() <= 0
}

func (p *PeekBatch) WillTrigger() bool {
	return p.Room() <= 1
}

func (p *PeekBatch) Flush(ctx context.Context) (service.MessageBatch, error) {
	batch, err := p.batcher.Flush(ctx)
	if err == nil {
		p.count = 0 // Reset count after flushing
	}
	return batch, err
}

func (p *PeekBatch) Close(ctx context.Context) error {
	return p.batcher.Close(ctx)
}

func (p *PeekBatch) Add(msg *service.Message) bool {
	p.count++
	return p.batcher.Add(msg)
}

func (p *PeekBatch) AddIsLast(msg *service.Message) bool {
	p.Add(msg)
	return p.WillTrigger()
}

func (p *PeekBatch) UntilNext() (time.Duration, bool) {
	return p.batcher.UntilNext()
}
