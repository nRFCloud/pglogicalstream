// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benthos

import (
	"context"
	"pglogicalstream/listener"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type closureOffsetTracker struct {
	fn func(pglogrepl.LSN)
}

func (c *closureOffsetTracker) MarkOffset(offset pglogrepl.LSN) {
	c.fn(offset)
}

func (k *pgWalReader) runPartitionConsumer(
	ctx context.Context,
	stream *listener.Stream,
	deathChan chan struct{},
) {
	k.mgr.Logger().Debugf("Consuming messages from wal log\n")
	defer k.mgr.Logger().Debugf("Stopped consuming messages from wal log\n")
	defer close(deathChan)

	batchPolicy, err := NewPeekBatch(&k.batching, k.mgr)
	if err != nil {
		k.mgr.Logger().Errorf("Failed to initialise batch policy: %v, falling back to no policy.\n", err)
		conf := service.BatchPolicy{Count: 1}
		if batchPolicy, err = NewPeekBatch(&conf, k.mgr); err != nil {
			panic(err)
		}
	}
	defer batchPolicy.Close(context.Background())

	var nextTimedBatchChan <-chan time.Time
	var flushBatch func(context.Context, chan<- asyncMessage, service.MessageBatch, pglogrepl.LSN) bool
	if k.checkpointLimit > 1 {
		flushBatch = k.asyncCheckpointer()
	} else {
		flushBatch = k.syncCheckpointer()
	}

	latestOffset := stream.GetRestartLSN()

	k.mgr.Logger().Tracef("Benthos consumer got initial offset: %v\n", latestOffset)

	flushWrapper := func() bool {
		nextTimedBatchChan = nil
		flushedBatch, err := batchPolicy.Flush(ctx)
		if err != nil {
			k.mgr.Logger().Debugf("Timed flush batch error: %w", err)
			return false
		}
		return flushBatch(ctx, k.msgChan, flushedBatch, latestOffset)
	}

partMsgLoop:
	for {
		if nextTimedBatchChan == nil {
			if tNext, exists := batchPolicy.UntilNext(); exists {
				nextTimedBatchChan = time.After(tNext)
			}
		}
		select {
		case <-nextTimedBatchChan:
			k.mgr.Logger().Trace("Flushing timed batch")
			nextTimedBatchChan = nil
			if !flushWrapper() {
				break partMsgLoop
			}
		case data, open := <-stream.ChangeChannel:
			if !open {
				break partMsgLoop
			}
			k.mgr.Logger().Tracef("Received message from channel: %v, BufferMsgs: %v, IsFull: %v, Room: %v\n", data, batchPolicy.Count(), batchPolicy.IsFull(), batchPolicy.Room())

			switch v := data.(type) {
			case *listener.WalChangeCommit:
				latestOffset = v.NextLSN
				k.mgr.Logger().Tracef("Received commit message with offset: %v\n", latestOffset)

				// If the batch is already full, flush now
				if batchPolicy.IsFull() {
					k.mgr.Logger().Trace("Flushing commit batch")
					if !flushWrapper() {
						break partMsgLoop
					}
				}
				continue
			case *listener.WalChangeBegin:
				continue
			}

			msg := dataToPart(&data)

			if msg != nil {
				// If the batch is already full, flush now
				// This intentionally happens before adding the message to the batch
				// This prevents flushing the last message of a transaction before the commit
				if batchPolicy.IsFull() {
					if !flushWrapper() {
						break partMsgLoop
					}
				}

				batchPolicy.Add(msg)
			}
		case <-ctx.Done():
			break partMsgLoop
		}
	}

	k.mgr.Logger().Errorf("Consumer has been closed")
}

func (k *pgWalReader) connectExplicitTopics(ctx context.Context, streamConfig *listener.PgStreamConfig) (err error) {
	var consumer *listener.Stream
	streamConfig.ChangeChannel = make(chan listener.WalChange)

	defer func() {
		if err != nil {
			if consumer != nil {
				consumer.Stop()
			}
		}
	}()

	if consumer, err = listener.NewPgStream(context.Background(), *streamConfig); err != nil {
		return err
	}

	msgChan := make(chan asyncMessage)
	ctx, doneFn := context.WithCancel(context.Background())
	currentLSN := consumer.GetRestartLSN()

	offsetTracker := &closureOffsetTracker{
		fn: func(offset pglogrepl.LSN) {
			currentLSN = offset
		},
	}

	deathChan := make(chan struct{})
	go k.runPartitionConsumer(ctx, consumer, deathChan)

	doneCtx, doneFn := context.WithCancel(context.Background())
	go func() {
		defer doneFn()
		looping := true
		for looping {
			select {
			case <-ctx.Done():
				looping = false
			case <-deathChan:
				looping = false
			case <-time.After(k.commitPeriod):
			}
			k.cMut.Lock()
			putLSN := currentLSN
			k.cMut.Unlock()
			if err := consumer.AckLSN(putLSN); err != nil {
				k.mgr.Logger().Errorf("Failed to commit LSN: %v\n", err)
				looping = false
			}
		}
		consumer.Stop()
		if consumer.GetLastError() != nil {
			k.mgr.Logger().Errorf("Consumer stopped with error: %v\n", consumer.GetLastError())
		}
		k.mgr.Logger().Debugf("Commit loop stopped\n")
		k.cMut.Lock()
		if k.msgChan != nil {
			close(k.msgChan)

			k.msgChan = nil
		}
		k.cMut.Unlock()
	}()

	k.consumerCloseFn = doneFn
	k.consumerDoneCtx = doneCtx
	k.session = offsetTracker
	k.msgChan = msgChan
	return nil
}
