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
	listener *listener.Stream,
	deathChan chan struct{},
) {
	k.mgr.Logger().Debugf("Consuming messages from wal log\n")
	defer k.mgr.Logger().Debugf("Stopped consuming messages from wal log\n")
	defer close(deathChan)

	batchPolicy, err := k.batching.NewBatcher(k.mgr)
	if err != nil {
		k.mgr.Logger().Errorf("Failed to initialise batch policy: %v, falling back to no policy.\n", err)
		conf := service.BatchPolicy{Count: 1}
		if batchPolicy, err = conf.NewBatcher(k.mgr); err != nil {
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

	latestOffset := listener.GetRestartLSN()

	k.mgr.Logger().Tracef("Benthos consumer got initial offset: %v\n", latestOffset)

partMsgLoop:
	for {
		if nextTimedBatchChan == nil {
			if tNext, exists := batchPolicy.UntilNext(); exists {
				nextTimedBatchChan = time.After(tNext)
			}
		}
		select {
		case <-nextTimedBatchChan:
			nextTimedBatchChan = nil
			flushedBatch, err := batchPolicy.Flush(ctx)
			if err != nil {
				k.mgr.Logger().Debugf("Timed flush batch error: %w", err)
				break partMsgLoop
			}
			if !flushBatch(ctx, k.msgChan, flushedBatch, latestOffset) {
				break partMsgLoop
			}
		case data, open := <-listener.ChangeChannel:
			if !open || data.Error != nil {
				if data.Error != nil {
					k.mgr.Logger().Errorf("Wal message recv error: %v\n", data.Error)
				}
				break partMsgLoop
			}

			for i, msg := range data.Change.Changes {
				// Only update the offset if we are the last message
				// This is to prevent commiting the offset before all messages from the given LSN are processed
				if i == len(data.Change.Changes)-1 {
					latestOffset = data.Change.Lsn
				}
				part := dataToPart(data.Change.Lsn, &msg)
				if batchPolicy.Add(part) {
					k.mgr.Logger().Tracef("Flushing message batch\n")
					nextTimedBatchChan = nil
					flushedBatch, err := batchPolicy.Flush(ctx)
					if err != nil {
						k.mgr.Logger().Errorf("Flush batch error: %w", err)
						break partMsgLoop
					}
					if !flushBatch(ctx, k.msgChan, flushedBatch, latestOffset) {
						k.mgr.Logger().Errorf("Failed to flush batch: %v\n", err)
						break partMsgLoop
					}
				}
			}
		case <-ctx.Done():
			break partMsgLoop
		}
	}

	k.mgr.Logger().Errorf("Consumer has been closed")
}

func (k *pgWalReader) connectExplicitTopics(ctx context.Context, streamConfig *listener.PgStreamConfig) (err error) {
	var consumer *listener.Stream
	streamConfig.ChangeChannel = make(chan listener.WalMessage)

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

	// for topic, partitions := range k.topicPartitions {
	// 	for _, partition := range partitions {
	// 		topic := topic
	// 		partition := partition

	// 		offset := sarama.OffsetNewest
	// 		if k.startFromOldest {
	// 			offset = sarama.OffsetOldest
	// 		}
	// 		if block := offsetRes.GetBlock(topic, partition); block != nil {
	// 			if block.Err == sarama.ErrNoError {
	// 				if block.Offset > 0 {
	// 					offset = block.Offset
	// 				}
	// 			} else {
	// 				k.mgr.Logger().Debugf("Failed to acquire offset for topic %v partition %v: %v\n", topic, partition, block.Err)
	// 			}
	// 		} else {
	// 			k.mgr.Logger().Debugf("Failed to acquire offset for topic %v partition %v\n", topic, partition)
	// 		}

	// 		var partConsumer sarama.PartitionConsumer
	// 		if partConsumer, err = consumer.ConsumePartition(topic, partition, offset); err != nil {
	// 			// TODO: Actually verify the error was caused by a non-existent offset
	// 			if k.startFromOldest {
	// 				offset = sarama.OffsetOldest
	// 				k.mgr.Logger().Warnf("Failed to read from stored offset, restarting from oldest offset: %v\n", err)
	// 			} else {
	// 				offset = sarama.OffsetNewest
	// 				k.mgr.Logger().Warnf("Failed to read from stored offset, restarting from newest offset: %v\n", err)
	// 			}
	// 			if partConsumer, err = consumer.ConsumePartition(topic, partition, offset); err != nil {
	// 				doneFn()
	// 				return fmt.Errorf("failed to consume topic %v partition %v: %v", topic, partition, err)
	// 			}
	// 		}

	// 		consumerWG.Add(1)
	// 		partConsumers = append(partConsumers, partConsumer)
	// 		go k.runPartitionConsumer(ctx, &consumerWG, topic, partition, partConsumer)
	// 	}
	// }

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
