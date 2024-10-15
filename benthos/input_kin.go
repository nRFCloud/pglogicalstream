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
	"errors"
	"os"
	"pglogicalstream/listener"
	"sync"
	"time"

	"github.com/Jeffail/checkpoint"
	"github.com/charmbracelet/log"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	iskFieldDSN             = "dsn"
	iskFieldTables          = "tables"
	iskFieldSlot            = "slot"
	iskFieldCheckpointLimit = "checkpoint_limit"
	iskFieldCommitPeriod    = "commit_period"
	iskFieldBatching        = "batching"
)

func pgwalConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Connects to a postgres instance using logical decoding to consume WAL`).Fields(
		service.NewAutoRetryNacksToggleField(),
		service.NewStringField("dsn"),
		service.NewStringListField("tables").
			Description("A list of tables to replicate.").
			Example([]string{"public.table1", "public.table2"}),
		service.NewStringField("slot").
			Description("The name of the slot to create and replicate from.").
			Example("benthos_slot"),
		service.NewIntField("checkpoint_limit").
			Description("The maximum number of messages to consume before committing the offset."),
		service.NewDurationField("commit_period").
			Description("The period at which to commit the offset."),
		service.NewBatchPolicyField(iskFieldBatching).Advanced(),
	)
}

func init() {
	err := service.RegisterBatchInput("pg_wal", pgwalConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		i, err := newPGWalReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}

		r, err := service.AutoRetryNacksBatchedToggled(conf, i)
		if err != nil {
			return nil, err
		}

		return conf.WrapBatchInputExtractTracingSpanMapping("pg_wal", r)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type offsetMarker interface {
	MarkOffset(offset pglogrepl.LSN)
}

type pgWalReader struct {
	dsn             string
	batching        service.BatchPolicy
	listenerConfig  *listener.PgStreamConfig
	checkpointLimit int
	commitPeriod    time.Duration
	pgConnConfig    *pgx.ConnConfig
	slotName        string
	// Connection resources
	cMut            sync.Mutex
	consumerCloseFn context.CancelFunc
	consumerDoneCtx context.Context
	msgChan         chan asyncMessage
	session         offsetMarker

	mgr *service.Resources

	closeOnce  sync.Once
	closedChan chan struct{}
}

func newPGWalReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*pgWalReader, error) {
	k := pgWalReader{
		consumerCloseFn: nil,
		mgr:             mgr,
		closedChan:      make(chan struct{}),
	}
	var err error

	if k.batching, err = conf.FieldBatchPolicy(iskFieldBatching); err != nil {
		return nil, err
	} else if k.batching.IsNoop() {
		k.batching.Count = 1
	}

	tables, err := conf.FieldStringList(iskFieldTables)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, errors.New("must specify at least one table in the tables field")
	}

	if k.checkpointLimit, err = conf.FieldInt(iskFieldCheckpointLimit); err != nil {
		return nil, err
	}
	if k.commitPeriod, err = conf.FieldDuration(iskFieldCommitPeriod); err != nil {
		return nil, err
	}
	if k.slotName, err = conf.FieldString(iskFieldSlot); err != nil {
		return nil, err
	}

	if k.slotName == "" {
		return nil, errors.New("a slot name must be specified")
	}

	if k.dsn, err = conf.FieldString(iskFieldDSN); err != nil {
		return nil, err
	}

	if k.pgConnConfig, err = pgx.ParseConfig(k.dsn); err != nil {
		return nil, err
	}

	k.listenerConfig = &listener.PgStreamConfig{
		SlotName: k.slotName,
		Tables:   tables,
		DbConfig: *k.pgConnConfig,
		BaseLogger: log.NewWithOptions(os.Stdout, log.Options{
			Level:        log.DebugLevel,
			ReportCaller: true,
		}),
	}

	return &k, nil
}

//------------------------------------------------------------------------------

func (k *pgWalReader) asyncCheckpointer() func(context.Context, chan<- asyncMessage, service.MessageBatch, pglogrepl.LSN) bool {
	cp := checkpoint.NewCapped[pglogrepl.LSN](int64(k.checkpointLimit))
	return func(ctx context.Context, c chan<- asyncMessage, msg service.MessageBatch, offset pglogrepl.LSN) bool {
		if msg == nil {
			return true
		}
		resolveFn, err := cp.Track(ctx, offset, int64(len(msg)))
		if err != nil {
			if ctx.Err() == nil {
				k.mgr.Logger().Errorf("Failed to checkpoint offset: %v\n", err)
			}
			return false
		}
		select {
		case c <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res error) error {
				maxOffset := resolveFn()
				if maxOffset == nil {
					return nil
				}
				k.cMut.Lock()
				if k.session != nil {
					k.mgr.Logger().Tracef("Marking LSN: %v", *maxOffset)
					k.session.MarkOffset(pglogrepl.LSN(*maxOffset))
				} else {
					k.mgr.Logger().Debugf("Unable to mark LSN: %v", *maxOffset)
				}
				k.cMut.Unlock()
				return nil
			},
		}:
		case <-ctx.Done():
			return false
		}
		return true
	}
}

func (k *pgWalReader) syncCheckpointer() func(context.Context, chan<- asyncMessage, service.MessageBatch, pglogrepl.LSN) bool {
	ackedChan := make(chan error)
	return func(ctx context.Context, c chan<- asyncMessage, msg service.MessageBatch, offset pglogrepl.LSN) bool {
		if msg == nil {
			return true
		}
		select {
		case c <- asyncMessage{
			msg: msg,
			ackFn: func(ctx context.Context, res error) error {
				resErr := res
				if resErr == nil {
					k.cMut.Lock()
					if k.session != nil {
						k.mgr.Logger().Debugf("Marking LSN: %v", offset)
						k.session.MarkOffset(offset)
					} else {
						k.mgr.Logger().Debugf("Unable to mark LSN: %v", offset)
					}
					k.cMut.Unlock()
				}
				select {
				case ackedChan <- resErr:
				case <-ctx.Done():
				}
				return nil
			},
		}:
			select {
			case resErr := <-ackedChan:
				if resErr != nil {
					k.mgr.Logger().Errorf("Received error from message batch: %v, shutting down consumer.\n", resErr)
					return false
				}
			case <-ctx.Done():
				return false
			}
		case <-ctx.Done():
			return false
		}
		return true
	}
}

func dataToPart(lsn pglogrepl.LSN, data *listener.WalChange) *service.Message {
	part := service.NewMessage(nil)
	part.SetStructured(data)

	part.MetaSetMut("lsn", lsn.String())
	part.MetaSetMut("table", data.Table)
	part.MetaSetMut("schema", data.Schema)

	return part
}

//------------------------------------------------------------------------------

func (k *pgWalReader) closeGroupAndConsumers() {
	k.cMut.Lock()
	consumerCloseFn := k.consumerCloseFn
	consumerDoneCtx := k.consumerDoneCtx
	k.cMut.Unlock()

	if consumerCloseFn != nil {
		k.mgr.Logger().Debug("Waiting for topic consumers to close.")
		consumerCloseFn()
		<-consumerDoneCtx.Done()
		k.mgr.Logger().Debug("Topic consumers are closed.")
	}

	k.closeOnce.Do(func() {
		close(k.closedChan)
	})
}

func (k *pgWalReader) Connect(ctx context.Context) error {
	k.cMut.Lock()
	defer k.cMut.Unlock()
	if k.msgChan != nil {
		return nil
	}

	return k.connectExplicitTopics(ctx, k.listenerConfig)
}

func (k *pgWalReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	k.cMut.Lock()
	msgChan := k.msgChan
	k.cMut.Unlock()

	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	k.mgr.Logger().Tracef("Benthos waiting for pgwal message")
	select {
	case m, open := <-msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		k.mgr.Logger().Tracef("Benthos read batch from pgwal")
		return m.msg, m.ackFn, nil
	case <-ctx.Done():
	}
	return nil, nil, ctx.Err()
}

func (k *pgWalReader) Close(ctx context.Context) (err error) {
	k.closeGroupAndConsumers()
	select {
	case <-k.closedChan:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}
