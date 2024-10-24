package listener

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pglogrepl"
	"github.com/valyala/fastjson"
)

type KeyType struct {
	Key  string `json:"key"`
	Type string `json:"type"`
}

type KeyValue struct {
	KeyType
	Value *interface{} `json:"value"`
}

type WalChangeAction string

const (
	ActionInsert   WalChangeAction = "I"
	ActionUpdate   WalChangeAction = "U"
	ActionDelete   WalChangeAction = "D"
	ActionTruncate WalChangeAction = "T"
	ActionBegin    WalChangeAction = "B"
	ActionCommit   WalChangeAction = "C"
)

type WalChangeBase struct {
	Action    WalChangeAction `json:"action"`
	LSN       pglogrepl.LSN   `json:"lsn"`
	Timestamp time.Time       `json:"timestamp"`
}

type WalChange interface {
	IsWalChange()
}

type WalChangeInsert struct {
	WalChangeBase
	Columns *[]KeyValue `json:"columns"`
	PK      *[]KeyType  `json:"pk"`
	Schema  string      `json:"schema"`
	Table   string      `json:"table"`
}

func (w *WalChangeInsert) IsWalChange() {}

type WalChangeUpdate struct {
	WalChangeBase
	Columns  *[]KeyValue `json:"new"`
	Identity *[]KeyValue `json:"identity"`
	PK       *[]KeyType  `json:"pk"`
	Schema   string      `json:"schema"`
	Table    string      `json:"table"`
}

func (w *WalChangeUpdate) IsWalChange() {}

type WalChangeDelete struct {
	WalChangeBase
	Schema   string      `json:"schema"`
	Table    string      `json:"table"`
	PK       *[]KeyType  `json:"pk"`
	Identity *[]KeyValue `json:"identity"`
}

func (w *WalChangeDelete) IsWalChange() {}

type WalChangeTruncate struct {
	WalChangeBase
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

func (w *WalChangeTruncate) IsWalChange() {}

type WalChangeBegin struct {
	WalChangeBase
	NextLSN pglogrepl.LSN `json:"nextlsn"`
}

func (w *WalChangeBegin) IsWalChange() {}

type WalChangeCommit struct {
	WalChangeBase
	NextLSN pglogrepl.LSN `json:"nextlsn"`
}

func (w *WalChangeCommit) IsWalChange() {}

func parseKeyValues(columns []*fastjson.Value) *[]KeyValue {
	kvs := make([]KeyValue, len(columns))
	for i, column := range columns {
		var value interface{} = nil
		if column != nil {
			switch column.Type() {
			case fastjson.TypeNumber:
				value = column.GetFloat64()
			case fastjson.TypeString:
				value = string(column.GetStringBytes())
			case fastjson.TypeTrue:
				value = true
			case fastjson.TypeFalse:
				value = false
			case fastjson.TypeNull:
				value = nil
			}
		}

		ref := &value
		if value == nil {
			ref = nil
		}

		kvs[i] = KeyValue{
			KeyType: KeyType{
				Key:  string(column.GetStringBytes()),
				Type: string(column.GetStringBytes()),
			},
			Value: ref,
		}
	}
	return &kvs
}

func parseKeyTypes(pks []*fastjson.Value) *[]KeyType {
	kts := make([]KeyType, len(pks))
	for i, pk := range pks {
		kts[i] = KeyType{
			Key:  string(pk.GetStringBytes()),
			Type: string(pk.GetStringBytes()),
		}
	}
	return &kts
}

// Add this new function to parse WAL changes
func ParseWalChange(parsed *fastjson.Value) (WalChange, error) {
	action := WalChangeAction(string(parsed.GetStringBytes("action")))
	lsn := string(parsed.GetStringBytes("lsn"))
	parsedLsn, err := pglogrepl.ParseLSN(lsn)
	if err != nil {
		return nil, errors.WithDetail(err, "failed to parse LSN")
	}

	timestamp, err := time.Parse("2006-01-02 15:04:05.999999+00", string(parsed.GetStringBytes("timestamp")))
	if err != nil {
		return nil, errors.WithDetail(err, "failed to parse timestamp")
	}

	changeBase := WalChangeBase{
		Action:    action,
		LSN:       parsedLsn,
		Timestamp: timestamp,
	}

	var change WalChange

	switch action {
	case ActionInsert:
		change = &WalChangeInsert{
			WalChangeBase: changeBase,
			Columns:       parseKeyValues(parsed.GetArray("columns")),
			PK:            parseKeyTypes(parsed.GetArray("pk")),
			Schema:        string(parsed.GetStringBytes("schema")),
			Table:         string(parsed.GetStringBytes("table")),
		}
	case ActionUpdate:
		change = &WalChangeUpdate{
			WalChangeBase: changeBase,
			Columns:       parseKeyValues(parsed.GetArray("columns")),
			Identity:      parseKeyValues(parsed.GetArray("identity")),
			PK:            parseKeyTypes(parsed.GetArray("pk")),
			Schema:        string(parsed.GetStringBytes("schema")),
			Table:         string(parsed.GetStringBytes("table")),
		}
	case ActionDelete:
		change = &WalChangeDelete{
			WalChangeBase: changeBase,
			PK:            parseKeyTypes(parsed.GetArray("pk")),
			Identity:      parseKeyValues(parsed.GetArray("identity")),
			Schema:        string(parsed.GetStringBytes("schema")),
			Table:         string(parsed.GetStringBytes("table")),
		}
	case ActionTruncate:
		change = &WalChangeTruncate{
			WalChangeBase: changeBase,
			Schema:        string(parsed.GetStringBytes("schema")),
			Table:         string(parsed.GetStringBytes("table")),
		}
	case ActionBegin:
		nextLsn := string(parsed.GetStringBytes("nextlsn"))
		parsedNextLsn, err := pglogrepl.ParseLSN(nextLsn)
		if err != nil {
			return nil, errors.WithDetail(err, "failed to parse next LSN")
		}
		change = &WalChangeBegin{
			WalChangeBase: changeBase,
			NextLSN:       parsedNextLsn,
		}
	case ActionCommit:
		nextLsn := string(parsed.GetStringBytes("nextlsn"))
		parsedNextLsn, err := pglogrepl.ParseLSN(nextLsn)
		if err != nil {
			return nil, errors.WithDetail(err, "failed to parse next LSN")
		}
		change = &WalChangeCommit{
			WalChangeBase: changeBase,
			NextLSN:       parsedNextLsn,
		}
	default:
		return nil, errors.Newf("unknown message type: %v", parsed)
	}

	return change, nil
}
