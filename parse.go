package pgoutput

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/jackc/pgx/pgtype"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

func (d *decoder) bool() bool {
	x := d.buf.Next(1)[0]
	return x != 0

}

func (d *decoder) uint8() uint8 {
	x := d.buf.Next(1)[0]
	return x

}

func (d *decoder) uint16() uint16 {
	x := d.order.Uint16(d.buf.Next(2))
	return x
}

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		// TODO: Return an error
		panic(err)
	}
	return string(s[:len(s)-1])
}

func (d *decoder) uint32() uint32 {
	x := d.order.Uint32(d.buf.Next(4))
	return x

}

func (d *decoder) uint64() uint64 {
	x := d.order.Uint64(d.buf.Next(8))
	return x
}

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowinfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	} else {
		d.buf.UnreadByte()
		return false
	}
}

func (d *decoder) tupledata() []Tuple {
	size := int(d.uint16())
	data := make([]Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
		case 'u':
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = Tuple{Flag: 't', Value: d.buf.Next(vsize)}
		}
	}
	return data
}

func (d *decoder) columns() []Column {
	size := int(d.uint16())
	data := make([]Column, size)
	for i := 0; i < size; i++ {
		data[i] = Column{
			Key:  d.bool(),
			Name: d.string(),
			Type: d.uint32(),
			Mode: d.uint32(),
		}
	}
	return data
}

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp time.Time
	// 	Xid of the transaction.
	XID int32
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Commit struct {
	Flags uint8
	// The final LSN of the transaction.
	LSN uint64
	// The final LSN of the transaction.
	TransactionLSN uint64
	Timestamp      time.Time
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Relation struct {
	// ID of the relation.
	ID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	Name      string
	Replica   uint8
	Columns   []Column
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

func (r Relation) IsEmpty() bool {
	return r.ID == 0 && r.Name == "" && r.Replica == 0 && len(r.Columns) == 0
}

type Type struct {
	// ID of the data type
	ID        uint32
	Namespace string
	Name      string
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Insert struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	New bool
	Row []Tuple
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Update struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Old    bool
	Key    bool
	New    bool
	OldRow []Tuple
	Row    []Tuple
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Delete struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Key bool // TODO
	Old bool // TODO
	Row []Tuple
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Truncate struct {
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type Origin struct {
	LSN  uint64
	Name string
	// position where this message starts in WAL
	walStart uint64
	// position where this message ends in WAL
	walEnd uint64
}

type DecoderValue interface {
	pgtype.TextDecoder
	pgtype.Value
}

type Column struct {
	Key  bool
	Name string
	Type uint32
	Mode uint32
}
type Tuple struct {
	Flag  int8
	Value []byte
}

type Message interface {
	SetWalStart(offset uint64)
	WalStart() uint64
	SetWalEnd(offset uint64)
	WalEnd() uint64
}

func (m *Begin) SetWalStart(offset uint64)    { m.walStart = offset }
func (m *Relation) SetWalStart(offset uint64) { m.walStart = offset }
func (m *Update) SetWalStart(offset uint64)   { m.walStart = offset }
func (m *Insert) SetWalStart(offset uint64)   { m.walStart = offset }
func (m *Delete) SetWalStart(offset uint64)   { m.walStart = offset }
func (m *Commit) SetWalStart(offset uint64)   { m.walStart = offset }
func (m *Origin) SetWalStart(offset uint64)   { m.walStart = offset }
func (m *Type) SetWalStart(offset uint64)     { m.walStart = offset }
func (m *Truncate) SetWalStart(offset uint64) { m.walStart = offset }

func (m Begin) WalStart() uint64    { return m.walStart }
func (m Relation) WalStart() uint64 { return m.walStart }
func (m Update) WalStart() uint64   { return m.walStart }
func (m Insert) WalStart() uint64   { return m.walStart }
func (m Delete) WalStart() uint64   { return m.walStart }
func (m Commit) WalStart() uint64   { return m.walStart }
func (m Origin) WalStart() uint64   { return m.walStart }
func (m Type) WalStart() uint64     { return m.walStart }
func (m Truncate) WalStart() uint64 { return m.walStart }

func (m *Begin) SetWalEnd(offset uint64)    { m.walEnd = offset }
func (m *Relation) SetWalEnd(offset uint64) { m.walEnd = offset }
func (m *Update) SetWalEnd(offset uint64)   { m.walEnd = offset }
func (m *Insert) SetWalEnd(offset uint64)   { m.walEnd = offset }
func (m *Delete) SetWalEnd(offset uint64)   { m.walEnd = offset }
func (m *Commit) SetWalEnd(offset uint64)   { m.walEnd = offset }
func (m *Origin) SetWalEnd(offset uint64)   { m.walEnd = offset }
func (m *Type) SetWalEnd(offset uint64)     { m.walEnd = offset }
func (m *Truncate) SetWalEnd(offset uint64) { m.walEnd = offset }

func (m Begin) WalEnd() uint64    { return m.walEnd }
func (m Relation) WalEnd() uint64 { return m.walEnd }
func (m Update) WalEnd() uint64   { return m.walEnd }
func (m Insert) WalEnd() uint64   { return m.walEnd }
func (m Delete) WalEnd() uint64   { return m.walEnd }
func (m Commit) WalEnd() uint64   { return m.walEnd }
func (m Origin) WalEnd() uint64   { return m.walEnd }
func (m Type) WalEnd() uint64     { return m.walEnd }
func (m Truncate) WalEnd() uint64 { return m.walEnd }

// Parse a logical replication message.
// See https://www.postgresql.org/docs/current/static/protocol-logicalrep-message-formats.html
func Parse(src []byte) (Message, error) {
	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	switch msgType {
	case 'B':
		b := &Begin{}
		b.LSN = d.uint64()
		b.Timestamp = d.timestamp()
		b.XID = d.int32()
		return b, nil
	case 'C':
		c := &Commit{}
		c.Flags = d.uint8()
		c.LSN = d.uint64()
		c.TransactionLSN = d.uint64()
		c.Timestamp = d.timestamp()
		return c, nil
	case 'O':
		o := &Origin{}
		o.LSN = d.uint64()
		o.Name = d.string()
		return o, nil
	case 'R':
		r := &Relation{}
		r.ID = d.uint32()
		r.Namespace = d.string()
		r.Name = d.string()
		r.Replica = d.uint8()
		r.Columns = d.columns()
		return r, nil
	case 'Y':
		t := &Type{}
		t.ID = d.uint32()
		t.Namespace = d.string()
		t.Name = d.string()
		return t, nil
	case 'I':
		i := &Insert{}
		i.RelationID = d.uint32()
		i.New = d.uint8() > 0
		i.Row = d.tupledata()
		return i, nil
	case 'U':
		u := &Update{}
		u.RelationID = d.uint32()
		u.Key = d.rowinfo('K')
		u.Old = d.rowinfo('O')
		if u.Key || u.Old {
			u.OldRow = d.tupledata()
		}
		u.New = d.uint8() > 0
		u.Row = d.tupledata()
		return u, nil
	case 'D':
		dl := &Delete{}
		dl.RelationID = d.uint32()
		dl.Key = d.rowinfo('K')
		dl.Old = d.rowinfo('O')
		dl.Row = d.tupledata()
		return dl, nil
	case 'T':
		tr := &Truncate{}
		return tr, nil
	default:
		return nil, fmt.Errorf("Unknown message type for %s (%d)", []byte{msgType}, msgType)
	}
}
