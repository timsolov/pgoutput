package pgoutput

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
)

var (
	ErrorSlotExist   = errors.New("slot already exists")
	ErrorReplication = errors.New("replication failed: nil message received, should not happen")
)

type Subscription struct {
	SlotName        string // name of replication slot
	PublicationName string
	StatusTimeout   time.Duration

	conn       *pgconn.PgConn
	maxWal     uint64 // WAL pos readed from DB
	walRetain  uint64
	walFlushed uint64 // WAL pos acknowledged as processed
	logger     Logger

	failOnHandler bool // whether to exit from Start() func on failure in user's handler

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func(messages []Message) error

type SubOpt func(*Subscription)

// set some logger
func SetLogger(l Logger) SubOpt {
	return func(s *Subscription) {
		s.logger = l
	}
}

// number bytes in past do not apply as processed in WAL
func SetWALRetain(retainBytes uint64) SubOpt {
	return func(s *Subscription) {
		s.walRetain = retainBytes
	}
}

// whether to exit from Start() func on failure in users handler
func SetFailOnHandler(b bool) SubOpt {
	return func(s *Subscription) {
		s.failOnHandler = b
	}
}

// NewSubscription creates and fill out Subscription struct.
// To start listening logical replication you've to run Start method.
//   name - replication slot name
//   publication - publication name
func NewSubscription(conn *pgconn.PgConn, slotName, publicationName string, opts ...SubOpt) *Subscription {
	sub := &Subscription{
		SlotName:        slotName,
		PublicationName: publicationName,
		StatusTimeout:   10 * time.Second,

		conn: conn,
	}

	for _, opt := range opts {
		opt(sub)
	}

	return sub
}

func pluginArgs(version, publicationName string) []string {
	return []string{
		fmt.Sprintf("proto_version '%s'", version),
		fmt.Sprintf("publication_names '%s'", publicationName),
	}
}

// CreateSlot creates a replication slot if it doesn't exist.
// The slot should be created before Start func.
//   returns:
//     nil - slot created;
//     ErrorSlotExist - not an error but means slot already exists;
//     other error - means an error.
func (s *Subscription) CreateSlot() (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	ctx := context.Background()

	_, err = pglogrepl.CreateReplicationSlot(
		ctx,
		s.conn,
		s.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == "42710" { // replication slot already exists
			return ErrorSlotExist
		}
		return err
	}

	return nil
}

// DropSlot removes a replication slot if it exists.
func (s *Subscription) DropSlot() (err error) {
	ctx := context.Background()
	err = pglogrepl.DropReplicationSlot(ctx, s.conn, s.SlotName, pglogrepl.DropReplicationSlotOptions{Wait: false})
	if err != nil {
		return err
	}

	return nil
}

func (s *Subscription) sendStatus(offsetProcessed uint64) error {
	if offsetProcessed > atomic.LoadUint64(&s.maxWal) {
		return fmt.Errorf("walWrite should be >= walFlush")
	}

	// Confirm only walRetain bytes in past
	// If s.walRetain is zero - will confirm current walPos as flushed
	walFlushed := offsetProcessed - s.walRetain

	if walFlushed < 0 {
		// If we have less than walRetain bytes - just report zero
		walFlushed = 0
	}

	if s.logger != nil {
		s.logger.Debugf("Send status (real) => walFlushed: %d (%s)\n", walFlushed, pglogrepl.LSN(walFlushed))
	}

	s.Lock()
	defer s.Unlock()

	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(walFlushed),
			WALFlushPosition: pglogrepl.LSN(walFlushed),
			WALApplyPosition: pglogrepl.LSN(walFlushed),
		})

	if err != nil {
		return err
	}

	atomic.StoreUint64(&s.walFlushed, offsetProcessed)

	return nil
}

// FlushWalPosition sends to postgresql info about offset position already processed.
func (s *Subscription) FlushWalPosition(offset uint64) error {
	if offset > atomic.LoadUint64(&s.maxWal) {
		return fmt.Errorf("offset more then read position")
	}
	if offset < atomic.LoadUint64(&s.walFlushed) {
		return fmt.Errorf("offset before flushed position")
	}

	atomic.StoreUint64(&s.walFlushed, offset)

	return s.sendStatus(offset)
}

// SlotRestartLSN returns the value of the last possible to restart offset for a specific slot.
func (s *Subscription) SlotRestartLSN(ctx context.Context, slotName string) (pglogrepl.LSN, error) {
	sql := fmt.Sprintf("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='%s'", slotName)
	mrr := s.conn.Exec(ctx, sql)

	results, err := mrr.ReadAll()
	if err != nil {
		return 0, err
	}
	if len(results) != 1 {
		return 0, fmt.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return 0, fmt.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 1 {
		return 0, fmt.Errorf("expected 1 result columns, got %d", len(row))
	}

	colData := string(row[0])

	return pglogrepl.ParseLSN(colData)
}

// Start replication and block until error or ctx is canceled
//   startOffset - LSN position where logical replication should start from (to take the offset from LSN possible using pglogrepl.ParseLSN);
//   recvWindow  - timeout to waiting messages from PostgreSQL;
//   h           - callback handler to process RELATION, INSERT, UPDATE, DELETE messages.
func (s *Subscription) Start(ctx context.Context, startOffset uint64, recvWindow time.Duration, h Handler) error {
	var err error

	err = pglogrepl.StartReplication(
		ctx,
		s.conn,
		s.SlotName,
		pglogrepl.LSN(startOffset),
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs("1", s.PublicationName),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	s.maxWal = startOffset

	sendStatus := func() error {
		walFlushed := atomic.LoadUint64(&s.walFlushed)

		if s.logger != nil {
			s.logger.Debugf("Send status (virtual) => walFlushed: %d (%s)\n", walFlushed, pglogrepl.LSN(walFlushed))
		}

		return s.sendStatus(walFlushed)
	}

	heartbeat := make(chan bool, 1)
	defer close(heartbeat)
	sendStatusErr := make(chan error, 1)

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err := sendStatus(); err != nil {
					sendStatusErr <- err
				}
			case _, ok := <-heartbeat:
				if !ok {
					return
				}
				if err := sendStatus(); err != nil {
					sendStatusErr <- err
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	logmsgs := make([]Message, 0)

	for {
		select {
		case <-ctx.Done():
			// Send final status and exit
			if err = sendStatus(); err != nil {
				return fmt.Errorf("Unable to send final status: %w", err)
			}

			return err
		case err = <-sendStatusErr:
			return fmt.Errorf("Unable to send status from heartbeat: %w", err)
		default:
			{
				wctx, cancel := context.WithTimeout(ctx, recvWindow)
				s.Lock()
				message, err := s.conn.ReceiveMessage(wctx)
				s.Unlock()
				cancel()

				if err != nil {
					if pgconn.Timeout(err) { // timeout, break loop
						break
					}
					return fmt.Errorf("ReceiveMessage failed: %w", err)
				}

				if message == nil {
					return fmt.Errorf("replication failed: nil message received, should not happen")
				}

				copyData, isCopyData := message.(*pgproto3.CopyData)
				if !isCopyData {
					continue
				}

				msg := copyData.Data

				switch msg[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg[1:])
					if err != nil {
						return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
					}
					if s.logger != nil {
						s.logger.Debugf("Primary Keepalive Message => ServerWALEnd: %d (%s) ReplyRequested: %t", pkm.ServerWALEnd, pkm.ServerWALEnd, pkm.ReplyRequested)
					}

					serverWalEnd := uint64(pkm.ServerWALEnd)
					if serverWalEnd > atomic.LoadUint64(&s.maxWal) {
						atomic.StoreUint64(&s.maxWal, serverWalEnd)
					}

					if pkm.ReplyRequested {
						heartbeat <- true
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg[1:])
					if err != nil {
						return fmt.Errorf("ParseXLogData failed: %w", err)
					}

					walStart := uint64(xld.WALStart)
					serverWalEnd := uint64(xld.ServerWALEnd)
					if serverWalEnd > atomic.LoadUint64(&s.maxWal) {
						atomic.StoreUint64(&s.maxWal, serverWalEnd)
					}

					// Skip stuff that's in the past
					if walStart > 0 && walStart <= startOffset {
						continue
					}

					var logmsg Message

					logmsg, err = Parse(xld.WALData)
					if err != nil {
						return fmt.Errorf("invalid pgoutput message: %w", err)
					}
					logmsg.SetWalStart(walStart)

					if len(logmsgs) > 0 {
						logmsgs[len(logmsgs)-1].SetWalEnd(walStart)
					}

					switch logmsg.(type) {
					case *Relation, *Insert, *Update, *Delete:
						logmsgs = append(logmsgs, logmsg)
					case *Begin:
						logmsgs = nil
					case *Commit:
						if len(logmsgs) > 0 {
							if err = h(logmsgs); err != nil && s.failOnHandler {
								return err
							}
						}
					}
				}
			}
		}
	}
}

func msgType(logmsg Message) string {
	switch logmsg.(type) {
	case *Relation:
		return "RELATION"
	case *Insert:
		return "INSERT"
	case *Update:
		return "UPDATE"
	case *Delete:
		return "DELETE"
	case *Begin:
		return "BEGIN"
	case *Commit:
		return "COMMIT"
	}
	return "Unk"
}
