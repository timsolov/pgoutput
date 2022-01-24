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

type Handler func([]Message, uint64) error

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

// Whether to exit from Start() func on failure in users handler
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

func (s *Subscription) sendStatus(walWrite, walFlush uint64) error {
	if walFlush > walWrite {
		return fmt.Errorf("walWrite should be >= walFlush")
	}

	s.Lock()
	defer s.Unlock()

	err := pglogrepl.SendStandbyStatusUpdate(context.Background(), s.conn,
		pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(walWrite),
			WALFlushPosition: pglogrepl.LSN(walFlush),
			WALApplyPosition: pglogrepl.LSN(walFlush),
		})

	if err != nil {
		return err
	}

	atomic.StoreUint64(&s.walFlushed, walFlush)

	return nil
}

// Flush sends the status message to server indicating that we've fully applied all of the events until s.maxWal.
// This allows PostgreSQL to purge it's WAL logs
func (s *Subscription) Flush() error {
	wp := atomic.LoadUint64(&s.maxWal)
	err := s.sendStatus(wp, wp)
	if err == nil {
		atomic.StoreUint64(&s.walFlushed, wp)
	}

	return err
}

// Start replication and block until error or ctx is canceled
func (s *Subscription) Start(ctx context.Context, startLSN uint64, batchSize int, recvWindow time.Duration, h Handler) error {
	var err error

	err = pglogrepl.StartReplication(
		ctx,
		s.conn,
		s.SlotName,
		pglogrepl.LSN(startLSN),
		pglogrepl.StartReplicationOptions{
			PluginArgs: pluginArgs("1", s.PublicationName),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	s.maxWal = startLSN

	sendStatus := func() error {
		walPos := atomic.LoadUint64(&s.maxWal)
		walLastFlushed := atomic.LoadUint64(&s.walFlushed)

		// Confirm only walRetain bytes in past
		// If walRetain is zero - will confirm current walPos as flushed
		walFlush := walPos - s.walRetain

		if walLastFlushed > walFlush {
			// If there was a manual flush - report it's position until we're past it
			walFlush = walLastFlushed
		} else if walFlush < 0 {
			// If we have less than walRetain bytes - just report zero
			walFlush = 0
		}

		if s.logger != nil {
			s.logger.Debugf("Send status => walWrite: %d (%s), walFlush: %d (%s)\n", walPos, pglogrepl.LSN(walPos), walFlush, pglogrepl.LSN(walFlush))
		}

		return s.sendStatus(walPos, walFlush)
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
			n := 0
			messages := make([][]byte, 0)

			start := time.Now()

			// batch recv messages up to batchSize
			for n < batchSize {
				wctx, cancel := context.WithTimeout(ctx, recvWindow)
				s.Lock()
				msg, err := s.conn.ReceiveMessage(wctx)
				s.Unlock()
				cancel()

				if err != nil {
					if pgconn.Timeout(err) { // timeout, break loop
						break
					}
					return fmt.Errorf("ReceiveMessage failed: %w", err)
				}

				if msg == nil {
					return fmt.Errorf("replication failed: nil message received, should not happen")
				}

				if msg, ok := msg.(*pgproto3.CopyData); ok {
					if s.logger != nil {
						s.logger.Debugf("received CopyData message: %s", &msg, string(msg.Data))
					}
					messages = append(messages, msg.Data)
				}

				n++
			}

			if len(messages) == 0 { // no message received, continue receiving
				continue
			}

			if s.logger != nil {
				s.logger.Debugf("recv %d messages in %d ms.\n", len(messages), time.Since(start).Milliseconds())
			}

			logmsgs := make([]Message, 0)

			var walStart, serverWalEnd uint64

			for _, msg := range messages {
				switch msg[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg[1:])
					if err != nil {
						return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
					}
					if s.logger != nil {
						s.logger.Debugf("Primary Keepalive Message => ServerWALEnd: %d (%s) ReplyRequested: %t", pkm.ServerWALEnd, pkm.ServerWALEnd, pkm.ReplyRequested)
					}

					serverWalEnd = uint64(pkm.ServerWALEnd)

					if pkm.ReplyRequested {
						heartbeat <- true
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg[1:])
					if err != nil {
						return fmt.Errorf("ParseXLogData failed: %w", err)
					}
					if s.logger != nil {
						s.logger.Debugf("XLogData => WALStart: %d (%s) ServerWALEnd: %d (%s) WALDataLen: %d", xld.WALStart, xld.WALStart, xld.ServerWALEnd, xld.ServerWALEnd, len(xld.WALData))
					}

					if walStart == 0 {
						// walStart set only here
						walStart = uint64(xld.WALStart)
					}

					// Skip stuff that's in the past
					if walStart > 0 && walStart <= startLSN {
						continue
					}

					if walStart > atomic.LoadUint64(&s.maxWal) {
						atomic.StoreUint64(&s.maxWal, walStart)
					}

					serverWalEnd = uint64(xld.ServerWALEnd)

					logmsg, err := Parse(xld.WALData)
					if err != nil {
						return fmt.Errorf("invalid pgoutput message: %w", err)
					}

					logmsgs = append(logmsgs, logmsg)
				}
			}

			if len(logmsgs) > 0 {
				if err = h(logmsgs, walStart); err != nil && s.failOnHandler {
					return err
				}

				if walStart > atomic.LoadUint64(&s.maxWal) {
					atomic.StoreUint64(&s.maxWal, walStart)
					heartbeat <- true
				}
			}

			if serverWalEnd > atomic.LoadUint64(&s.maxWal) {
				atomic.StoreUint64(&s.maxWal, serverWalEnd)
				heartbeat <- true
			}
		}
	}
}
