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
	Name          string
	Publication   string
	StatusTimeout time.Duration

	conn       *pgconn.PgConn
	maxWal     uint64
	walRetain  uint64
	walFlushed uint64

	failOnHandler bool

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func([]Message, uint64) error

func NewSubscription(conn *pgconn.PgConn, name, publication string, walRetain uint64, failOnHandler bool) *Subscription {
	return &Subscription{
		Name:          name,
		Publication:   publication,
		StatusTimeout: 10 * time.Second,

		conn:          conn,
		walRetain:     walRetain,
		failOnHandler: failOnHandler,
	}
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, publication)
}

// CreateSlot creates a replication slot if it doesn't exist
func (s *Subscription) CreateSlot() (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	ctx := context.Background()
	//sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", s.Publication)
	//result := s.conn.Exec(context.Background(), sql)
	//_, err = result.ReadAll()
	//if err != nil {
	//	return fmt.Errorf("drop publication if exists error", err)
	//}
	//
	//var sql string
	//if len(s.tables) == 0 {
	//	sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", s.Publication)
	//} else {
	//	sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", s.Publication, strings.Join(s.tables, ","))
	//}
	//
	//result := s.conn.Exec(context.Background(), sql)
	//_, err = result.ReadAll()
	//if err != nil {
	//	if e, ok := err.(*pgconn.PgError); ok && e.Code == "42710" {
	//		// publication exists
	//	} else {
	//		return err
	//	}
	//}

	//sysident, err := pglogrepl.IdentifySystem(context.Background(), s.conn)
	//if err != nil {
	//	return fmt.Errorf("IdentifySystem failed:", err)
	//}
	//log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(ctx, s.conn, s.Name, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == "42710" {
			return ErrorSlotExist
		}
		return err
	}

	return nil
}

func (s *Subscription) DropSlot() (err error) {
	ctx := context.Background()
	err = pglogrepl.DropReplicationSlot(ctx, s.conn, s.Name, pglogrepl.DropReplicationSlotOptions{Wait: false})
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

// Flush sends the status message to server indicating that we've fully applied all of the events until maxWal.
// This allows PostgreSQL to purge it's WAL logs
func (s *Subscription) Flush() error {
	wp := atomic.LoadUint64(&s.maxWal)
	err := s.sendStatus(wp, wp)
	if err == nil {
		// atomic.StoreUint64(&s.walFlushed, wp)
	}

	//fmt.Printf("Flush => walWrite: %d, walFlush: %d\n", wp, wp)
	return err
}

// Start replication and block until error or ctx is canceled
func (s *Subscription) Start(ctx context.Context, startLSN uint64, batchSize int, recvWindow time.Duration, h Handler) error {
	var err error
	err = pglogrepl.StartReplication(ctx, s.conn, s.Name, pglogrepl.LSN(startLSN),
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{pluginArgs("1", s.Publication)},
		})

	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
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

		// fmt.Printf("Send status => walWrite: %d (%s), walFlush: %d (%s)\n", walPos, pglogrepl.LSN(walPos), walFlush, pglogrepl.LSN(walFlush))

		return s.sendStatus(walPos, walFlush)
	}

	heartbeat := make(chan bool)
	defer close(heartbeat)
	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				sendStatus()
			case _, ok := <-heartbeat:
				if !ok {
					return
				}
				sendStatus()
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
				return fmt.Errorf("Unable to send final status: %s", err)
			}

			return err

		default:
			n := 0
			messages := make([][]byte, 0)
			// batch recv messages up to batchSize
			//start :=  time.Now()
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
					return fmt.Errorf("ReceiveMessage failed: %s", err)
				}

				if msg == nil {
					return fmt.Errorf("replication failed: nil message received, should not happen")
				}

				//log.Printf("%v, %#v", &msg, msg)
				if msg, ok := msg.(*pgproto3.CopyData); ok {
					messages = append(messages, msg.Data)
				}

				n++
			}

			//fmt.Printf("recv %d messages in %d ms.\n", len(messages), time.Since(start).Milliseconds())

			//log.Printf("%#v", messages)
			// no message received, continue receiving
			if len(messages) == 0 {
				continue
			}

			logmsgs := make([]Message, 0)
			var walStart, serverWalEnd uint64
			for _, msg := range messages {
				switch msg[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg[1:])
					if err != nil {
						return fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %s", err)
					}
					//fmt.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

					serverWalEnd = uint64(pkm.ServerWALEnd)

					if pkm.ReplyRequested {
						//if err = sendStatus(); err != nil {
						//	return err
						//}
						heartbeat <- true
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg[1:])
					if err != nil {
						return fmt.Errorf("ParseXLogData failed: %s", err)
					}
					//fmt.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

					if walStart == 0 {
						walStart = uint64(xld.WALStart)
					}

					// Skip stuff that's in the past
					if walStart > 0 && walStart <= startLSN {
						continue
					}

					serverWalEnd = uint64(xld.ServerWALEnd)
					//if walStart > atomic.LoadUint64(&s.maxWal) {
					//	atomic.StoreUint64(&s.maxWal, walStart)
					//}

					logmsg, err := Parse(xld.WALData)
					if err != nil {
						return fmt.Errorf("invalid pgoutput message: %s", err)
					}
					logmsgs = append(logmsgs, logmsg)
				}
			}

			// Ignore the error from handler for now
			if len(logmsgs) > 0 {
				if err = h(logmsgs, walStart); err != nil && s.failOnHandler {
					return err
				}

				//if walStart > atomic.LoadUint64(&s.maxWal) {
				//	atomic.StoreUint64(&s.maxWal, walStart)
				//}
			}

			if serverWalEnd > atomic.LoadUint64(&s.maxWal) {
				atomic.StoreUint64(&s.maxWal, serverWalEnd)
				// heartbeat <- true
				// err = s.Flush() // notify server than all WAL messages are processed
				// if err != nil {
				// 	return err
				// }
			}
		}
	}
}
