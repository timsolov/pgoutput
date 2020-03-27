package pgoutput

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
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
	WaitTimeout   time.Duration
	StatusTimeout time.Duration
	tables        []string

	conn       *pgconn.PgConn
	maxWal     uint64
	walRetain  uint64
	walFlushed uint64

	failOnHandler bool

	// Mutex is used to prevent reading and writing to a connection at the same time
	sync.Mutex
}

type Handler func(Message, uint64) error

func NewSubscription(conn *pgconn.PgConn, name, publication string, tables []string, walRetain uint64, failOnHandler bool) *Subscription {
	if tables == nil {
		tables = make([]string, 0)
	}

	return &Subscription{
		Name:          name,
		Publication:   publication,
		WaitTimeout:   1 * time.Second,
		StatusTimeout: 10 * time.Second,
		tables:        tables,

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
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", s.Publication)
	result := s.conn.Exec(context.Background(), sql)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("drop publication if exists error", err)
	}

	if len(s.tables) == 0 {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES;", s.Publication)
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s;", s.Publication, strings.Join(s.tables, ","))
	}

	result = s.conn.Exec(context.Background(), sql)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("create publication error", err)
	}

	sysident, err := pglogrepl.IdentifySystem(context.Background(), s.conn)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed:", err)
	}
	log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

	_, err = pglogrepl.CreateReplicationSlot(ctx, s.conn, s.Name, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if e, ok := err.(*pgconn.PgError); ok && e.Code == "42710" {
			return ErrorSlotExist
		}
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
	//log.Printf("Send status => walWrite: %d, walFlush: %d\n", walWrite, walFlush)

	return nil
}

// Flush sends the status message to server indicating that we've fully applied all of the events until maxWal.
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
func (s *Subscription) Start(ctx context.Context, startLSN uint64, h Handler) (err error) {
	err = pglogrepl.StartReplication(context.Background(), s.conn, s.Name, pglogrepl.LSN(startLSN),
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

		return s.sendStatus(walPos, walFlush)
	}

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = sendStatus(); err != nil {
					return
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
				return fmt.Errorf("Unable to send final status: %s", err)
			}

			return

		default:
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			s.Lock()
			msg, err := s.conn.ReceiveMessage(wctx)
			s.Unlock()
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				return fmt.Errorf("ReceiveMessage failed: %s", err)
			}

			//if err == context.DeadlineExceeded {
			//	continue
			//} else if err == context.Canceled {
			//	return err
			//} else if err != nil {
			//	if pgconn.Timeout(err) {
			//		continue
			//	}
			//	return fmt.Errorf("replication failed: %s", err)
			//}

			if msg == nil {
				return fmt.Errorf("replication failed: nil message received, should not happen")
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						return fmt.Errorf("ParsePrimaryKeepaliveMessage failed:", err)
					}
					//log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

					if pkm.ReplyRequested {
						if err = sendStatus(); err != nil {
							return err
						}
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						return fmt.Errorf("ParseXLogData failed:", err)
					}
					//log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

					walStart := uint64(xld.WALStart)
					// Skip stuff that's in the past
					if walStart > 0 && walStart <= startLSN {
						continue
					}

					if walStart > atomic.LoadUint64(&s.maxWal) {
						atomic.StoreUint64(&s.maxWal, walStart)
					}

					var logmsg Message
					logmsg, err = Parse(xld.WALData)
					if err != nil {
						return fmt.Errorf("invalid pgoutput message: %s", err)
					}

					// Ignore the error from handler for now
					if err = h(logmsg, walStart); err != nil && s.failOnHandler {
						return err
					}

				}
			default:
				//log.Printf("Received unexpected message: %#v\n", msg)
			}
		}
	}
}
