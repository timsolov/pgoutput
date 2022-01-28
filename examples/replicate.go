package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/timsolov/pgoutput"
)

type L struct {
	pgoutput.Logger
}

func (l *L) Debugf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func main() {
	// PGOUTPUT_DEMO_CONN_STRING=postgres://dbuser:@127.0.0.1/dbname?replication=database
	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGOUTPUT_DEMO_CONN_STRING"))
	if err != nil {
		log.Fatal(err)
	}

	set := pgoutput.NewRelationSet(nil)

	dump := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %s", err)
		}
		r, ok := set.Get(relation)
		if ok {
			log.Printf("Relation : %s", r.Name)
		}

		for name, value := range values {
			val := value.Get()
			log.Printf("%s %T: %v", name, val, val)
		}
		return nil
	}

	nMsg := 0

	sub := pgoutput.NewSubscription(conn, "db3", "outbox_insert",
		pgoutput.SetLogger(&L{}))
	err = sub.CreateSlot()
	if err != nil {
		if err == pgoutput.ErrorSlotExist {
			log.Println(err)
		} else {
			log.Fatal(err)
		}
	}

	handler := func(messages []pgoutput.Message) error {
		for _, m := range messages {
			log.Println(m.WalStart(), pglogrepl.LSN(m.WalStart()), m.WalEnd(), pglogrepl.LSN(m.WalEnd()))
			switch v := m.(type) {
			case *pgoutput.Relation:
				log.Printf("RELATION")
				set.Add(*v)
			case *pgoutput.Insert:
				log.Printf("INSERT")
				dump(v.RelationID, v.Row)
			case *pgoutput.Update:
				log.Printf("UPDATE")
				dump(v.RelationID, v.Row)
			case *pgoutput.Delete:
				log.Printf("DELETE")
				dump(v.RelationID, v.Row)
			case *pgoutput.Begin:
				log.Printf("BEGIN")
			case *pgoutput.Commit:
				log.Printf("COMMIT")
			}
			nMsg++
		}

		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Initialize signal handler
	osSignal := make(chan os.Signal, 1)
	go func() {
		for {
			switch <-osSignal {
			case syscall.SIGINT, syscall.SIGTERM:
				// cleanup and terminate
				log.Println("SIGINT/SIGTERM received")
				cancel()
			}
		}
	}()

	signal.Notify(osSignal, syscall.SIGINT, syscall.SIGTERM)
	if err = sub.Start(ctx, 0, time.Second*1, handler); err != nil {
		log.Fatal(err)
	}
}
