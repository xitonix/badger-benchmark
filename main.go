package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

type entry struct {
	key, value []byte
}

func main() {
	parallel := flag.Int("parallel", 10, "Number of go routines for writing data concurrently")
	keySize := flag.Int("key-size", 64, "Key size in bytes")
	valueSize := flag.Int("value-size", 1024, "Value size in bytes")
	count := flag.Int("count", 10000, "Number of documents to write")
	batchSize := flag.Int("batch-size", 1000, "The size of the batch to write data into the disk")

	flag.Parse()

	opts := badger.DefaultOptions
	opts.Dir = "data"
	opts.ValueDir = "data/logs"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := db.Close()
		fmt.Println("Closing the database...")
		if err != nil {
			fmt.Printf("Err05: %s\n", err)
		}
	}()

	producer := newProducer(*count, *keySize, *valueSize, *parallel)
	wg := sync.WaitGroup{}
	start := time.Now()
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go consume(&wg, producer, *batchSize, db)
	}

	producer.start()
	wg.Wait()
	now := time.Now()
	fmt.Printf("  Key Size: %d Bytes\n", *keySize)
	fmt.Printf("Value Size: %d Bytes\n", *valueSize)
	fmt.Printf("  Parallel: %d\n", *parallel)
	fmt.Printf("     Count: %d\n", *count)
	fmt.Printf("Batch Size: %d\n", *batchSize)
	duration := now.Sub(start)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf(" Writes /s: %v\n", int(float64(*count)/duration.Seconds()))
}

func consume(wg *sync.WaitGroup, p *producer, batchSize int, db *badger.DB) {
	defer wg.Done()
	counter := 0
	txn := db.NewTransaction(true)

	commit := func(txn *badger.Txn, counter *int) error {
		err := txn.Commit(nil)
		if err != nil {
			return err
		}
		*counter = 0
		return nil
	}

	for e := range p.output() {
		err := txn.Set(e.key, e.value)
		if err != nil {
			if err == badger.ErrTxnTooBig {
				txn.Commit(nil)
				txn = db.NewTransaction(true)
				err = txn.Set(e.key, e.value)
				if err != nil {
					fmt.Printf("Err01: %s\n", err)
					return
				}
				continue
			}
			//err != badger.ErrTxnTooBig
			fmt.Printf("Err02: %s\n", err)
			return
		}
		if counter >= batchSize {
			if err = commit(txn, &counter); err != nil {
				fmt.Printf("Err03: %s\n", err)
				return
			}
		}
	}

	if err := commit(txn, &counter); err != nil {
		fmt.Printf("Err04: %s\n", err)
		return
	}
}
