package main

import (
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"os"
)

type entry struct {
	key, value []byte
}

const (
	dataDir = "data"
	logDir  = "logs"
)

func main() {
	parallel := flag.Int("parallel", 1, "Number of go routines for writing data concurrently")
	keySize := flag.Int("key-size", 64, "Key size in bytes")
	valueSize := flag.Int("value-size", 1024, "Value size in bytes")
	count := flag.Int("count", 10000, "Number of documents to write")
	batchSize := flag.Int("batch-size", 1000, "The size of the batch to write data into the disk")
	sameValue := flag.Bool("same-value", false, "If true, the value of all the keys will be the same")
	syncWrite := flag.Bool("sync-write", false, "Sync all writes to disk")
	clean := flag.Bool("clean", false, "Removes data files and the logs before write")

	flag.Parse()

	err := initialise(*clean)
	if err != nil {
		log.Fatalf("Failed to initialise the directories. %s", err)
	}

	opts := badger.DefaultOptions
	opts.SyncWrites = *syncWrite
	opts.ValueLogLoadingMode = options.MemoryMap
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = dataDir
	opts.ValueDir = logDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		fmt.Println("Closing the database...")
		err := db.Close()
		if err != nil {
			fmt.Printf("Err06: %s\n", err)
		}
	}()

	producer := newProducer(*count, *keySize, *valueSize, *parallel, *sameValue)
	fmt.Printf("Preparing %v documents...\n", *count)
	producer.generateAll()
	fmt.Printf("Writing %d unique keys into the database\n", producer.len())
	wg := sync.WaitGroup{}

	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go consume(&wg, producer, *batchSize, db)
	}
	start := time.Now()
	producer.start()
	wg.Wait()
	duration := time.Now().Sub(start)
	fmt.Printf("  Key Size: %d Bytes\n", *keySize)
	fmt.Printf("Value Size: %d Bytes\n", *valueSize)
	fmt.Printf("  Parallel: %d\n", *parallel)
	fmt.Printf("     Count: %d\n", *count)
	fmt.Printf("Batch Size: %d\n", *batchSize)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf(" Writes /s: %v\n", int(float64(*count)/duration.Seconds()))
}

func consume(wg *sync.WaitGroup, p *producer, batchSize int, db *badger.DB) {
	defer wg.Done()
	counter := 0
	txn := db.NewTransaction(true)

	for e := range p.output() {
		err := txn.Set(e.key, e.value)
		if err != nil {
			if err == badger.ErrTxnTooBig {
				err := txn.Commit(nil)
				if err != nil {
					fmt.Printf("Err01: %s\n", err)
				}
				txn = db.NewTransaction(true)
				err = txn.Set(e.key, e.value)
				if err != nil {
					fmt.Printf("Err02: %s\n", err)
					return
				}
				counter = 1
				continue
			}
			//err != badger.ErrTxnTooBig
			fmt.Printf("Err03: %s\n", err)
			return
		}
		counter++
		if counter >= batchSize {
			if err = txn.Commit(nil); err != nil {
				fmt.Printf("Err04: %s\n", err)
				return
			}
			counter = 0
			txn = db.NewTransaction(true)
		}
	}

	err := txn.Commit(nil)
	if err != nil {
		fmt.Printf("Err05: %s\n", err)
	}
}

func initialise(clean bool) error {
	if err := createIfNotExist(dataDir, clean); err != nil {
		return err
	}

	return createIfNotExist(logDir, clean)
}

func createIfNotExist(dir string, clean bool) error {
	_, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return os.MkdirAll(dir, 0777)
	}
	if err != nil {
		return err
	}

	if !clean {
		return nil
	}
	fmt.Printf("Cleaning up %s directory...\n", dir)
	err = os.RemoveAll(dir)
	if err != nil {
		return err
	}
	return os.MkdirAll(dir, 0777)
}
