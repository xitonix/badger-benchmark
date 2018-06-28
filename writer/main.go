package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"context"
	"git.campmon.com/golang/corekit/proc"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/spf13/pflag"
	"github.com/xitonix/badger-benchmark/common"
)

type entry struct {
	key, value []byte
}

func main() {
	parallel := pflag.Int("parallel", 1, "Number of go routines for writing data concurrently")
	keySize := pflag.Int("key-size", 16, "Key size in bytes")
	valueSize := pflag.Int("value-size", 1024, "Value size in bytes")
	count := pflag.Int("count", 1000000, "Number of documents to write")
	reportEvery := pflag.Int("report-every", 100000, "Defines how often to print report")
	batchSize := pflag.Int("batch-size", 1000, "The size of the batch to write data into the disk")
	syncWrite := pflag.Bool("sync-write", false, "Sync all writes to disk")
	fileIO := pflag.Bool("file-io", false, "Indicates that the database tables and logs must be loaded using standard I/O instead of memory map")
	clean := pflag.Bool("clean", false, "Removes data files and the logs before write")

	pflag.Parse()

	err := initialise(*clean)
	if err != nil {
		log.Fatalf("Failed to initialise the directories. %s", err)
	}

	every := int(math.Min(float64(*reportEvery), float64(*count)))

	opts := badger.DefaultOptions
	opts.SyncWrites = *syncWrite
	if *fileIO {
		opts.ValueLogLoadingMode = options.FileIO
		opts.TableLoadingMode = options.FileIO
	} else {
		opts.ValueLogLoadingMode = options.MemoryMap
		opts.TableLoadingMode = options.MemoryMap
	}

	opts.Dir = common.DataDir
	opts.ValueDir = common.LogDir
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}

	cnx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		proc.WaitForTermination()
		fmt.Println("Stopping the writer...")
		cancel()
	}()

	defer func() {
		fmt.Println("Closing the database...")
		err := db.Close()
		if err != nil {
			fmt.Printf("Err06: %s\n", err)
		}
	}()

	producer := newProducer(*count, *keySize, *valueSize, *parallel)
	fmt.Printf("Preparing %v documents...\n", *count)
	producer.generateAll()
	fmt.Printf("Writing %d unique keys into the database\n", producer.len())
	wg := sync.WaitGroup{}

	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go consume(cnx, &wg, producer, *batchSize, db, *syncWrite, every)
	}
	start := time.Now()
	producer.start(cnx)
	wg.Wait()
	duration := time.Now().Sub(start)
	fmt.Println("\nSUMMARY")
	fmt.Println("-------")
	fmt.Printf("  Key Size: %d Bytes\n", *keySize)
	fmt.Printf("Value Size: %d Bytes\n", *valueSize)
	fmt.Printf("  Parallel: %d\n", *parallel)
	fmt.Printf("     Count: %d\n", *count)
	fmt.Printf("Batch Size: %d\n", *batchSize)
	fmt.Printf("  Duration: %v\n", duration)
	fmt.Printf(" Writes/s: %v\n\n", int(float64(*count)/duration.Seconds()))
}

func consume(cnx context.Context, wg *sync.WaitGroup, p *producer, batchSize int, db *badger.DB, syncWrite bool, reportEvery int) {
	defer wg.Done()
	counter := 0
	txn := db.NewTransaction(true)

	type callback func(error)
	var cb callback
	if !syncWrite {
		cb = func(e error) {
			if e != nil {
				fmt.Printf("Callback Err: %s\n", e)
			}
		}
	}

	reportCounter := 0
	reportTime := time.Now()
	for {
		select {
		case <-cnx.Done():
			txn.Discard()
			return
		case doc, more := <-p.output():
			if !more {
				err := txn.Commit(cb)
				if err != nil {
					fmt.Printf("Err05: %s\n", err)
				}
				return
			}

			err := txn.Set(doc.key, doc.value)
			if err != nil {
				if err == badger.ErrTxnTooBig {
					err := txn.Commit(cb)
					if err != nil {
						fmt.Printf("Err01: %s\n", err)
					}
					txn = db.NewTransaction(true)
					err = txn.Set(doc.key, doc.value)
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
			reportCounter++
			if counter >= batchSize {
				if err = txn.Commit(cb); err != nil {
					fmt.Printf("Err04: %s\n", err)
					return
				}
				counter = 0
				txn = db.NewTransaction(true)
			}

			if reportCounter >= reportEvery {
				fmt.Printf("It took %s to insert %d records\n", time.Since(reportTime), reportCounter)
				reportCounter = 0
				reportTime = time.Now()
			}
		}
	}
}

func initialise(clean bool) error {
	if err := createIfNotExist(common.DataDir, clean); err != nil {
		return err
	}

	return createIfNotExist(common.LogDir, clean)
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
