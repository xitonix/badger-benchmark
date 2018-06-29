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
	compactors := pflag.Int("compactors", 3, "Number of concurrent compactors")
	count := pflag.Int("count", 1000000, "Number of documents to write")
	reportEvery := pflag.Int("report-every", 100000, "Defines how often to print report")
	batchSize := pflag.Int("batch-size", 1000, "The size of the batch to write data into the disk")
	syncWrite := pflag.Bool("sync-write", false, "Sync all writes to disk")
	fileIO := pflag.Bool("file-io", false, "Indicates that the database tables and logs must be loaded using standard I/O instead of memory map")
	clean := pflag.Bool("clean", false, "Removes data files and the logs before write")
	readBeforeWrite := pflag.Bool("read-before-write", false, "Checks if a key exists before writing")

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

	var cancelled bool
	opts.Dir = common.DataDir
	opts.ValueDir = common.LogDir
	opts.NumCompactors = *compactors
	fmt.Println("Openning the database")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("The database has been openned")

	cnx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		fmt.Println("Closing the database...")
		err := db.Close()
		if err != nil {
			fmt.Printf("Err06: %s\n", err)
		}
	}()

	producer := newProducer(*count, *keySize, *valueSize, *parallel)

	go func() {
		proc.WaitForTermination()
		producer.stop()
		cancelled = true
		cancel()
	}()

	wg := sync.WaitGroup{}

	report, err := common.NewCSVWriter("write_report.csv")
	if err != nil {
		log.Fatal("CSV ERR:", err)
	}

	report.AddHeaders("KEY SIZE", "VALUE SIZE", "BATCH SIZE", "PARALLEL", "FILE I/O", "SYNC", "READ BEFORE WRITE", "COMPACTORS", "WRITTEN", "WRITE/sec")

	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go consume(cnx, &wg, producer, *batchSize, db, *syncWrite, every, *readBeforeWrite)
	}
	start := time.Now()
	fmt.Printf("Writing %d unique keys into the database\n", *count)
	producer.start()
	wg.Wait()
	duration := time.Now().Sub(start)
	fmt.Println("\nSUMMARY")
	fmt.Println("-------")
	fmt.Printf("  Key Size: %d Bytes\n", *keySize)
	fmt.Printf("Value Size: %d Bytes\n", *valueSize)
	fmt.Printf("  Parallel: %d\n", *parallel)
	fmt.Printf("  File I/O: %v\n", common.B2S(*fileIO))
	fmt.Printf("Batch Size: %d\n", *batchSize)
	fmt.Printf("Compactors: %d\n", *compactors)
	fmt.Printf("Read First: %v\n", common.B2S(*readBeforeWrite))
	fmt.Printf("Sync Write: %v\n\n", common.B2S(*syncWrite))
	fmt.Printf("     Count: %d\n", *count)
	fmt.Printf("  Duration: %v\n", duration)
	rate := int(float64(*count) / duration.Seconds())
	fmt.Printf(" Writes/s: %v\n\n", rate)

	if !cancelled {
		report.Write(duration, *keySize, *valueSize, *batchSize, *parallel, common.B2S(*fileIO), common.B2S(*syncWrite), common.B2S(*readBeforeWrite), *compactors, *count, rate)
	}
}

func consume(cnx context.Context, wg *sync.WaitGroup, p *producer, batchSize int, db *badger.DB, syncWrite bool, reportEvery int, readBeforeWrite bool) {
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

			if readBeforeWrite {
				item, err := txn.Get(doc.key)
				if err != nil && err != badger.ErrKeyNotFound {
					fmt.Printf("READ ERR: %s\n", err)
					continue
				}
				if item != nil {
					fmt.Println("Duplicate key detected. Overwriting!")
				}
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

			if reportEvery > 0 && reportCounter >= reportEvery {
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
