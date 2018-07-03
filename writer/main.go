package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"git.campmon.com/golang/corekit/proc"
	"git.campmon.com/golang/corekit/str"
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
	keySize := pflag.Int("key-size", 20, "Key size in bytes")
	valueSize := pflag.Int("value-size", 100, "Value size in bytes")
	compactors := pflag.Int("compactors", 3, "Number of concurrent compactors")
	count := pflag.Int("count", 100000000, "Number of documents to write")
	reportEvery := pflag.Int("report-every", 1000000, "Defines how often to print report")
	batchSize := pflag.Int("batch-size", 1000, "The size of the batch to write data into the disk")
	syncWrite := pflag.Bool("sync-write", false, "Sync all writes to disk")
	fileIO := pflag.Bool("file-io", false, "Indicates that the database tables and logs must be loaded using standard I/O instead of memory map")
	clean := pflag.Bool("clean", false, "Removes data files and the logs before write")
	readBeforeWrite := pflag.Bool("read-before-write", false, "Checks if a key exists before writing")
	cpu := pflag.Int("cpu", 8, "The maximum number of CPUs that can be executing the code. Set to 0 to go with defaults")
	base := pflag.String("db", "", "Database base directory")

	pflag.Parse()

	if str.IsEmpty(*base) {
		log.Fatal("Database directory cannot be empty")
	}

	dataPath, logPath := common.GetDirs(*base)

	err := initialise(dataPath, logPath, *clean)
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
	opts.Dir = dataPath
	opts.ValueDir = logPath
	opts.NumCompactors = *compactors
	fmt.Println("Openning the database")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("The database has been openned")

	cnx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rater := common.NewRater(time.Second)

	if *cpu > 0 {
		runtime.GOMAXPROCS(*cpu)
	}

	mp := runtime.NumCPU()

	report, err := common.NewCSVWriter("write_report.csv")
	if err != nil {
		log.Fatal("CSV ERR:", err)
	}

	defer func() {
		fmt.Println("Closing the database...")
		err := db.Close()
		if err != nil {
			fmt.Printf("CLOSE DB ERR: %s\n", err)
		}

		duration, rate := rater.Stop()
		fmt.Println("\nSUMMARY")
		fmt.Println("-------")
		fmt.Printf("  Key Size: %d Bytes\n", *keySize)
		fmt.Printf("Value Size: %d Bytes\n", *valueSize)
		fmt.Printf("  MAX CPUs: %d\n", mp)
		fmt.Printf("  Parallel: %d\n", *parallel)
		fmt.Printf("  File I/O: %v\n", common.B2S(*fileIO))
		fmt.Printf("Batch Size: %d\n", *batchSize)
		fmt.Printf("Compactors: %d\n", *compactors)
		fmt.Printf("Read First: %v\n", common.B2S(*readBeforeWrite))
		fmt.Printf("Sync Write: %v\n\n", common.B2S(*syncWrite))
		fmt.Printf("     Count: %d\n", *count)
		fmt.Printf("  Duration: %v\n", duration)
		fmt.Printf(" Writes/s: %v\n\n", rate)

		if !cancelled {
			report.Write(duration, mp, *keySize, *valueSize, *batchSize, *parallel, common.B2S(*fileIO), common.B2S(*syncWrite), common.B2S(*readBeforeWrite), *compactors, *count, rate)
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

	report.AddHeaders("MAX CPUs", "KEY SIZE", "VALUE SIZE", "BATCH SIZE", "PARALLEL", "FILE I/O", "SYNC", "READ BEFORE WRITE", "COMPACTORS", "WRITTEN", "WRITE/sec")
	rater.Start()
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go consume(cnx, rater, &wg, producer, *batchSize, db, *syncWrite, every, *readBeforeWrite)
	}
	fmt.Printf("Writing %d unique keys into the database utilising %d CPUs\n", *count, mp)
	producer.start()
	wg.Wait()
}

func consume(cnx context.Context, rater *common.Rater, wg *sync.WaitGroup, p *producer, batchSize int, db *badger.DB, syncWrite bool, reportEvery int, readBeforeWrite bool) {
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
					fmt.Printf("ERR-05: %s\n", err)
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
						fmt.Printf("ERR-01: %s\n", err)
					}
					txn = db.NewTransaction(true)
					err = txn.Set(doc.key, doc.value)
					if err != nil {
						fmt.Printf("ERR-02: %s\n", err)
						return
					}
					counter = 1
					continue
				}
				//err != badger.ErrTxnTooBig
				fmt.Printf("ERR-03: %s\n", err)
				return
			}
			counter++
			rater.Inc()
			reportCounter++
			if counter >= batchSize {
				if err = txn.Commit(cb); err != nil {
					fmt.Printf("ERR-04: %s\n", err)
					return
				}
				counter = 0
				txn = db.NewTransaction(true)
			}

			if reportEvery > 0 && reportCounter >= reportEvery {
				fmt.Printf("It took %s to insert %d records (avg. %d /sec)\n", time.Since(reportTime), reportCounter, rater.Rate())
				reportCounter = 0
				reportTime = time.Now()
			}
		}
	}
}

func initialise(data, log string, clean bool) error {
	if err := createIfNotExist(data, clean); err != nil {
		return err
	}

	return createIfNotExist(log, clean)
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
