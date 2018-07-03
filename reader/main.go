package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"git.campmon.com/golang/corekit/proc"
	"git.campmon.com/golang/corekit/str"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/spf13/pflag"
	"github.com/xitonix/badger-benchmark/common"
)

func main() {
	reportEvery := pflag.Int("report-every", 1000000, "Defines how often to print report")
	prefetchSize := pflag.Int("prefetch-size", 10, "Specifies how many KV pairs to prefetch while iterating. Set to zero to disable pre-fetching")
	memMap := pflag.Bool("mem-map", false, "Indicates that the database tables and logs must be loaded using memory map instead of standard file I/O")
	reverse := pflag.Bool("reverse", false, "Direction of iteration. Set to true for backward iteration")
	cpu := pflag.Int("cpu", 8, "The maximum number of CPUs that can be executing the code. Set to 0 to go with defaults")
	base := pflag.String("db", "", "Database base directory")

	pflag.Parse()

	if str.IsEmpty(*base) {
		log.Fatal("Database directory cannot be empty")
	}

	if *prefetchSize < 0 {
		*prefetchSize = 0
	}

	report, err := common.NewCSVWriter("read_report.csv")
	if err != nil {
		log.Fatal("CSV ERR:", err)
	}

	if *cpu > 0 {
		runtime.GOMAXPROCS(*cpu)
	}

	mp := runtime.NumCPU()

	report.AddHeaders("MAX CPUs", "OPENING", "PREFETCH", "PREFETCH SIZE", "MEMORY MAP", "REVERSE", "ITEMS READ", "READ/sec")

	opts := badger.DefaultOptions
	if *memMap {
		opts.ValueLogLoadingMode = options.MemoryMap
		opts.TableLoadingMode = options.MemoryMap
	} else {
		opts.ValueLogLoadingMode = options.FileIO
		opts.TableLoadingMode = options.FileIO
	}

	opts.Dir, opts.ValueDir = common.GetDirs(*base)

	fmt.Printf("Openning the database utilising %d CPUs\n", mp)
	start := time.Now()
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	openDBTime := time.Since(start)
	fmt.Printf("The database has been openned. It tool %s. \n", openDBTime)

	defer func() {
		fmt.Println("Closing the database...")
		err := db.Close()
		if err != nil {
			fmt.Printf("CLOSE ERR: %s\n", err)
		}
	}()

	var cancelled bool

	go func() {
		proc.WaitForTermination()
		cancelled = true
	}()

	var counter int
	rater := common.NewRater(time.Second)
	start = time.Now()
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = *prefetchSize
		opts.PrefetchValues = *prefetchSize > 0
		opts.Reverse = *reverse
		it := txn.NewIterator(opts)
		defer it.Close()
		var reportCounter int
		reportTime := time.Now()
		for it.Rewind(); it.Valid(); it.Next() {
			if cancelled {
				break
			}
			counter++
			rater.Inc()
			reportCounter++

			if reportCounter >= *reportEvery {
				fmt.Printf("It took %s to read %d records (avg. %d /sec)\n", time.Since(reportTime), reportCounter, rater.Rate())
				reportCounter = 0
				reportTime = time.Now()
			}

			item := it.Item()
			_ = item.Key()
			_, _ = item.Value()
		}
		return nil
	})

	if err != nil {
		fmt.Printf("VIEW ERR:%s\n", err)
		return
	}

	duration := time.Now().Sub(start)
	rate := rater.Rate()
	fmt.Println("\nSUMMARY")
	fmt.Println("-------")

	fmt.Printf("Prefetch Enabled: %v\n", common.B2S(*prefetchSize > 0))
	fmt.Printf("   Prefetch Size: %v\n", *prefetchSize)
	fmt.Printf("      Memory Map: %v\n", common.B2S(*memMap))
	fmt.Printf("        MAX CPUs: %d\n", mp)
	fmt.Printf("         Reverse: %v\n\n", common.B2S(*reverse))
	fmt.Printf("  Opening The DB: %v\n", openDBTime)
	fmt.Printf("      Items Read: %d\n", counter)
	fmt.Printf("        Duration: %v\n", duration)
	fmt.Printf("         Reads/s: %v\n\n", rate)

	if !cancelled {
		report.Write(duration, mp, openDBTime, common.B2S(*prefetchSize > 0), *prefetchSize, common.B2S(*memMap), common.B2S(*reverse), counter, rate)
	}
}
