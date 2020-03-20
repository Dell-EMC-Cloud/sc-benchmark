package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
        "io"
	"os"
        "path/filepath"
	"runtime"
	"sync"
	"strconv"
	"time"
)

func main() {

	// define and set default command parameter flags
	var pFlag = flag.Int("p", 1, "Optional: set number of concurrent file writers to use; defaults to 1")
	var vFlag = flag.Bool("v", false, "Optional: Turn on verbose output mode; it will print the progres every second")

	flag.Parse()
        args := flag.Args()

        if len(args) != 5 {
		fmt.Fprintf(os.Stderr, "Error! 5 positional arguments required.\n")
                fmt.Fprintf(os.Stderr, "\nUsage: %s [-p <parallel threads> -v (verbose)] <amount in GB> <file-size-bytes> <random-file-size-multiplier> <rb|ro|wo> <writable-directory>\n", os.Args[0])
                fmt.Fprintf(os.Stderr, "\nExample: %s -v -p 8 1024 10485760 1 rb /tmp\n\n", os.Args[0])
		os.Exit(1)
	}

        nFlag, _ := strconv.Atoi(args[0])
        sFlag, _ := strconv.Atoi(args[1])
        mFlag, _ := strconv.Atoi(args[2])
        rFlag := args[3]
        dFlag := args[4]

	// runtime.GOMAXPROCS(*pFlag)
	size := sFlag
	wg := new(sync.WaitGroup)
	sema := make(chan struct{}, *pFlag)
	out := genstring(size)
	progress := make(chan int64, 256)
	readback := make(chan string, 256)
	start := time.Now().Unix()

        if rFlag == "ro" {
		wg.Add(1)
                go readAllFiles(wg, dFlag)
        } else {
                deleteAllFiles(dFlag)

		hostname, err := os.Hostname()
		if err != nil {
			hostname = "dna"
		}
		rand.Seed(time.Now().UnixNano())
		hostname += fmt.Sprintf("_%08d",rand.Intn(100000000))

		path := fmt.Sprintf("%s/%s", dFlag, hostname)
		if _, err = os.Stat(path); os.IsNotExist(err) {
			err = os.Mkdir(path, 0755)
		}

		check(err)
		fmt.Printf("Writing %d GB with filesizes between %.1f MB and %.1f MB...\n\n", nFlag, float64(size)/1048576, float64(size)/1048576 * float64(mFlag))
		wg.Add(1)
		go spraydna(nFlag, wg, sema, readback, &out, dFlag, progress, mFlag, hostname)

		if rFlag == "rb" {
			go func() {
				readbackFiles(readback)
			}()
		}
        }

        go func() {
                wg.Wait()
                close(progress)
        }()

        // If the '-v' flag was provided, periodically print the progress stats
        var tick <-chan time.Time
        if *vFlag {

	        tick = time.Tick(1000 * time.Millisecond)
        }

        var nfiles, nbytes int64

loop:
        for {
                select {
                case size, ok := <-progress:
                        if !ok {
				break loop // progress was closed
                        }
                        nfiles++
                        nbytes += size
                case <-tick:
			if rFlag == "rb" || rFlag == "wo" {
				printProgress(nfiles, nbytes, start)
                        }
                }
        }

        // Final totals
        printDiskUsage(nfiles, nbytes, start)

}


func genstring(size int) []byte {
	//r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Building random DNA sequence of %.1f MB...\n\n", float64(size)/1048576)
	dnachars := []byte("GATC")
	dna := make([]byte, 0)
	for x := 0; x < size; x++ {
		dna = append(dna, dnachars[rand.Intn(len(dnachars))])
	}
	return dna
}

func check(e error) {
	if e != nil {
		fmt.Printf("Error: %v\n", e)
		panic(e)
	}
}

func printError(e error) {
	if e != nil {
		fmt.Printf("Error: %v\n", e)
	}
}

func readbackFiles(readback <-chan string) {
loop:
        for {
                select {
                case filename, ok := <-readback:
		        if !ok {
                            break loop
                        }
			readFile(filename)
                }
        }
}

func readFile(filename string) {
        now := time.Now()
	fmt.Printf("%v: Start reading %q\n", now.Format(time.UnixDate), filename)
        file, err := os.Open(filename)
	defer file.Close()
        printError(err)

        finfo, err1 := file.Stat()
        printError(err1)

        end_of_file := false
        var rerr error = nil
        var total int64 = 0
        for ! end_of_file  {
		data := make([]byte, 512000)
		count, err2 := file.Read(data)
                if err2 != io.EOF {
			printError(err2)
			rerr = err2
                } else {
                    end_of_file = true
                }
                total = total + int64(count)
        }

	if rerr == nil {
		now := time.Now()
		fmt.Printf("%v: Read %d bytes from %q with size %d\n", now.Format(time.UnixDate), total, filename, finfo.Size())
	}
}

func readAllFiles(wg *sync.WaitGroup, path string) {
	defer wg.Done()
	fmt.Printf("Start ReadAllFiles: %s \n", path)
	filepath.Walk(path, func(fname string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Printf("prevent panic by handling failure accessing a path %q: %v\n", fname, err)
			return err
		}
		if info.Mode().IsRegular() {
		    readFile(fname)
		}
		return nil
	})
	fmt.Printf("Complete ReadAllFiles: %s \n", path)
}


func deleteAllFiles(path string) {
        err := os.RemoveAll(path)
        printError(err)
}


func spraydna(count int, wg *sync.WaitGroup, sema chan struct{}, readback chan<- string, out *[]byte, dir string, progress chan<- int64, mFlag int, hostname string) {
	defer wg.Done()
	sema <- struct{}{}        // acquire token
	defer func() { <-sema }() // release token
	path := fmt.Sprintf("%s/%s", dir, hostname)

        // GB to Bytes
        var total int64 = int64(count) * 1000 * 1000 * 1000
	num_files := 0
        for total > 0 {
		multiple := rand.Intn(mFlag) + 1
		var size int64 = int64(multiple) * int64(len(*out))
		total = total - size
		filename := fmt.Sprintf("%s/%s-%d-%d.txt", path, hostname, num_files, multiple)
		f, err := os.Create(filename)
		check(err)
		// defer f.Close()

		w := bufio.NewWriter(f)
		writtenBytes := 0
		B := 0
		for j := 0; j < multiple; j++ {
			B, _ = w.Write(*out)
			writtenBytes += B
			//fmt.Printf("wrote %d bytes\n", writtenBytes)
			w.Flush()
		}
		f.Close()
		progress <- int64(writtenBytes)
                if readback != nil {
			readback <- filename
                }
		num_files++
	}
        close(readback)
}

func printProgress(nfiles, nbytes int64, start int64) {
        now := time.Now().Unix()
        elapsed := now - start
        if elapsed == 0 {
                elapsed = 1
        }
        fps := nfiles / elapsed
        tp := nbytes / elapsed
        fmt.Printf("Files Completed: %7d, Data Written: %5.1fGiB, Files Remaining: %7d, Cur FPS: %5d, Throughput: %4d MiB/s\n", nfiles, float64(nbytes)/1073741824, runtime.NumGoroutine(), fps, tp/1048576)
}

// Prints the final summary
func printDiskUsage(nfiles, nbytes int64, start int64) {
        stop := time.Now().Unix()
        elapsed := stop - start
        if elapsed == 0 {
                elapsed = 1
        }
        fps := nfiles / elapsed
        tp := nbytes / elapsed
        fmt.Printf("\nDone!\nNumber of Files Written: %d, Total Size: %.1fGiB, Avg FPS: %d, Avg Throughput: %d MiB/s, Elapsed Time: %d seconds\n", nfiles, float64(nbytes)/1073741824, fps, tp/1048576, elapsed)
}
