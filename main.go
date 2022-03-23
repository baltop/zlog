package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	compressOld = flag.Bool("gzip", false, "Gzip old files")
	outputFile  = flag.String("output", "./output.log", "Output file")
	maxFiles    = flag.Int("max-files", 31, "Maximum files to preserve")
	maxFileSize = flag.Int("max-size", 300*1024*1024, "Maximum file size")
	timeInsert  = flag.Bool("time", false, "add timestamp default false, use -time=true")
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "%s\n\treads lines from stdin in writes them compressed with gzip\n\tinto 'output' rotating them as specified by flags\n", path.Base(os.Args[0]))
		fmt.Fprintf(os.Stderr, "\nFLAGS:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	var appender Appender
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	appender.ticker = ticker

	appender.lastFileChan = make(chan string, 100)
	appender.openFile()
	defer appender.closeFile()
	go appender.listenForSignals()
	go appender.manageFiles()

	reader := bufio.NewReader(os.Stdin)
	// var readChan chan string
	readChan := make(chan string)
	go func() {
		for {
			line, read_err := reader.ReadString('\n')
			if read_err == io.EOF {
				// appender.closeFile()
				log.Fatalln("Exit: stdin is EOF :", read_err)
			}

			if len(line) == 0 {
				continue
			}
			readChan <- line
		}
	}()
	for !appender.closed {

		select {
		case t := <-ticker.C:
			// fmt.Println("Tick at", t)
			if t.Hour() == 0 && t.Minute() == 0 {
				appender.rotateFile()
			}

		case line := <-readChan:
			appender.Append(line)
		}

	}

	appender.wg.Wait()
}

type Appender struct {
	file     *os.File
	filePath string
	// writer       *bufio.Writer
	bytesWritten int
	closed       bool
	ticker       *time.Ticker

	wg           sync.WaitGroup
	lastFileChan chan string
}

func (s *Appender) listenForSignals() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	// Block until a signal is received.
	<-c
	s.closed = true
	s.closeFile()
	s.wg.Wait()
	os.Exit(0)
}

func (s *Appender) openFile() {
	f, err := os.OpenFile(*outputFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("ERROR: cannot open file:", err)
	}

	s.file = f
	s.filePath = *outputFile
	// s.writer = bufio.NewWriter(f)
	st, err := s.file.Stat()
	if err != nil {
		log.Fatalln("ERROR", err)
	}
	s.bytesWritten = int(st.Size())
}

func (s *Appender) closeFile() {
	// s.writer.Flush()
	s.ticker.Stop()
	s.file.Close()
}

func (s *Appender) rotateFile() {
	s.closeFile()

	archiveName := s.archiveFileName()
	os.Rename(s.filePath, archiveName)
	s.wg.Add(1)
	s.lastFileChan <- archiveName

	s.openFile()
}

func (s *Appender) manageFiles() {
	for lastFile := range s.lastFileChan {
		if *compressOld {
			s.compressFile(lastFile)
		}
		s.removeOldFiles()
		s.wg.Done()
	}
}

func (s *Appender) removeOldFiles() {
	infos, err := ioutil.ReadDir(path.Dir(s.filePath))
	if err != nil {
		log.Fatalln("ERROR", err)
	}

	archives := []string{}
	baseName := path.Base(s.filePath)
	dir := path.Dir(s.filePath)
	for _, info := range infos {
		name := info.Name()
		if strings.HasPrefix(name, baseName+"_2") {
			archives = append(archives, name)
		}
	}

	sort.Strings(archives)
	for index := 0; index < len(archives)-*maxFiles; index++ {
		fileName := archives[index]
		err := os.Remove(path.Join(dir, fileName))
		if err != nil {
			log.Fatalln("ERROR", err)
		}
	}
}

func (s *Appender) compressFile(fileName string) {
	inFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalln("ERROR: cannot open file:", err)
	}

	outFile, err := os.OpenFile(fileName+".gz", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("ERROR: cannot open file:", err)
	}

	w := gzip.NewWriter(outFile)

	io.Copy(w, inFile)

	w.Close()
	inFile.Close()
	outFile.Close()

	os.Remove(fileName)
}

func (s *Appender) archiveFileName() string {
	ts := time.Now().Format("2006-01-02T150405.999")
	return s.filePath + "_" + ts
}

func (s *Appender) Append(line string) {
	if s.bytesWritten >= *maxFileSize {
		s.rotateFile()
	}
	if *timeInsert {
		curT := time.Now().Format("2006-01-02 15:04:05 - ")
		line = curT + line
	}
	n, _ := s.file.WriteString(line)
	// s.file.WriteString()

	s.bytesWritten += n + 1
}
