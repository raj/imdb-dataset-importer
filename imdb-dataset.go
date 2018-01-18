package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

// Main data for downloading from IMDB
const (
	MainURL             = "https://datasets.imdbws.com/"
	NameFile            = "name.basics.tsv.gz"
	TitleAkasFile       = "title.akas.tsv.gz"
	TitleBasicsFile     = "title.basics.tsv.gz"
	TitleCrewFile       = "title.crew.tsv.gz"
	TitleEpisodeFile    = "title.episode.tsv.gz"
	TitlePrincipalsFile = "title.principals.tsv.gz"
	TileRatingsFile     = "title.ratings.tsv.gz"
)

var (
	downloadList    []string
	dbAdapter       string
	dbConnectionURL string
)

func init() {
	dbAdapter = "postgres"
	dbConnectionURL = "host=127.0.0.1 port=5433 user=postgres dbname=imdb sslmode=disable password=admin"
}

func main() {
	fmt.Printf("Hello, world.\n")

	// TODO : use flag
	// downloadFiles()

	// TODO : use flag
	// decompressFile(NameFile)
	// decompressFile(TitleAkasFile)
	// decompressFile(TitleBasicsFile)
	// decompressFile(TitleCrewFile)
	// decompressFile(TitleEpisodeFile)
	// decompressFile(TitlePrincipalsFile)
	// decompressFile(TileRatingsFile)

	// TODO :  remove gz file

	// import data to database
	db, err := gorm.Open(dbAdapter, dbConnectionURL)
	if err != nil {
		log.Fatal(err)
	}
	db.Exec("")

}

func decompressFile(name string) {
	fmt.Println(name)
	filepath := filepath.Base(name)
	handle, err := UnpackGzipFile(filepath)
	if err != nil {
		fmt.Println("[ERROR] Unzip file:", err)
	}
	fmt.Println(handle)

}

func downloadFiles() {
	downloadList := make([]string, 7)
	downloadList[0] = NameFile
	downloadList[1] = TitleAkasFile
	downloadList[2] = TitleBasicsFile
	downloadList[3] = TitleCrewFile
	downloadList[4] = TitleEpisodeFile
	downloadList[5] = TitlePrincipalsFile
	downloadList[6] = TileRatingsFile
	// fmt.Println(downloadList)

	var wg sync.WaitGroup
	p := mpb.New(mpb.WithWaitGroup(&wg))
	fmt.Println(p)

	for _, item := range downloadList {
		url := MainURL + item
		wg.Add(1)
		go download(&wg, p, item, url)
	}

	p.Stop()
	fmt.Println("Finished")
}

// code from https://github.com/vbauerster/mpb/blob/master/examples/io/multiple/main.go
func download(wg *sync.WaitGroup, p *mpb.Progress, name, url string) {
	defer wg.Done()
	resp, err := http.Get(url)
	if err != nil {
		log.Printf("%s: %v", name, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("non-200 status: %s", resp.Status)
		log.Printf("%s: %v", name, err)
		return
	}

	size := resp.ContentLength

	// create dest
	destName := filepath.Base(url)
	dest, err := os.Create(destName)
	if err != nil {
		err = fmt.Errorf("Can't create %s: %v", destName, err)
		log.Printf("%s: %v", name, err)
		return
	}

	// create bar with appropriate decorators
	bar := p.AddBar(size,
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
			decor.CountersKibiByte("%6.1f / %6.1f", 18, 0),
		),
		mpb.AppendDecorators(decor.ETA(5, decor.DwidthSync)),
	)

	// create proxy reader
	reader := bar.ProxyReader(resp.Body)
	// and copy from reader
	_, err = io.Copy(dest, reader)

	if closeErr := dest.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		log.Printf("%s: %v", name, err)
	}
}

func UnpackGzipFile(gzFilePath string) (int64, error) {

	dstFilePath := gzFilePath[0 : len(gzFilePath)-3]
	gzFile, err := os.Open(gzFilePath)
	if err != nil {
		return 0, fmt.Errorf("Failed to open file %s for unpack: %s", gzFilePath, err)
	}
	dstFile, err := os.OpenFile(dstFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0660)
	if err != nil {
		return 0, fmt.Errorf("Failed to create destination file %s for unpack: %s", dstFilePath, err)
	}

	ioReader, ioWriter := io.Pipe()

	go func() { // goroutine leak is possible here
		gzReader, _ := gzip.NewReader(gzFile)
		// it is important to close the writer or reading from the other end of the
		// pipe or io.copy() will never finish
		defer func() {
			gzFile.Close()
			gzReader.Close()
			ioWriter.Close()
		}()

		io.Copy(ioWriter, gzReader)
	}()

	written, err := io.Copy(dstFile, ioReader)
	if err != nil {
		return 0, err // goroutine leak is possible here
	}
	ioReader.Close()
	dstFile.Close()

	return written, nil
}
