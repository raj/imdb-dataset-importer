package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

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
	downloadList []string
)

func main() {
	fmt.Printf("Hello, world.\n")
	// downloadFiles()
	decompressFile(NameFile)
}

func decompressFile(name string) {
	fmt.Println(name)
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
