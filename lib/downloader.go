package lib

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func DecompressFiles(tmpDir string, downloadList []string) {

	for _, item := range downloadList {
		u, _ := url.Parse(item)
		filename := strings.Trim(u.Path, "/")
		fmt.Println("decompress file : ", filename)
		filepath := filepath.Join(tmpDir, filename)
		_, err := unpackGzipFile(filepath)
		if err != nil {
			fmt.Println("[ERROR] Unzip file:", err)
		}
	}

}

func DownloadFiles(tmpDir string, downloadList []string) {
	// fmt.Println(downloadList)

	var wg sync.WaitGroup
	p := mpb.New(mpb.WithWaitGroup(&wg))
	// fmt.Println(p)

	for _, item := range downloadList {
		url := item
		wg.Add(1)
		go download(&wg, p, item, tmpDir, url)
	}

	p.Stop()
	fmt.Println("Download Finished")
}

// code from https://github.com/vbauerster/mpb/blob/master/examples/io/multiple/main.go
func download(wg *sync.WaitGroup, p *mpb.Progress, name, tmpDir string, url string) {
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
	dest, err := os.Create(filepath.Join(tmpDir, destName))
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

func unpackGzipFile(gzFilePath string) (int64, error) {

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
	//TODO : remove original file

	return written, nil
}
