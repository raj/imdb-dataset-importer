package lib

import (
	"github.com/jinzhu/gorm"
    "fmt"
	"os"
	// "math/rand"
	// "time"

	// "github.com/valyala/tsvreader"
	"io"
	"bytes"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)


func ImportName(filename string, db *gorm.DB) {

	fmt.Println(filename)

	
	f, err := os.Open(filename)
	if err != nil {
        panic(err)
	} 
	
	count, err := lineCounter(f)

    if err != nil {
        fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
        os.Exit(1)
	}

	fmt.Println(count)

	_, err = f.Seek(0, 0)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
        os.Exit(1)
	}


	p := mpb.New()

	total := count
	name := "import names"
	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	for i := 0; i < count; i++ {
		// time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second / 100)
		bar.Increment()
	}

	p.Stop()



	// r := tsvreader.New(f)
	// for r.Next() {
	// 	col1 := r.String()
	// 	col2 := r.String()
	// 	col3 := r.String()
	// 	col4 := r.String()
	// 	col5 := r.String()
	// 	col6 := r.String()	
	// 	fmt.Printf("col1=%s, col2=%s,col3=%s, col4=%s,col5=%s, col6=%s\n", col1, col2, col3, col4, col5, col6)
	// }
	// if err := r.Error(); err != nil {
	// 	fmt.Printf("unexpected error: %s", err)
	// }
	defer f.Close()

	// createNameTable := `CREATE TABLE IF NOT EXISTS public.name
	// 	(
	// 		nconst text  NOT NULL,
	// 		primaryName text,
	// 		birthYear text ,
	// 		deathYear text ,
	// 		primaryProfession text ,
	// 		knownForTitles text 
	// 	)`
    // fmt.Println(createNameTable)
}



func lineCounter(r io.Reader) (int, error) {
    buf := make([]byte, 32*1024)
    count := 0
    lineSep := []byte{'\n'}

    for {
        c, err := r.Read(buf)
        count += bytes.Count(buf[:c], lineSep)

        switch {
        case err == io.EOF:
            return count, nil

        case err != nil:
            return count, err
        }
    }
}