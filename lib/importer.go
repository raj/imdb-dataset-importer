package lib

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/lib/pq"
	"github.com/valyala/tsvreader"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func ImportName(filename string, dbUrl string) {

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("open ping: %v", err)
	}
	defer db.Close()

	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("begin: %v", err)
	}

	createNameTable := `CREATE TABLE IF NOT EXISTS public.name
		(
			nconst text COLLATE pg_catalog."default" NOT NULL,
			primaryName text COLLATE pg_catalog."default",
			birthYear text COLLATE pg_catalog."default",
			deathYear text COLLATE pg_catalog."default",
			primaryProfession text COLLATE pg_catalog."default",
			knownForTitles text COLLATE pg_catalog."default"
		)`
	fmt.Println(createNameTable)
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("name", "nconst", "primaryname", "birthyear", "deathyear", "primaryprofession", "knownfortitles"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
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

	r := tsvreader.New(f)

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

	counter := 0
	birthYear := 0
	deathYear := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		col4 := r.String()
		col5 := r.String()
		col6 := r.String()

		if counter > 0 {
			// fmt.Printf("col1=%s, col2=%s,col3=%s, col4=%s,col5=%s, col6=%s, counter=%s\n", col1, col2, col3, col4, col5, col6, counter)
			birthYear, err = strconv.Atoi(col3)
			if err != nil {
				birthYear = 0
			}
			deathYear, err = strconv.Atoi(col4)
			if err != nil {
				deathYear = 0
			}
			_, err = stmt.Exec(col1, col2, birthYear, deathYear, col5, col6)
			if err != nil {
				log.Fatalf("exec: %v", err)
			}

		}

		counter = counter + 1
		bar.Increment()
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatalf("exec: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatalf("stmt close: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatalf("commit: %v", err)
	}

	// f, err := os.Open(filename)
	// if err != nil {
	// 	panic(err)
	// }

	// sql := "\\COPY name	 FROM '" + filename + "' DELIMITER E'\\t';"
	// sql := "COPY name FROM 'C:/Users/Raj/AppData/Local/Temp/name.basics.tsv' DELIMITER E'\t';"
	// db.Exec("COPY name FROM 'C:/Users/Raj/AppData/Local/Temp/name.basics.tsv' DELIMITER E'\t';")

	// fmt.Print(sql)
	// db.Exec(sql)
}

// func ImportName(filename string, db *gorm.DB) {
// 	db.AutoMigrate(&models.Name{})
// 	fmt.Println(filename)

// 	f, err := os.Open(filename)
// 	if err != nil {
// 		panic(err)
// 	}

// 	count, err := lineCounter(f)

// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
// 		os.Exit(1)
// 	}

// 	fmt.Println(count)

// 	_, err = f.Seek(0, 0)
// 	if err != nil {
// 		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
// 		os.Exit(1)
// 	}

// 	p := mpb.New()

// 	total := count
// 	name := "import names"
// 	bar := p.AddBar(int64(total),
// 		mpb.PrependDecorators(
// 			decor.StaticName(name, 0, 0),
// 		),
// 		mpb.AppendDecorators(
// 			decor.CountersNoUnit("%d / %d", 12, 0),
// 		),
// 	)
// 	r := tsvreader.New(f)

// 	// for r.Next() {
// 	// 	// col1 := r.String()
// 	// 	// col2 := r.String()
// 	// 	// col3 := r.String()
// 	// 	// col4 := r.String()
// 	// 	// col5 := r.String()
// 	// 	// col6 := r.String()
// 	// 	// fmt.Printf("col1=%s, col2=%s,col3=%s, col4=%s,col5=%s, col6=%s\n", col1, col2, col3, col4, col5, col6)

// 	// 	bar.Increment()
// 	// }
// 	// if err := r.Error(); err != nil {
// 	// 	fmt.Printf("unexpected error: %s", err)
// 	// }

// 	// for i := 0; i < count; i++ {
// 	// 	// time.Sleep(time.Duration(rand.Intn(10)+1) * time.Second / 100)
// 	// 	r.Next()
// 	// 	if i >= 1 {
// 	// 		nconst := r.String()
// 	// 		primaryName := r.String()
// 	// 		birthYear := r.Int()
// 	// 		deathYear := r.Int()
// 	// 		primaryProfession := r.String()
// 	// 		knownForTitles := r.String()

// 	// 		fmt.Printf("nconst = %s", nconst)
// 	// 		name := models.Name{Nconst: nconst, PrimaryName: primaryName, BirthYear: birthYear, DeathYear: deathYear, PrimaryProfession: primaryProfession, KnownForTitles: knownForTitles}
// 	// 		// fmt.Println(name)
// 	// 		db.Create(&name)
// 	// 	}
// 	// 	bar.Increment()
// 	// }

// 	// if err := r.Error(); err != nil {
// 	// 	fmt.Printf("unexpected error: %s", err)
// 	// }
// 	counter := 0
// 	for r.Next() {
// 		col1 := r.String()
// 		col2 := r.String()
// 		col3 := r.String()
// 		col4 := r.String()
// 		col5 := r.String()
// 		col6 := r.String()

// 		if counter > 0 {
// 			// fmt.Printf("col1=%s, col2=%s,col3=%s, col4=%s,col5=%s, col6=%s, counter=%s\n", col1, col2, col3, col4, col5, col6, counter)
// 			birthYear, err := strconv.Atoi(col3)
// 			if err != nil {
// 			}
// 			deathYear, err := strconv.Atoi(col4)
// 			if err != nil {
// 			}

// 			name := models.Name{Nconst: col1, PrimaryName: col2, BirthYear: birthYear, DeathYear: deathYear, PrimaryProfession: col5, KnownForTitles: col6}
// 			db.Create(&name)
// 		}
// 		counter = counter + 1
// 		bar.Increment()
// 	}
// 	if err := r.Error(); err != nil {
// 		fmt.Printf("unexpected error: %s", err)
// 	}

// 	p.Stop()

// 	// r := tsvreader.New(f)
// 	// for r.Next() {
// 	// 	col1 := r.String()
// 	// 	col2 := r.String()
// 	// 	col3 := r.String()
// 	// 	col4 := r.String()
// 	// 	col5 := r.String()
// 	// 	col6 := r.String()
// 	// 	fmt.Printf("col1=%s, col2=%s,col3=%s, col4=%s,col5=%s, col6=%s\n", col1, col2, col3, col4, col5, col6)
// 	// }
// 	// if err := r.Error(); err != nil {
// 	// 	fmt.Printf("unexpected error: %s", err)
// 	// }
// 	defer f.Close()

// 	// createNameTable := `CREATE TABLE IF NOT EXISTS public.name
// 	// 	(
// 	// 		nconst text  NOT NULL,
// 	// 		primaryName text,
// 	// 		birthYear text ,
// 	// 		deathYear text ,
// 	// 		primaryProfession text ,
// 	// 		knownForTitles text
// 	// 	)`
// 	// fmt.Println(createNameTable)
// }

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
