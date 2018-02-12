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

func ImportTitleRatings(filename string, dbUrl string) {
	// tconst  averageRating   numVotes
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_ratings
		(
			tconst text COLLATE pg_catalog."default" NOT NULL,
			average_rating text COLLATE pg_catalog."default",
			num_votes text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_ratings", "tconst", "average_rating", "num_votes"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "ratings"

	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		if counter > 0 {
			_, err = stmt.Exec(col1, col2, col3)
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

}

func ImportTitleEpisodes(filename string, dbUrl string) {
	// tconst  parentTconst    seasonNumber    episodeNumber
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_episodes
		(
			tconst text COLLATE pg_catalog."default" NOT NULL,
			parent_tconst text COLLATE pg_catalog."default",
			season_number int,
			episode_number int
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_episodes", "tconst", "parent_tconst", "season_number", "episode_number"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "episodes"

	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		col4 := r.String()
		seasonNumber, _ := strconv.Atoi(col3)
		episodeNumber, _ := strconv.Atoi(col4)
		if counter > 0 {
			_, err = stmt.Exec(col1, col2, seasonNumber, episodeNumber)
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

}

func ImportTitlePrincipals(filename string, dbUrl string) {
	// tconst  ordering        nconst  category        job     characters
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_principals
		(
			tconst text COLLATE pg_catalog."default" NOT NULL,
			ordering text COLLATE pg_catalog."default",
			nconst text COLLATE pg_catalog."default",
			category text COLLATE pg_catalog."default",
			job text COLLATE pg_catalog."default",
			characters text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_principals", "tconst", "ordering", "nconst", "category", "job", "characters"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "title principals"

	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		col4 := r.String()
		col5 := r.String()
		col6 := r.String()
		if counter > 0 {
			_, err = stmt.Exec(col1, col2, col3, col4, col5, col6)
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

}

func ImportTitleCrew(filename string, dbUrl string) {
	// tconst  directors       writers
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_crew
		(
			tconst text COLLATE pg_catalog."default" NOT NULL,
			directors text COLLATE pg_catalog."default",
			writers text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_crew", "tconst", "directors", "writers"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "crew"

	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		if counter > 0 {
			_, err = stmt.Exec(col1, col2, col3)
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

}

func ImportTitleBasics(filename string, dbUrl string) {
	// tconst  titleType       primaryTitle    originalTitle   isAdult startYear       endYear runtimeMinutes  genres
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_basics
		(
			tconst text COLLATE pg_catalog."default" NOT NULL,
			title_type text COLLATE pg_catalog."default",
			primary_title text COLLATE pg_catalog."default",
			original_title text COLLATE pg_catalog."default",
			is_adult text COLLATE pg_catalog."default",
			start_year text COLLATE pg_catalog."default",
			end_year text COLLATE pg_catalog."default",
			runtime_minutes text COLLATE pg_catalog."default",
			genres text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_basics", "tconst", "title_type", "primary_title", "original_title", "is_adult", "start_year", "end_year", "runtime_minutes", "genres"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "title basics"

	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		col4 := r.String()
		col5 := r.String()
		col6 := r.String()
		col7 := r.String()
		col8 := r.String()
		col9 := r.String()
		if counter > 0 {
			_, err = stmt.Exec(col1, col2, col3, col4, col5, col6, col7, col8, col9)
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

}

func ImportTitleAkas(filename string, dbUrl string) {
	// titleId ordering        title   region  language        types   attributes      isOriginalTitle
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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.title_akas
		(
			title_id text COLLATE pg_catalog."default" NOT NULL,
			ordering text COLLATE pg_catalog."default",
			title text COLLATE pg_catalog."default",
			region text COLLATE pg_catalog."default",
			language text COLLATE pg_catalog."default",
			types text COLLATE pg_catalog."default",
			attributes text COLLATE pg_catalog."default",
			is_original_title text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("title_akas", "title_id", "ordering", "title", "region", "language", "types", "attributes", "is_original_title"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "title akas"
	bar := p.AddBar(int64(total),
		mpb.PrependDecorators(
			decor.StaticName(name, 0, 0),
		),
		mpb.AppendDecorators(
			decor.CountersNoUnit("%d / %d", 12, 0),
		),
	)

	counter := 0

	for r.Next() {
		col1 := r.String()
		col2 := r.String()
		col3 := r.String()
		col4 := r.String()
		col5 := r.String()
		col6 := r.String()
		col7 := r.String()
		col8 := r.String()

		if counter > 0 {
			_, err = stmt.Exec(col1, col2, col3, col4, col5, col6, col7, col8)
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

}

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

	createNameTable := `CREATE TABLE IF NOT EXISTS public.name_basics
		(
			nconst text COLLATE pg_catalog."default" NOT NULL,
			primaryName text COLLATE pg_catalog."default",
			birth_year text COLLATE pg_catalog."default",
			death_year text COLLATE pg_catalog."default",
			primary_profession text COLLATE pg_catalog."default",
			known_for_titles text COLLATE pg_catalog."default"
		)`
	db.Exec(createNameTable)

	stmt, err := txn.Prepare(pq.CopyIn("name_basics", "nconst", "primaryname", "birth_year", "death_year", "primary_profession", "known_for_titles"))
	if err != nil {
		log.Fatalf("prepare: %v", err)
	}

	count, err := lineCounter(f)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while counting: %v", err)
		os.Exit(1)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Encountered error while seek 0 : %v", err)
		os.Exit(1)
	}

	r := tsvreader.New(f)

	p := mpb.New()
	defer p.Stop()

	total := count
	name := "names"
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
