package lib

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/lib/pq"
	"github.com/valyala/tsvreader"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

func execSQL(label string, query string, db *sql.DB) {
	fmt.Println(time.Now().Format("3:04PM"), label)
	db.Exec(query)
}

func SanityzeDb(dbUrl string) {
	p := fmt.Println
	db, err := sql.Open("postgres", dbUrl)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()
	execSQL("create search conf on postgres", "CREATE EXTENSION unaccent;", db)
	execSQL("search conf", "CREATE TEXT SEARCH CONFIGURATION search_conf ( COPY = english );ALTER TEXT SEARCH CONFIGURATION search_conf ALTER MAPPING FOR hword, hword_part, word WITH unaccent, english_stem;", db)
	execSQL("indexing name_basics nconst", "CREATE INDEX ON public.name_basics (nconst);", db)
	execSQL("indexing name_basics primary name", "CREATE INDEX name_basics_primary_idx ON public.name_basics USING gin(to_tsvector('search_conf', primary_name));", db)
	execSQL("indexing ratings tconst", "CREATE INDEX ON public.title_ratings (tconst);", db)
	execSQL("indexing episodes tconst", "CREATE INDEX ON public.title_episodes (tconst);", db)
	execSQL("indexing episodes parent_tconst", "CREATE INDEX ON public.title_episodes (parent_tconst);", db)
	execSQL("indexing crew tconst", "CREATE INDEX ON public.title_crew (tconst);", db)
	execSQL("indexing title_basics tconst", "CREATE INDEX ON public.title_basics (tconst);", db)
	execSQL("indexing title_akas title_id", "CREATE INDEX ON public.title_akas (title_id);", db)
	execSQL("indexing title_akas title", "CREATE INDEX akas_title_idx ON public.title_akas USING gin(to_tsvector('search_conf', title));", db)
	execSQL("indexing principals tconst", "CREATE INDEX ON public.title_principals (tconst);", db)
	execSQL("indexing principals nconst", "CREATE INDEX ON public.title_principals (nconst);", db)
	p(time.Now().Format("3:04PM"), "indexing finished")
}

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

	createNameTable := `DROP TABLE IF EXISTS public.title_ratings;CREATE TABLE IF NOT EXISTS public.title_ratings
		(
			tconst text NOT NULL,
			average_rating numeric(3,1) ,
			num_votes int 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_ratings")

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
		averageRating, _ := strconv.ParseFloat(col2, 32)
		numberOfVotes, _ := strconv.Atoi(col3)

		if counter > 0 {
			_, err = stmt.Exec(col1, averageRating, numberOfVotes)
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

	createNameTable := `DROP TABLE IF EXISTS public.title_episodes;CREATE TABLE IF NOT EXISTS public.title_episodes
		(
			tconst text  NOT NULL,
			parent_tconst text ,
			season_number int,
			episode_number int
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_episodes")

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

	createNameTable := `DROP TABLE IF EXISTS public.title_principals;CREATE TABLE IF NOT EXISTS public.title_principals
		(
			tconst text  NOT NULL,
			ordering int,
			nconst text,
			category text,
			job text,
			characters text 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_principals")

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
		ordering, _ := strconv.Atoi(col2)
		if col5 == "N" {
			col5 = "" // job field
		}
		if col6 == "N" {
			col6 = "" // characters field
		}
		if counter > 0 {
			_, err = stmt.Exec(col1, ordering, col3, col4, col5, col6)
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
			tconst text  NOT NULL,
			directors text ,
			writers text 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_crew")

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
			tconst text  NOT NULL,
			title_type text ,
			primary_title text ,
			original_title text ,
			is_adult text ,
			start_year text ,
			end_year text ,
			runtime_minutes text ,
			genres text 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_basics")

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
			title_id text  NOT NULL,
			ordering text ,
			title text ,
			region text ,
			language text ,
			types text ,
			attributes text ,
			is_original_title text 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.title_akas")

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

	createNameTable := `DROP TABLE IF EXISTS public.name_basics;CREATE TABLE IF NOT EXISTS public.name_basics
		(
			nconst text NOT NULL,
			primary_name text ,
			birth_year int ,
			death_year int ,
			primary_profession text ,
			known_for_titles text 
		)`
	db.Exec(createNameTable)
	db.Exec("TRUNCATE public.name_basics")

	stmt, err := txn.Prepare(pq.CopyIn("name_basics", "nconst", "primary_name", "birth_year", "death_year", "primary_profession", "known_for_titles"))
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
