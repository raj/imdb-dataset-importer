package main

import (
	"fmt"
	"log"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
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

	// downloadList := make([]string, 7)
	// downloadList[0] = MainURL + NameFile
	// downloadList[1] = MainURL + TitleAkasFile
	// downloadList[2] = MainURL + TitleBasicsFile
	// downloadList[3] = MainURL + TitleCrewFile
	// downloadList[4] = MainURL + TitleEpisodeFile
	// downloadList[5] = MainURL + TitlePrincipalsFile
	// downloadList[6] = MainURL + TileRatingsFile

	// TODO : use flag
	// lib.DownloadFiles(downloadList)

	// TODO : use flag
	// lib.DecompressFile(NameFile)
	// lib.DecompressFile(TitleAkasFile)
	// lib.DecompressFile(TitleBasicsFile)
	// lib.DecompressFile(TitleCrewFile)
	// lib.DecompressFile(TitleEpisodeFile)
	// lib.DecompressFile(TitlePrincipalsFile)
	// lib.DecompressFile(TileRatingsFile)

	// TODO :  remove gz file

	// import data to database
	db, err := gorm.Open(dbAdapter, dbConnectionURL)
	if err != nil {
		log.Fatal(err)
	}

	// db.AutoMigrate(&models.Name{})

	createNameTable := `CREATE TABLE public.name
		(
			nconst text COLLATE pg_catalog."default" NOT NULL,
			primaryName text COLLATE pg_catalog."default",
			birthYear text COLLATE pg_catalog."default",
			deathYear text COLLATE pg_catalog."default",
			primaryProfession text COLLATE pg_catalog."default",
			knownForTitles text COLLATE pg_catalog."default"
		)`

	db.Exec(createNameTable)
	db.Exec("COPY name	 FROM 'E:/Projects/gopath/src/github.com/raj/imdb-dataset-importer/name.basics.tsv' DELIMITER E'\t';")

}
