package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/raj/imdb-dataset-importer/lib"
)

// Main data for downloading from IMDB
const (
	MainURL             = "https://datasets.imdbws.com/"
	NameFile            = "name.basics"
	TitleAkasFile       = "title.akas"
	TitleBasicsFile     = "title.basics"
	TitleCrewFile       = "title.crew"
	TitleEpisodeFile    = "title.episode"
	TitlePrincipalsFile = "title.principals"
	TileRatingsFile     = "title.ratings"
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
	fmt.Printf("Import IMDB dataset.\n")

	downloadAction := flag.Bool("d", false, "download all files from aws dataset.")
	importAction := flag.Bool("i", false, "import files to database.")

	flag.Parse()

	if *downloadAction {
		fmt.Printf("downloadAction\n")

		downloadList := make([]string, 7)
		extension := ".tsv.gz"
		downloadList[0] = MainURL + NameFile + extension
		downloadList[1] = MainURL + TitleAkasFile + extension
		downloadList[2] = MainURL + TitleBasicsFile + extension
		downloadList[3] = MainURL + TitleCrewFile + extension
		downloadList[4] = MainURL + TitleEpisodeFile + extension
		downloadList[5] = MainURL + TitlePrincipalsFile + extension
		downloadList[6] = MainURL + TileRatingsFile + extension
		// TODO : use flag
		lib.DownloadFiles(os.TempDir(), downloadList)

		// TODO : use flag
		lib.DecompressFile(os.TempDir(), NameFile, extension)
		lib.DecompressFile(os.TempDir(), TitleAkasFile, extension)
		lib.DecompressFile(os.TempDir(), TitleBasicsFile, extension)
		lib.DecompressFile(os.TempDir(), TitleCrewFile, extension)
		lib.DecompressFile(os.TempDir(), TitleEpisodeFile, extension)
		lib.DecompressFile(os.TempDir(), TitlePrincipalsFile, extension)
		lib.DecompressFile(os.TempDir(), TileRatingsFile, extension)

	}

	if *importAction {
		fmt.Printf("import Files to Database\n")

		// import data to database
		db, err := gorm.Open(dbAdapter, dbConnectionURL)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
		lib.ImportName(filepath.Join(os.TempDir(), NameFile+".tsv"), dbConnectionURL)

		// createNameTable := `CREATE TABLE public.name
		// 	(
		// 		nconst text COLLATE pg_catalog."default" NOT NULL,
		// 		primaryName text COLLATE pg_catalog."default",
		// 		birthYear text COLLATE pg_catalog."default",
		// 		deathYear text COLLATE pg_catalog."default",
		// 		primaryProfession text COLLATE pg_catalog."default",
		// 		knownForTitles text COLLATE pg_catalog."default"
		// 	)`

		// db.Exec(createNameTable)
		// db.Exec("COPY name FROM 'C:/Users/Raj/AppData/Local/Temp/name.basics.tsv' DELIMITER E'\t';")

	}

	fmt.Println("tail:", flag.Args())

	for i, arg := range flag.Args() {
		// print index and value
		fmt.Println("item", i, "is", arg)
	}

	// TODO :  remove gz file

	// db.AutoMigrate(&models.Name{})

	// createNameTable := `CREATE TABLE public.name
	// 	(
	// 		nconst text COLLATE pg_catalog."default" NOT NULL,
	// 		primaryName text COLLATE pg_catalog."default",
	// 		birthYear text COLLATE pg_catalog."default",
	// 		deathYear text COLLATE pg_catalog."default",
	// 		primaryProfession text COLLATE pg_catalog."default",
	// 		knownForTitles text COLLATE pg_catalog."default"
	// 	)`

	// db.Exec(createNameTable)
	// db.Exec("COPY name FROM 'E:/Projects/gopath/src/github.com/raj/imdb-dataset-importer/name.basics.tsv' DELIMITER E'\t';")

}
