package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/julienschmidt/httprouter"
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
	TitleRatingsFile    = "title.ratings"
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
	apiAction := flag.Bool("api", false, "provide api.")

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
		downloadList[6] = MainURL + TitleRatingsFile + extension
		// TODO : use flag
		lib.DownloadFiles(os.TempDir(), downloadList)

		// TODO : use flag
		lib.DecompressFile(os.TempDir(), NameFile, extension)
		lib.DecompressFile(os.TempDir(), TitleAkasFile, extension)
		lib.DecompressFile(os.TempDir(), TitleBasicsFile, extension)
		lib.DecompressFile(os.TempDir(), TitleCrewFile, extension)
		lib.DecompressFile(os.TempDir(), TitleEpisodeFile, extension)
		lib.DecompressFile(os.TempDir(), TitlePrincipalsFile, extension)
		lib.DecompressFile(os.TempDir(), TitleRatingsFile, extension)

	}

	if *importAction {
		fmt.Printf("import Files to Database")

		lib.ImportName(filepath.Join(os.TempDir(), NameFile+".tsv"), dbConnectionURL)
		lib.ImportTitleAkas(filepath.Join(os.TempDir(), TitleAkasFile+".tsv"), dbConnectionURL)
		lib.ImportTitleBasics(filepath.Join(os.TempDir(), TitleBasicsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleCrew(filepath.Join(os.TempDir(), TitleCrewFile+".tsv"), dbConnectionURL)
		lib.ImportTitlePrincipals(filepath.Join(os.TempDir(), TitlePrincipalsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleRatings(filepath.Join(os.TempDir(), TitleRatingsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleEpisodes(filepath.Join(os.TempDir(), TitleEpisodeFile+".tsv"), dbConnectionURL)
		lib.SanityzeDb(dbConnectionURL)
	}

	if *apiAction {
		fmt.Printf("api")
		fmt.Printf("API\n ")

		db, err := gorm.Open(dbAdapter, dbConnectionURL)
		if err != nil {
			log.Fatal(err)
		}

		r := httprouter.New()
		// Get a MainController instance
		uc := lib.NewMainController(db)

		// Get main resource
		r.GET("/", uc.GetMain)

		http.ListenAndServe("0.0.0.0:3000", r)
	}

	fmt.Println("tail:", flag.Args())

	for i, arg := range flag.Args() {
		// print index and value
		fmt.Println("item", i, "is", arg)
	}

	// TODO :  remove gz file

}
