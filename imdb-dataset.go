package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/julienschmidt/httprouter"
	"github.com/raj/imdb-dataset-importer/lib"
	"github.com/raj/imdb-dataset-importer/models"
)

// Main data for downloading from IMDB

var (
	downloadList    []string
	dbAdapter       string
	dbConnectionURL string
	config          models.TomlConfig
)

func init() {
	dbAdapter = "postgres"
	if _, err := toml.DecodeFile("config.toml", &config); err != nil {
		fmt.Println(err)
		return
	}
	dbConnectionURL = fmt.Sprintf("host=%s port=%v user=%s dbname=%s sslmode=%s password=%s",
		config.DB.Server,
		config.DB.Port,
		config.DB.User,
		config.DB.DbName,
		config.DB.SslMode,
		config.DB.Password)
}

func main() {
	fmt.Printf("Import IMDB dataset.\n")

	downloadAction := flag.Bool("d", false, "download all files from aws dataset.")
	importAction := flag.Bool("i", false, "import files to database.")
	apiAction := flag.Bool("api", false, "provide api.")
	searchAction := flag.Bool("s", false, "search.")

	flag.Parse()

	if *searchAction {
		fmt.Println("search")
	}

	if *downloadAction {
		log.Println("downloadAction")

		for _, item := range config.Imdb.Files {
			url := config.Imdb.BaseURL + item + ".tsv.gz"
			downloadList = append(downloadList, url)
		}
		lib.DownloadFiles(os.TempDir(), downloadList)
		lib.DecompressFiles(os.TempDir(), downloadList)

	}

	if *importAction {
		log.Println("import Files to Database")
		lib.ImportName(filepath.Join(os.TempDir(), "name.basics.tsv"), dbConnectionURL)
		lib.ImportTitleAkas(filepath.Join(os.TempDir(), "title.akas.tsv"), dbConnectionURL)
		lib.ImportTitleBasics(filepath.Join(os.TempDir(), "title.basics.tsv"), dbConnectionURL)
		lib.ImportTitleCrew(filepath.Join(os.TempDir(), "title.crew.tsv"), dbConnectionURL)
		lib.ImportTitlePrincipals(filepath.Join(os.TempDir(), "title.episode.tsv"), dbConnectionURL)
		lib.ImportTitleRatings(filepath.Join(os.TempDir(), "title.principals.tsv"), dbConnectionURL)
		lib.ImportTitleEpisodes(filepath.Join(os.TempDir(), "title.ratings.tsv"), dbConnectionURL)
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
		r.GET("/search_for_title/:query", uc.SearchForTitle)

		bindListen := fmt.Sprintf("%s:%v", config.ServerInfo.Bind, config.ServerInfo.Port)
		http.ListenAndServe(bindListen, r)
	}

	fmt.Println("tail:", flag.Args())

	for i, arg := range flag.Args() {
		// print index and value
		fmt.Println("item", i, "is", arg)
	}

	// TODO :  remove gz file

}
