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
		fmt.Printf("downloadAction\n")

		downloadList := make([]string, 7)
		extension := ".tsv.gz"
		downloadList[0] = config.Imdb.BaseURL + config.Imdb.NameBasicsFile + extension
		downloadList[1] = config.Imdb.BaseURL + config.Imdb.TitleAkasFile + extension
		downloadList[2] = config.Imdb.BaseURL + config.Imdb.TitleBasicsFile + extension
		downloadList[3] = config.Imdb.BaseURL + config.Imdb.TitleCrewFile + extension
		downloadList[4] = config.Imdb.BaseURL + config.Imdb.TitleEpisodeFile + extension
		downloadList[5] = config.Imdb.BaseURL + config.Imdb.TitlePrincipalsFile + extension
		downloadList[6] = config.Imdb.BaseURL + config.Imdb.TitleRatingsFile + extension
		// TODO : use flag
		lib.DownloadFiles(os.TempDir(), downloadList)

		// TODO : use flag
		lib.DecompressFile(os.TempDir(), config.Imdb.NameBasicsFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitleAkasFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitleBasicsFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitleCrewFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitleEpisodeFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitlePrincipalsFile, extension)
		lib.DecompressFile(os.TempDir(), config.Imdb.TitleRatingsFile, extension)

	}

	if *importAction {
		fmt.Printf("import Files to Database")

		lib.ImportName(filepath.Join(os.TempDir(), config.Imdb.NameBasicsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleAkas(filepath.Join(os.TempDir(), config.Imdb.TitleAkasFile+".tsv"), dbConnectionURL)
		lib.ImportTitleBasics(filepath.Join(os.TempDir(), config.Imdb.TitleBasicsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleCrew(filepath.Join(os.TempDir(), config.Imdb.TitleCrewFile+".tsv"), dbConnectionURL)
		lib.ImportTitlePrincipals(filepath.Join(os.TempDir(), config.Imdb.TitlePrincipalsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleRatings(filepath.Join(os.TempDir(), config.Imdb.TitleRatingsFile+".tsv"), dbConnectionURL)
		lib.ImportTitleEpisodes(filepath.Join(os.TempDir(), config.Imdb.TitleEpisodeFile+".tsv"), dbConnectionURL)
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
