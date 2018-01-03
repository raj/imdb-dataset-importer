package main

import "fmt"
import "github.com/raj/imdb-dataset-importer/lib"

// Main data for downloading from IMDB
const (
	MainURL             = "https://datasets.imdbws.com"
	NameFile            = "name.basics.tsv.gz"
	TitleAkasFile       = "title.akas.tsv.gz"
	TitleBasicsFile     = "title.basics.tsv.gz"
	TitleCrewFile       = "title.crew.tsv.gz"
	TitleEpisodeFile    = "title.episode.tsv.gz"
	TitlePrincipalsFile = "title.principals.tsv.gz"
	TileRatingsFile     = "title.ratings.tsv.gz"
)

func main() {
	fmt.Printf("Hello, world.\n")
	lib.DownloadFile("https://wordpress.org/wordpress-4.4.2.zip", "./")
}
