package lib

import (
	"log"
	"strings"

	"github.com/jinzhu/gorm"
	"github.com/raj/imdb-dataset-importer/models"
)

type (
	// MainFactory represents the maucine factory
	MainFactory struct {
		db *gorm.DB
	}
	// JSONView represents
	JSONView struct {
		Titles []models.TitleBasic `json:"titles"`
	}
)

// NewMainFactory : the main factory contructor
func NewMainFactory(db *gorm.DB) *MainFactory {
	return &MainFactory{db}
}

// GetMain return JSONView to send on internet
func (mc MainFactory) GetMain() JSONView {

	var basics []models.TitleBasic

	var rawSQL = `select * from name_basics limit 1`

	rows, err := mc.db.Raw(rawSQL).Rows() // (*sql.Rows, error)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	uj := JSONView{Titles: basics}

	return uj

}

// SearchForTitle return JSONView to send on internet
func (mc MainFactory) SearchForTitle(query string) JSONView {

	var basics []models.TitleBasic
	qu := strings.Replace(query, " ", "_", -1)
	qu = strings.Replace(qu, ":", "_", -1)

	var rawSQL = `
			select distinct(tconst),title_type,primary_title,original_title,is_adult,start_year,end_year,runtime_minutes,genres
			from public.title_basics where tconst in (
		 	SELECT distinct(title_id) FROM public.title_akas where to_tsvector('search_conf',title) @@ to_tsquery('search_conf', ?)
			) order by start_year desc`

	rows, err := mc.db.Raw(rawSQL, qu).Rows() // (*sql.Rows, error)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var basic models.TitleBasic
		mc.db.ScanRows(rows, &basic)
		basics = append(basics, basic)
	}

	uj := JSONView{Titles: basics}

	return uj

}
