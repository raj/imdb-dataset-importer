package lib

import (
	"log"

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
		Names []models.Name `json:"names"`
	}
)

// NewMainFactory : the main factory contructor
func NewMainFactory(db *gorm.DB) *MainFactory {
	return &MainFactory{db}
}

// GetMain return JSONView to send on internet
func (mc MainFactory) GetMain() JSONView {

	var names []models.Name

	var rawSQL = `select * from name_basics limit 1`

	rows, err := mc.db.Raw(rawSQL).Rows() // (*sql.Rows, error)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	uj := JSONView{Names: names}

	return uj

}
