package lib

import (
	"log"

	"github.com/jinzhu/gorm"
)

type (
	// MainFactory represents the maucine factory
	MainFactory struct {
		db *gorm.DB
	}
	// JSONView represents
	JSONView struct {
		Names []string `json:"names"`
	}
)

// NewMainFactory : the main factory contructor
func NewMainFactory(db *gorm.DB) *MainFactory {
	return &MainFactory{db}
}

// GetMain return JSONView to send on internet
func (mc MainFactory) GetMain() JSONView {

	var names []string

	var rawSQL = `select * from name_basics limit 1`

	rows, err := mc.db.Raw(rawSQL).Rows() // (*sql.Rows, error)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	uj := JSONView{Names: names}

	return uj

}
