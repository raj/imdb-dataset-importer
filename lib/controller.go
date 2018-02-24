package lib

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/jinzhu/gorm"
	"github.com/julienschmidt/httprouter"
)

type (
	// MainController represents the controller for operating on the Main
	MainController struct {
		db *gorm.DB
	}
)

// NewMainController : Comment for
func NewMainController(db *gorm.DB) *MainController {
	return &MainController{db}
}

// GetMain main data
func (mc MainController) GetMain(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	uc := NewMainFactory(mc.db)

	uj, _ := json.Marshal(uc.GetMain())

	// Write content-type, statuscode, payload
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	fmt.Fprintf(w, "%s", uj)
}

// SearchForTitle main data
func (mc MainController) SearchForTitle(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	uc := NewMainFactory(mc.db)
	query := p.ByName("query")
	uj, _ := json.Marshal(uc.SearchForTitle(query))

	// Write content-type, statuscode, payload
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	fmt.Fprintf(w, "%s", uj)
}
