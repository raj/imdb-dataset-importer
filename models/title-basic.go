package models

// TitleBasic movie basic info
type TitleBasic struct {
	Tconst         string `json:"tconst"`
	TitleType      string `json:"title_type"`
	PrimaryTitle   string `json:"primary_title"`
	OriginalTitle  string `json:"original_title"`
	IsAdult        string `json:"is_adult"`
	StartYear      string `json:"start_year"`
	EndYear        string `json:"end_year"`
	RuntimeMinutes string `json:"runtime_minutes"`
	Genres         string `json:"genres"`
}
