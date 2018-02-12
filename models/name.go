package models

// Name name.basics.tsv
type Name struct {
	Nconst            string `gorm:"primary_key"`
	PrimaryName       string
	BirthYear         int
	DeathYear         int `sql:"default:null"`
	PrimaryProfession string
	KnownForTitles    string
}

// nconst  primaryName     birthYear       deathYear       primaryProfession       knownForTitles
