package models

type TomlConfig struct {
	Imdb       imdbInfo     `toml:"imdb"`
	DB         databaseInfo `toml:"database"`
	ServerInfo serverInfo   `toml:"server"`
}

type databaseInfo struct {
	Server   string `toml:"server"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	DbName   string `toml:"dbname"`
	SslMode  string `toml:"sslmode"`
}

type imdbInfo struct {
	BaseURL string
	Files   []string
	// NameBasicsFile      string
	// TitleAkasFile       string
	// TitleBasicsFile     string
	// TitleCrewFile       string
	// TitleEpisodeFile    string
	// TitlePrincipalsFile string
	// TitleRatingsFile    string
}

type serverInfo struct {
	Bind string
	Port int
}
