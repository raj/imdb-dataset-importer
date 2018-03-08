# Welcome to Imdb Dataset import!


this go application will download **IMDB** dataset and import it in a postgreSQL database
it provide also a simple to search a imdb_id from a title or name



## Installation

clone the repository

```shell
go get github.com\raj\imdb-dataset-importer
```
     


# Command line usage

get the command line help message
```shell
cd %GOPATH%/src/github.com/raj/imdb-dataset-importer
imdb-dataset-importer.exe --help
```

result must be :


```shell
Import IMDB dataset.
Usage of imdb-dataset-importer.exe:
  -api
        provide api.
  -d    download all files from aws dataset.
  -i    import files to database.
  -s    search.
```

## 1. download dataset

```shell
cd %GOPATH%/src/github.com/raj/imdb-dataset-importer
go run imdb-dataset.go -d
 ```

result must be :


```shell
    
Import IMDB dataset.
2018/03/08 18:35:41 downloadAction
https://datasets.imdbws.com/title.basics.tsv.gz 62.3MiB / 84.0MiB [=========================================================>--------------------]    3ss
https://datasets.imdbws.com/title.ratings.tsv.gz   3.8MiB / 3.8MiB [==============================================================================]    0ss
https://datasets.imdbws.com/name.basics.tsv.gz15.0MiB / 158.6MiB [======>-----------------------------------------------------------------------] 1m23sss
https://datasets.imdbws.com/title.akas.tsv.gz 11.8MiB / 50.0MiB [=================>------------------------------------------------------------]   12ss
https://datasets.imdbws.com/title.episode.tsv.gz 12.5MiB / 16.9MiB [=========================================================>--------------------]    0ss
https://datasets.imdbws.com/title.crew.tsv.gz 12.4MiB / 34.3MiB [===========================>--------------------------------------------------]   14ss
https://datasets.imdbws.com/title.principals.tsv.gz10.5MiB / 230.7MiB [===>--------------------------------------------------------------------------]   32sss
```

  

## 2. import each files to database

```shell
go run imdb-dataset.go -i
 ```   

# Server API start

```shell
go run imdb-dataset.go -api
 ``` 

## search_for_title/:title

just replace with your 
```shell
curl -X GET http://localhost:3000/search_for_title/compartiment+tueurs
```

 
  response must be :


```json
{
"titles":
	[
		{
			"tconst":"tt0059050",
			"title_type":"movie",
			"primary_title":"Compartiment tueurs",
			"original_title":"Compartiment tueurs",
			"is_adult":"0",
			"start_year":"1965",
			"end_year":"N",
			"runtime_minutes":"95",
			"genres":"Drama,Mystery,Thriller"
		}
	]
}


```

 