package main

import (
	"log"
	"os"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/web"
	"github.com/dancannon/gorethink"
)

func main() {
	// Connect to database.
	database := os.Getenv("PLA_DATABASE")
	if database == "" {
		database = "pla"
	}
	dbAddress := os.Getenv("PLA_RETHINKDB_ADDRESS")
	if dbAddress == "" {
		dbAddress = "localhost:28015"
	}
	session, err := db.Connect(db.Options{
		Database: database,
		Rethinkdb: gorethink.ConnectOpts{
			Address:  dbAddress,
			Database: database,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := web.New(web.Options{
		Db:   session,
		Addr: ":9999",
	}).Run(); err != nil {
		log.Fatal(err)
	}
}
