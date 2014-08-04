package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/github"
	"github.com/Miniand/langtrend/worker"
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
	if err := session.Migrate(); err != nil {
		log.Fatal(err)
	}
	// Run worker.
	rate, _ := strconv.Atoi(os.Getenv("PLA_RATE"))
	earliest, _ := time.Parse(github.DateFormat, os.Getenv("PLA_EARLIEST"))
	worker.New(worker.Options{
		Db:       session,
		Rate:     time.Duration(rate),
		Username: os.Getenv("PLA_GITHUB_USERNAME"),
		Password: os.Getenv("PLA_GITHUB_PASSWORD"),
		Earliest: earliest,
	}).Run()
}
