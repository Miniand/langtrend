package main

import (
	"log"
	"os"
	"time"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/github"
	"github.com/Miniand/langtrend/worker"
)

func main() {
	// Connect to database.
	session, err := db.Connect(db.Options{
		Database: os.Getenv("DATABASE"),
		Address:  os.Getenv("RETHINKDB_ADDRESS"),
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := session.Migrate(); err != nil {
		log.Fatal(err)
	}
	// Run worker.
	earliest, _ := time.Parse(github.DateFormat, os.Getenv("EARLIEST"))
	worker.New(worker.Options{
		Db:       session,
		Username: os.Getenv("GITHUB_USERNAME"),
		Password: os.Getenv("GITHUB_PASSWORD"),
		Earliest: earliest,
	}).Run()
}
