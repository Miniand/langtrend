package main

import (
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/github"
	"github.com/Miniand/langtrend/web"
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
	// Run web and worker in routines and wait on each.
	wg := sync.WaitGroup{}
	// Run web.
	wg.Add(1)
	go func() {
		if err := web.New(web.Options{
			Db:   session,
			Addr: ":9999",
		}).Run(); err != nil {
			log.Fatal(err)
		}
		wg.Done()
	}()
	wg.Add(1)
	// Run worker.
	go func() {
		rate, _ := strconv.Atoi(os.Getenv("PLA_RATE"))
		earliest, _ := time.Parse(github.DateFormat, os.Getenv("PLA_EARLIEST"))
		worker.New(worker.Options{
			Db:       session,
			Rate:     time.Duration(rate),
			Username: os.Getenv("PLA_GITHUB_USERNAME"),
			Password: os.Getenv("PLA_GITHUB_PASSWORD"),
			Earliest: earliest,
		}).Run()
		wg.Done()
	}()
	// Wait for routines to finish.
	wg.Wait()
}
