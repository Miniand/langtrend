package main

import (
	"log"
	"os"
	"strconv"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/web"
	"github.com/Miniand/langtrend/web/options"
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
	port, _ := strconv.Atoi(os.Getenv("PORT"))
	if err := web.New(options.Options{
		Db:   session,
		Addr: os.Getenv("ADDRESS"),
		Port: port,
	}).Run(); err != nil {
		log.Fatal(err)
	}
}
