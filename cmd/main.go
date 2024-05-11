package main

import (
	"github.com/nulldiego/lingua/internal/api"
	"github.com/nulldiego/lingua/migrations"
	"gofr.dev/pkg/gofr"
)

func main() {
	// initialise gofr object
	app := gofr.New()

	// run migrations
	app.Migrate(migrations.All())

	api.RegisterRoutes(app)

	// Runs the server, it will listen on the default port 8000.
	// it can be over-ridden through configs
	app.Run()
}
