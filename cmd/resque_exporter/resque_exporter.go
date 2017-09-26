package main

import (
	"os"

	"github.com/catawiki/resque_exporter"
)

func main() {
	resqueExporter.Run(os.Args[1:])
}
