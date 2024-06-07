package main

import (
	"fmt"
	"log"
	"os"

	"github.com/buildbarn/bb-playground/pkg/bazelclient/arguments"
)

func main() {
	cmd, err := arguments.Parse(os.Args[1:])
	if err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}

	log.Printf("%#v", cmd)
}
