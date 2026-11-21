package main

import (
	"fmt"

	_ "github.com/x/remote"

	"example.com/fx/app/internal/core"
	"example.com/fx/lib/tagged"
)

func main() { fmt.Println(core.Name, tagged.Name) }
