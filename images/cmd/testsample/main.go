package main

import (
	"fmt"
	"os"
	"runtime/coverage"
)

func Add(a, b int) int {
	return a + b
}

func main() {
	res := Add(2, 3)
	fmt.Printf("result: %d\n", res)
	if dir := os.Getenv("FLUSH_DIR"); dir != "" {
		if err := coverage.WriteMetaDir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "WriteMetaDir: %v\n", err)
			os.Exit(1)
		}
		if err := coverage.WriteCountersDir(dir); err != nil {
			fmt.Fprintf(os.Stderr, "WriteCountersDir: %v\n", err)
			os.Exit(1)
		}
	}
}
