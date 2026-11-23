package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "tool" {
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "covdata" {
		args = args[1:]
	}

	covdataBin := os.Getenv("COVDATA_BIN")
	if covdataBin == "" {
		covdataBin = "/usr/local/bin/covdata"
		if _, err := os.Stat(covdataBin); err != nil {
			if selfExe, exeErr := os.Executable(); exeErr == nil {
				sibling := filepath.Join(filepath.Dir(selfExe), "covdata")
				if _, statErr := os.Stat(sibling); statErr == nil {
					covdataBin = sibling
				}
			}
		}
	}
	execArgs := append([]string{covdataBin}, args...)
	if err := syscall.Exec(covdataBin, execArgs, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to exec %s: %v\n", covdataBin, err)
		os.Exit(1)
	}
}
