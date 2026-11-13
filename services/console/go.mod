module github.com/azylman/aerial/services/console

go 1.27.3

require (
	github.com/azylman/aerial/pkg v0.0.0
	go.uber.org/goleak v1.3.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/azylman/aerial/pkg => ../../pkg
