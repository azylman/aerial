module github.com/azylman/aerial/services/dashboard

go 1.27.1

require (
	github.com/azylman/aerial/pkg v0.0.0
	gopkg.in/yaml.v3 v3.0.1
)

replace github.com/azylman/aerial/pkg => ../../pkg
