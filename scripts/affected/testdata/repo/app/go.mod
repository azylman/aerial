module example.com/fx/app

go 1.27.3

require example.com/fx/lib v0.0.0

replace (
	example.com/fx/lib v0.0.0 => ../lib // local checkout
	github.com/x/remote => github.com/x/remote v1.0.0
)
