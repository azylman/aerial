module github.com/azylman/aerial/services/synapse

go 1.27.3

require (
	github.com/azylman/aerial/pkg v0.0.0
	github.com/bwmarrin/discordgo v0.29.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/prometheus/client_golang v1.25.0
	github.com/riverqueue/river v0.50.0
	github.com/riverqueue/river/riverdriver/riverpgxv5 v0.50.0
	github.com/riverqueue/river/rivertype v0.50.0
	go.uber.org/goleak v1.3.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	github.com/riverqueue/river/riverdriver v0.50.0 // indirect
	github.com/riverqueue/river/rivershared v0.50.0 // indirect
	github.com/rogpeppe/go-internal v1.16.0 // indirect
	github.com/tidwall/gjson v1.20.0 // indirect
	github.com/tidwall/match v1.2.0 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
	golang.org/x/crypto v0.0.0-20210421170649-83a5a9bb288b // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/azylman/aerial/pkg => ../../pkg
