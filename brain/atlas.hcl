variable "dev_url" {
  type    = string
  default = "docker://pgvector/pg16/dev"
}

env "local" {
  src = "file://pkg/db/schema.sql"
  dev = var.dev_url
  migration {
    dir    = "file://pkg/db/migrations"
    format = atlas
  }
  schemas = ["public"]
}
