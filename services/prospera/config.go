package main

import "github.com/azylman/aerial/services/prospera/internal/config"

const DefaultConfigPath = config.DefaultConfigPath

type Config = config.Config

type ConfigSource = config.ConfigSource

type ProsperaYAMLConfig = config.ProsperaYAMLConfig

var (
	ResolveConfigPath = config.ResolveConfigPath
	NormalizeE164     = config.NormalizeE164
	LoadConfig        = config.LoadConfig
	LoadConfigFile    = config.LoadConfigFile
)
