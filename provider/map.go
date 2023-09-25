package provider

import "github.com/go-spatial/tegola/internal/env"

// A Map represents a map in the Tegola Config file.
type Map struct {
	Name        env.String       `toml:"name" json:"name"`
	Attribution env.String       `toml:"attribution" json:"attribution"`
	Bounds      []env.Float      `toml:"bounds" json:"bounds"`
	Center      [3]env.Float     `toml:"center" json:"center"`
	Layers      []MapLayer       `toml:"layers" json:"layers"`
	Parameters  []QueryParameter `toml:"params" json:"params"`
	TileBuffer  *env.Int         `toml:"tile_buffer" json:"tile_buffer"`
}
