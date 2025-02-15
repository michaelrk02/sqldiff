package internal

import (
	"encoding/json"
	"os"
)

type Config struct {
	Connections map[string]ConnectionProperties `json:"connections"`
}

func LoadConfig() (*Config, error) {
	f, err := os.Open("config.json")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	err = json.NewDecoder(f).Decode(&cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
