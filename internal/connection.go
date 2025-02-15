package internal

import (
	"fmt"

	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type Connection struct {
	*sqlx.DB

	Name string
}

type ConnectionProperties struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
	Name string `json:"name"`
}

func NewConnection(name string, cfg ConnectionProperties) (*Connection, error) {
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@(%s:%d)/%s", cfg.User, cfg.Pass, cfg.Host, cfg.Port, cfg.Name))
	if err != nil {
		return nil, err
	}
	return &Connection{
		DB:   db,
		Name: name,
	}, nil
}
