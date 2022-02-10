package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	user     = "root"
	password = "088fc01375fe2b689db8a872912392c6"
	host     = "localhost"
	port     = "15432"
	dbName   = "test_db"
)

var (
	url = fmt.Sprintf("postgresql://%s:%s@%s:%v/%s", user, password, host, port, dbName)
)

func main() {
	pool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		panic("connection failed: " + err.Error())
	}
	err = pool.Ping(context.Background())
	if err != nil {
		panic("ping failed: " + err.Error())
	}
	fmt.Println("connected!")

	s := &Suite{pool: pool}

	s.Run()
}
