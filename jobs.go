package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"math/rand"
	"sync"
	"time"
)

func (s *Suite) Jobs() {

	fmt.Println("================== start Jobs ===============")
	defer fmt.Println("=============================================")

	const number = 10
	for i := 0; i < number; i++ {
		_, err := s.pool.Exec(ctx(),
			"INSERT INTO jobs (name, description) VALUES ($1, $2)",
			fmt.Sprintf("job_%05d", i + 1), fmt.Sprintf("job_%05d description", i + 1))
		if err != nil {
			panic(err)
		}
	}

	executeOneJob := func(db *pgxpool.Pool) {
		var name string
		tx := mustNewTX(db, pgx.ReadCommitted)
		rows, err := tx.Query(ctx(), "SELECT name FROM jobs WHERE done IS FALSE " +
			"FOR UPDATE SKIP LOCKED LIMIT 1")
		if err != nil {
				panic(err)
			}
		defer rows.Close()
		for rows.Next() {
			err = rows.Scan(&name)
			if err != nil {
				panic(err)
			}
		}

		if name == "" {
			fmt.Println("jobs no more")
			tx.Rollback(ctx())
			return
		}

		fmt.Println("job", name, "started")

		time.Sleep(time.Second * time.Duration(rand.Int63n(5)))

		_, err = tx.Exec(ctx(), "UPDATE jobs SET done = true WHERE name = $1", name)
		if err != nil {
			panic(err)
		}
		tx.Commit(ctx())
		fmt.Println("job", name, "done")
	}

	wg := sync.WaitGroup{}
	wg.Add(number + 1)
	for i:=0; i<number + 1; i++ {
		go func() {
			executeOneJob(s.pool)
			wg.Done()
		}()
	}
	wg.Wait()

}
