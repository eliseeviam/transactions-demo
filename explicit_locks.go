package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"sync"
	"time"
)

func (s *Suite) ExplicitLocksBad() {

	fmt.Println("============ start ExplicitLocksBad =========")
	defer fmt.Println("=============================================")

	_, err := s.pool.Exec(ctx(),
		"INSERT INTO on_call (name, on_call, shift) VALUES ('bob', true, 'first'), ('alice', true, 'first')")
	if err != nil {
		panic(err)
	}

	takeVacation := func(wg *sync.WaitGroup, name string) {
		defer wg.Done()

		tx := mustNewTX(s.pool, pgx.ReadCommitted)

		var count int64
		err := tx.QueryRow(ctx(), "SELECT COUNT(*) FROM on_call WHERE on_call IS TRUE").Scan(&count)
		if err != nil {
			panic(err)
		}

		fmt.Println("on call: ", count, time.Now())
		time.Sleep(time.Millisecond * 500)

		if count < 2 {
			fmt.Println(name + " wasn't hurry enough")
			_ = tx.Rollback(ctx())
			return
		}

		fmt.Println(name + " took vacation!")

		_, err = tx.Exec(ctx(), "UPDATE on_call SET on_call = false WHERE name = $1", name)
		if err != nil {
			panic(err)
		}

		tx.Commit(ctx())
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go takeVacation(wg, "bob")
	go takeVacation(wg, "alice")

	wg.Wait()

	rows, err := s.pool.Query(ctx(), "SELECT name, on_call FROM on_call WHERE shift = 'first'")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		var (
			name   string
			onCall bool
		)
		err = rows.Scan(&name, &onCall)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s is on call: %t\n", name, onCall)
	}

}

func (s *Suite) ExplicitLocksGood() {

	fmt.Println("=========== start ExplicitLocksGood =========")
	defer fmt.Println("=============================================")

	_, err := s.pool.Exec(ctx(), "INSERT INTO on_call (name, on_call, shift) VALUES "+
		"('bob', true, 'first'), ('alice', true, 'first'), ('sam', true, 'second')")
	if err != nil {
		panic(err)
	}

	takeVacation := func(wg *sync.WaitGroup, name string) {
		defer wg.Done()

		tx := mustNewTX(s.pool, pgx.ReadCommitted)

		rows, err := tx.Query(ctx(), "SELECT name FROM on_call WHERE on_call IS TRUE AND shift = 'first' FOR UPDATE")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		var count int
		for rows.Next() {
			err = rows.Scan(new(string))
			if err != nil {
				panic(err)
			}
			count++
		}

		fmt.Println("on call: ", count, time.Now())
		time.Sleep(time.Millisecond * 500)

		if count < 2 {
			fmt.Println(name + " wasn't fast enough")
			_ = tx.Rollback(ctx())
			return
		}

		fmt.Println(name + " took vacation!")

		_, err = tx.Exec(ctx(), "UPDATE on_call SET on_call = false WHERE name = $1 AND shift = 'first'", name)
		if err != nil {
			panic(err)
		}

		tx.Commit(ctx())
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go takeVacation(wg, "bob")
	go takeVacation(wg, "alice")

	wg.Wait()

	rows, err := s.pool.Query(ctx(), "SELECT name, on_call FROM on_call WHERE shift = 'first'")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name   string
			onCall bool
		)
		err = rows.Scan(&name, &onCall)
		if err != nil {
			panic(err)
		}

		fmt.Printf("%s is on call: %t\n", name, onCall)
	}

}

