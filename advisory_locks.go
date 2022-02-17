package main

import (
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"math/rand"
	"sync"
	"time"
)

func addAccount(db *pgxpool.Pool, level pgx.TxIsoLevel) bool {

	tx := mustNewTX(db, level)

	var count int64
	err := tx.QueryRow(ctx(), "SELECT COUNT(*) FROM accounts WHERE owner = $1", 1).Scan(&count)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 500)

	if count >= 3 {
		_ = tx.Rollback(ctx())
		return true
	}

	_, err = tx.Exec(ctx(), "INSERT INTO accounts (owner, name) VALUES ($1, $2)",
		1, fmt.Sprintf("account_%d", rand.Int()))
	if e, ok := err.(*pgconn.PgError); ok {
		if e.Code == "40001" {
			tx.Rollback(ctx())
			return false
		}
	}
	if err != nil {
		panic(err)
	}
	err = tx.Commit(ctx())
	return err == nil
}

func (s *Suite) AccountLimitBad() {

	fmt.Println("============ start AccountLimitBad =========")
	defer fmt.Println("=============================================")

	const (
		number = 10
	)
	wg := &sync.WaitGroup{}
	wg.Add(number)
	for i := 0; i < number; i++ {
		go func() {
			addAccount(s.pool, pgx.ReadCommitted)
			wg.Done()
		}()
	}

	wg.Wait()

	var count int64
	err := s.pool.QueryRow(ctx(), "SELECT COUNT(*) FROM accounts WHERE owner = $1", 1).Scan(&count)
	if err != nil {
		panic(err)
	}

	fmt.Println("account count:", count)
}

func (s *Suite) AccountLimitOkay() {

	fmt.Println("=========== start AccountLimitOkay =========")
	defer fmt.Println("=============================================")

	const (
		number = 10
	)
	wg := &sync.WaitGroup{}
	for i := 0; i < number; i++ {
		wg.Add(1)
		go func() {
			for !addAccount(s.pool, pgx.Serializable) {
				// need to retry tx
			}
			wg.Done()
		}()
	}

	wg.Wait()

	var count int64
	err := s.pool.QueryRow(ctx(), "SELECT COUNT(*) FROM accounts WHERE owner = $1", 1).Scan(&count)
	if err != nil {
		panic(err)
	}

	fmt.Println("account count:", count)
}

func accountLimitAdvisoryLocking(wg *sync.WaitGroup, db *pgxpool.Pool, level pgx.TxIsoLevel, thread int) {
	defer wg.Done()

	prefix := fmt.Sprintf("thread: %v: ", thread)
	tx := mustNewTX(db, level)

	_, err := tx.Exec(ctx(), "SELECT pg_advisory_xact_lock($1)", 1)
	if err != nil {
		panic(err)
	}

	fmt.Println(prefix + "took lock")

	var count int64
	err = tx.QueryRow(ctx(), "SELECT COUNT(*) FROM accounts WHERE owner = $1", 1).Scan(&count)
	if err != nil {
		panic(err)
	}

	time.Sleep(time.Millisecond * 500)

	fmt.Println(prefix+"count:", count)
	if count >= 3 {
		fmt.Println(prefix + "released lock")
		_ = tx.Rollback(ctx())
		return
	}

	_, err = tx.Exec(ctx(), "INSERT INTO accounts (owner, name) VALUES ($1, $2)",
		1, fmt.Sprintf("account_%d", rand.Int()))
	if err != nil {
		panic(err)
	}

	fmt.Println(prefix + "released lock")
	tx.Commit(ctx())
}

func (s *Suite) AccountLimitGood() {

	fmt.Println("=========== start AccountLimitGood =========")
	defer fmt.Println("=============================================")

	const (
		number = 10
	)
	wg := &sync.WaitGroup{}
	wg.Add(number)
	for i := 0; i < number; i++ {
		go accountLimitAdvisoryLocking(wg, s.pool, pgx.ReadCommitted, i)
	}

	wg.Wait()

	var count int64
	err := s.pool.QueryRow(ctx(), "SELECT COUNT(*) FROM accounts WHERE owner = $1", 1).Scan(&count)
	if err != nil {
		panic(err)
	}

	fmt.Println("account count:", count)
}
