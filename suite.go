package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
	"time"
)

func ctx() context.Context {
	return context.Background()
}

func mustNewTX(db *pgxpool.Pool, isolationLevel pgx.TxIsoLevel) pgx.Tx {
	tx, err := db.BeginTx(ctx(), pgx.TxOptions{
		IsoLevel: isolationLevel,
	})
	if err != nil {
		panic(err)
	}
	return tx
}

type Suite struct {
	pool *pgxpool.Pool
}

func (s *Suite) Run() {
	fs := []func(){
		//s.ImpossibleDirtyRead,
		//s.NonRepeatableReadFail,
		//s.NonRepeatableReadGood,
		s.ExplicitLocksBad,
		s.ExplicitLocksGood,
	}

	for _, f := range fs {
		s.clean()
		f()
	}
}

func (s *Suite) clean() {
	ctx, cancel := context.WithTimeout(ctx(), time.Second*10)
	defer cancel()
	_, err := s.pool.Exec(ctx, "TRUNCATE wallets")
	if err != nil {
		panic(err)
	}
	_, err = s.pool.Exec(ctx, "TRUNCATE on_call")
	if err != nil {
		panic(err)
	}
}

func (s *Suite) ImpossibleDirtyRead() {

	fmt.Println("========= start ImpossibleDirtyRead =========")
	defer fmt.Println("=============================================")

	/*
		In PostgreSQL, you can request any of the four standard transaction isolation levels, but internally only three distinct isolation levels are implemented, i.e., PostgreSQL's Read Uncommitted mode behaves like Read Committed. This is because it is the only sensible way to map the standard isolation levels to PostgreSQL's multiversion concurrency control architecture.
	*/

	tag, err := s.pool.Exec(ctx(), "INSERT INTO wallets (name, amount) VALUES ('john_wallet', 100)")
	if tag.RowsAffected() != 1 {
		panic("")
	}
	if err != nil {
		panic(err)
	}

	level := pgx.ReadUncommitted
	writingTx := mustNewTX(s.pool, level)
	readingTx := mustNewTX(s.pool, level)
	fmt.Println("read and write tx begins")

	var amount int64
	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read first time: ", amount)

	_, _ = writingTx.Exec(ctx(), "UPDATE wallets SET amount = $1 WHERE name = $2", 200, "john_wallet")
	fmt.Println("w amount changed to 200")

	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read second time: ", amount)

	_ = writingTx.Rollback(ctx())
	fmt.Println("writing tx rollbacks")

	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read after rollback: ", amount)

	_ = readingTx.Commit(ctx()) // no auto rollback
	fmt.Println("reading tx commits")
}

func testNonRepeatableRead(db *pgxpool.Pool, level pgx.TxIsoLevel) {
	tag, err := db.Exec(ctx(), "INSERT INTO wallets (name, amount) VALUES ('john_wallet', 100)")
	if tag.RowsAffected() != 1 {
		panic("")
	}
	if err != nil {
		panic(err)
	}

	writingTx := mustNewTX(db, level)
	readingTx := mustNewTX(db, level)
	fmt.Println("read and write tx begins")

	var amount int64
	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read first time: ", amount)

	_, _ = writingTx.Exec(ctx(), "UPDATE wallets SET amount = $1 WHERE name = $2", 200, "john_wallet")
	fmt.Println("w amount changed to 200")

	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read second time: ", amount)

	_ = writingTx.Commit(ctx())
	fmt.Println("writing tx commits")

	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "john_wallet").Scan(&amount)
	fmt.Println("r amount read after writing tx commit: ", amount)

	_ = readingTx.Commit(ctx())
	fmt.Println("reading tx commits")
}

func (s *Suite) NonRepeatableReadFail() {
	fmt.Println("======== start NonRepeatableReadFail ========")
	defer fmt.Println("=============================================")

	testNonRepeatableRead(s.pool, pgx.ReadCommitted)
}

func (s *Suite) NonRepeatableReadGood() {

	fmt.Println("======== start NonRepeatableReadGood ========")
	defer fmt.Println("=============================================")

	testNonRepeatableRead(s.pool, pgx.RepeatableRead)
}

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
		time.Sleep(time.Second)

		if count < 2 {
			_ = tx.Rollback(ctx())
			return
		}

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

	_, err := s.pool.Exec(ctx(), "INSERT INTO on_call (name, on_call, shift) VALUES ('bob', true, 'first'), ('alice', true, 'first'), ('sam', true, 'second')")
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
		time.Sleep(time.Second)

		if count < 2 {
			_ = tx.Rollback(ctx())
			return
		}

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
