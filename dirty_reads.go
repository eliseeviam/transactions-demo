package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
)

func (s *Suite) ImpossibleDirtyRead() {

	fmt.Println("========= start ImpossibleDirtyRead =========")
	defer fmt.Println("=============================================")

	/*
	In PostgreSQL, you can request any of the four standard transaction
	isolation levels, but internally only three distinct isolation levels
	are implemented, i.e., PostgreSQL's Read Uncommitted mode behaves like
	Read Committed. This is because it is the only sensible way to map the
	standard isolation levels to PostgreSQL's multiversion concurrency
	control architecture.
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

