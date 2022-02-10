package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

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
