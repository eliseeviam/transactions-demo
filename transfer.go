package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func consistentTransfer(db *pgxpool.Pool, level pgx.TxIsoLevel) {

	_, err := db.Exec(ctx(), "INSERT INTO wallets (name, amount) VALUES ($1, 100)", "wallet1")
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(ctx(), "INSERT INTO wallets (name, amount) VALUES ($1, 100)", "wallet2")
	if err != nil {
		panic(err)
	}


	readingTx := mustNewTX(db, level)
	var amountWallet1 int64
	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "wallet1").Scan(&amountWallet1)
	fmt.Println("wallet1 amount:", amountWallet1)

	{
		writingTx := mustNewTX(db, level)
		_, err = writingTx.Exec(ctx(), "UPDATE wallets SET amount = amount + 50 WHERE name = $1", "wallet1")
		if err != nil {
			panic(err)
		}
		_, err = writingTx.Exec(ctx(), "UPDATE wallets SET amount = amount - 50 WHERE name = $1", "wallet2")
		if err != nil {
			panic(err)
		}
		writingTx.Commit(ctx())
	}

	var amountWallet2 int64
	_ = readingTx.QueryRow(ctx(), "SELECT amount FROM wallets WHERE name = $1", "wallet2").Scan(&amountWallet2)
	fmt.Println("wallet2 amount:", amountWallet2)

	readingTx.Commit(ctx())
}

func (s *Suite) ConsistentTransferFail() {
	fmt.Println("======== start ConsistentTransferFail =======")
	defer fmt.Println("=============================================")

	consistentTransfer(s.pool, pgx.ReadCommitted)
}

func (s *Suite) ConsistentTransferGood() {

	fmt.Println("======== start ConsistentSnapshotGood =======")
	defer fmt.Println("=============================================")

	consistentTransfer(s.pool, pgx.RepeatableRead)
}
