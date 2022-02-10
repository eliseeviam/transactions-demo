package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func consistentSnapshot(db *pgxpool.Pool, level pgx.TxIsoLevel) {

	const number = 100
	for i := 0; i < number; i++ {
		_, err := db.Exec(ctx(), "INSERT INTO wallets (name, amount) VALUES ($1, 100)", fmt.Sprintf("wallet_%05d", i+1))
		if err != nil {
			panic(err)
		}
	}

	readingTx := mustNewTX(db, level)

	writingTx := mustNewTX(db, level)
	_, err := writingTx.Exec(ctx(), "UPDATE wallets SET amount = amount - 50 WHERE name = $1",
		fmt.Sprintf("wallet_%05d", 1))
	if err != nil {
		panic(err)
	}

	var (
		snapshotTotal int64
		skip          int64
		updated       bool
	)

	for skip < number {
		rows, err := readingTx.Query(ctx(), "SELECT name, amount FROM wallets " +
			"ORDER BY name OFFSET $1 LIMIT 10", skip)
		if err != nil {
			panic(err)
		}

		for rows.Next() {
			var (
				name string
				a int64
			)
			err = rows.Scan(&name, &a)
			switch name {
			case "wallet_00001", "wallet_00100":
				fmt.Println(name, ": ", a)
			}
			if err != nil {
				panic(err)
			}
			snapshotTotal += a
			skip++
		}

		if !updated {
			updated = true
			_, err = writingTx.Exec(ctx(),
				"UPDATE wallets SET amount = amount + 50 WHERE name = $1",
				fmt.Sprintf("wallet_%05d", number))
			if err != nil {
				panic(err)
			}
			writingTx.Commit(ctx())
			fmt.Println("write tx commited")
		}
	}

	readingTx.Commit(ctx())

	var total int64
	err = db.QueryRow(ctx(), "SELECT SUM(amount) FROM wallets").Scan(&total)
	if err != nil {
		panic(err)
	}

	fmt.Println("snapshotTotal:", snapshotTotal)
	fmt.Println("total:", total)
}

func (s *Suite) ConsistentSnapshotFail() {
	fmt.Println("======== start ConsistentSnapshotFail =======")
	defer fmt.Println("=============================================")

	consistentSnapshot(s.pool, pgx.ReadCommitted)
}

func (s *Suite) ConsistentSnapshotGood() {

	fmt.Println("======== start ConsistentSnapshotGood =======")
	defer fmt.Println("=============================================")

	consistentSnapshot(s.pool, pgx.RepeatableRead)
}
