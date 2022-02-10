package main

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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
		s.ImpossibleDirtyRead,
		s.NonRepeatableReadFail,
		s.NonRepeatableReadGood,
		s.ExplicitLocksBad,
		s.ExplicitLocksGood,
		s.SwapWithCompare,
		s.ConsistentTransferFail,
		s.ConsistentTransferGood,
		s.ConsistentSnapshotFail,
		s.ConsistentSnapshotGood,
		s.Jobs,
		s.AccountLimitBad,
		s.AccountLimitOkay,
		s.AccountLimitGood,
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
	_, err = s.pool.Exec(ctx, "TRUNCATE coupons")
	if err != nil {
		panic(err)
	}
	_, err = s.pool.Exec(ctx, "TRUNCATE jobs")
	if err != nil {
		panic(err)
	}
	_, err = s.pool.Exec(ctx, "TRUNCATE accounts")
	if err != nil {
		panic(err)
	}
}
