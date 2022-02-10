package main

import (
	"fmt"
	"github.com/jackc/pgx/v4"
	"sync"
	"sync/atomic"
	"time"
)

func (s *Suite) SwapWithCompare() {

	fmt.Println("============ start SwapWithCompare ==========")
	defer fmt.Println("=============================================")

	_, err := s.pool.Exec(ctx(), "INSERT INTO coupons (name) VALUES ('awesome_coupon')")
	if err != nil {
		panic(err)
	}

	var appliedTimes int64
	applyCoupon := func(wg *sync.WaitGroup, name string) {
		defer wg.Done()

		tx := mustNewTX(s.pool, pgx.ReadCommitted)

		tag, err := tx.Exec(ctx(), "UPDATE coupons SET applied = true WHERE name = $1 AND applied = false", name)
		if err != nil {
			panic(err)
		}
		if tag.RowsAffected() == 0 {
			fmt.Println("wasn't updated\t", time.Now())
			_ = tx.Rollback(ctx())
			return
		}
		fmt.Println("was updated\t", time.Now())
		atomic.AddInt64(&appliedTimes, 1)

		_, err = tx.Exec(ctx(), "UPDATE wallets SET amount =  amount + $1 WHERE name = $2", 200, "john_wallet")
		if err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
		tx.Commit(ctx())
	}

	const num = 10
	wg := &sync.WaitGroup{}
	wg.Add(num)

	for i := 0; i < num; i++ {
		go applyCoupon(wg, "awesome_coupon")
	}

	wg.Wait()

	fmt.Printf("coupon was applied %v time(s)\n", appliedTimes)

}
