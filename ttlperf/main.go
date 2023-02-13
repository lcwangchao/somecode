// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

func startInsertTask(ctx context.Context, db *sql.DB, ch <-chan any, counter *atomic.Int64) {
	conn, err := db.Conn(context.TODO())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	var sb strings.Builder
	for i := 0; i < *recordSize; i++ {
		sb.WriteByte('A' + byte(i%('z'-'A')))
	}
	blob := sb.String()

	stmt, err := conn.PrepareContext(context.TODO(), "INSERT INTO Serve(UUID, ServeItemId, Timestamp, `Blob`, ttl) VALUES (?,?,?,?,?)")
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return

		case _, ok := <-ch:
			if !ok {
				return
			}

			bs := [16]byte(uuid.New())
			md5sum := md5.Sum(bs[:])
			uid := string(md5sum[:])
			itemID := uid
			tm := time.Now().Add(offsetDuration)
			ts := tm.Unix()
			dt := tm.Format("2006-01-02 15:04:05")

			_, err = stmt.ExecContext(ctx, uid, itemID, ts, blob, dt)
			if err != nil {
				log.L().Error(err.Error(), zap.Error(err))
				continue
			}

			counter.Add(1)
		}
	}
}

func startDispatchToken(ctx context.Context, ch chan<- interface{}, limiter *rate.Limiter) {
	left := 0
	for {
		if left == 0 {
			err := limiter.WaitN(ctx, 10)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				panic(err)
			}
			left = 10
		}

		left--
		select {
		case <-ctx.Done():
			return
		case ch <- struct{}{}:
		}
	}
}

var (
	host           = flag.String("host", "127.0.0.1", "db host")
	port           = flag.Int("port", 4000, "db port")
	user           = flag.String("user", "root", "db user")
	db             = flag.String("database", "test", "use database")
	qps            = flag.Float64("qps", 100*1000, "the qps limit of the insert")
	threads        = flag.Int("threads", 10, "the thread count")
	offset         = flag.String("offset", "0h", "the offset of ttl time")
	recordSize     = flag.Int("recordSize", 512, "size of the record")
	offsetDuration = time.Duration(0)
)

func main() {
	flag.Parse()

	var err error
	offsetDuration, err = time.ParseDuration(*offset)
	if err != nil {
		panic(err)
	}

	fmt.Printf("start insert data, host: %s, port: %d, db: %s, qps: %.0f, threads: %d, offset: %v, recordSize: %d\n", *host, *port, *db, *qps, *threads, offsetDuration, *recordSize)
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	dsn := fmt.Sprintf("%s@tcp(%s:%d)/%s", *user, *host, *port, *db)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	limitQPS := rate.Limit(*qps)
	counters := make([]*atomic.Int64, *threads)
	counts := make([]int64, *threads)
	limiter := rate.NewLimiter(limitQPS, 100)
	ch := make(chan any, 100)

	for i := range counters {
		counters[i] = &atomic.Int64{}
		go startInsertTask(ctx, db, ch, counters[i])
	}

	startTime := time.Now()
	go startDispatchToken(ctx, ch, limiter)
	ticker := time.Tick(10 * time.Second)

	incs := make([]int64, len(counters))
	for {
		select {
		case <-ticker:
			var totalInc int64
			for i, counter := range counters {
				count := counts[i]
				newCount := counter.Load()
				counts[i] = newCount
				inc := newCount - count
				totalInc += inc
				incs[i] = inc
			}

			interval := time.Since(startTime)
			startTime = time.Now()
			for i, inc := range incs {
				threadQPS := float64(inc) / interval.Seconds()
				fmt.Printf("Thread [%02d] QPS: %.0f\n", i, threadQPS)
			}

			totalQPS := float64(totalInc) / interval.Seconds()
			fmt.Printf("Total QPS: %.0f\n\n", totalQPS)
		}
	}
}
