// Exercises bucketdb's Redis side with go-redis v9, unchanged: the connection
// handshake (HELLO 3 refused, RESP2), strings with TTLs, a lock with SET NX,
// counters, MSET/MGET, a pipeline, MULTI/EXEC, optimistic WATCH under
// concurrency, hashes, SCAN, RENAME and expiry. Every key is under a prefix of
// its own run, deleted at the end.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func check(ok bool, format string, args ...any) {
	if !ok {
		log.Fatalf("FAIL: "+format, args...)
	}
}

func main() {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379/0"
	}
	opt, err := redis.ParseURL(url)
	must(err)
	opt.PoolSize = 16
	rdb := redis.NewClient(opt)
	ctx := context.Background()
	p := fmt.Sprintf("gotest:%d:", time.Now().UnixNano())
	k := func(s string) string { return p + s }
	defer func() {
		var keys []string
		iter := rdb.Scan(ctx, 0, p+"*", 100).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	}()

	must(rdb.Ping(ctx).Err())

	// Strings and TTLs.
	must(rdb.Set(ctx, k("session"), "alice", time.Hour).Err())
	v, err := rdb.Get(ctx, k("session")).Result()
	must(err)
	check(v == "alice", "get: %q", v)
	ttl, err := rdb.TTL(ctx, k("session")).Result()
	must(err)
	check(ttl > 59*time.Minute && ttl <= time.Hour, "ttl: %v", ttl)
	_, err = rdb.Get(ctx, k("nope")).Result()
	check(errors.Is(err, redis.Nil), "get missing: %v", err)

	// A lock: one holder.
	ok, err := rdb.SetNX(ctx, k("lock"), "a", time.Minute).Result()
	must(err)
	check(ok, "first lock")
	ok, err = rdb.SetNX(ctx, k("lock"), "b", time.Minute).Result()
	must(err)
	check(!ok, "second lock")

	// Counters.
	n, err := rdb.IncrBy(ctx, k("hits"), 5).Result()
	must(err)
	check(n == 5, "incrby: %d", n)
	f, err := rdb.IncrByFloat(ctx, k("float"), 1.5).Result()
	must(err)
	check(f == 1.5, "incrbyfloat: %v", f)

	must(rdb.MSet(ctx, k("a"), "1", k("b"), "2").Err())
	vals, err := rdb.MGet(ctx, k("a"), k("b"), k("missing")).Result()
	must(err)
	check(vals[0] == "1" && vals[1] == "2" && vals[2] == nil, "mget: %v", vals)

	// A pipeline: sent together, answered together.
	cmds, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := 0; i < 20; i++ {
			pipe.Incr(ctx, k("piped"))
		}
		return nil
	})
	must(err)
	check(cmds[19].(*redis.IntCmd).Val() == 20, "pipeline: %v", cmds[19])

	// MULTI/EXEC.
	_, err = rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, k("tx1"), "x", 0)
		pipe.Incr(ctx, k("tx2"))
		pipe.HSet(ctx, k("txh"), "f", "v")
		return nil
	})
	must(err)
	got, err := rdb.MGet(ctx, k("tx1"), k("tx2")).Result()
	must(err)
	check(got[0] == "x" && got[1] == "1", "tx: %v", got)

	// Optimistic increments: WATCH, read, MULTI, write; retried on conflict.
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				for {
					err := rdb.Watch(ctx, func(tx *redis.Tx) error {
						n, err := tx.Get(ctx, k("opt")).Int()
						if err != nil && !errors.Is(err, redis.Nil) {
							return err
						}
						_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
							pipe.Set(ctx, k("opt"), strconv.Itoa(n+1), 0)
							return nil
						})
						return err
					}, k("opt"))
					if errors.Is(err, redis.TxFailedErr) {
						continue
					}
					must(err)
					break
				}
			}
		}()
	}
	wg.Wait()
	opt80, err := rdb.Get(ctx, k("opt")).Int()
	must(err)
	check(opt80 == 80, "optimistic: %d", opt80)

	// Hashes.
	must(rdb.HSet(ctx, k("user"), map[string]any{"name": "Ana", "visits": 1}).Err())
	visits, err := rdb.HIncrBy(ctx, k("user"), "visits", 2).Result()
	must(err)
	check(visits == 3, "hincrby: %d", visits)
	all, err := rdb.HGetAll(ctx, k("user")).Result()
	must(err)
	check(all["name"] == "Ana" && all["visits"] == "3" && len(all) == 2, "hgetall: %v", all)
	typ, err := rdb.Type(ctx, k("user")).Result()
	must(err)
	check(typ == "hash", "type: %s", typ)
	_, err = rdb.Get(ctx, k("user")).Result()
	check(err != nil && err.Error()[:9] == "WRONGTYPE", "wrongtype: %v", err)

	// SCAN by prefix, in pages.
	for i := 0; i < 25; i++ {
		must(rdb.Set(ctx, k(fmt.Sprintf("scan:%02d", i)), i, 0).Err())
	}
	var scanned []string
	iter := rdb.Scan(ctx, 0, p+"scan:*", 7).Iterator()
	for iter.Next(ctx) {
		scanned = append(scanned, iter.Val())
	}
	must(iter.Err())
	sort.Strings(scanned)
	check(len(scanned) == 25 && scanned[0] == k("scan:00"), "scan: %d %v", len(scanned), scanned)

	must(rdb.Rename(ctx, k("session"), k("session2")).Err())
	ttl, err = rdb.TTL(ctx, k("session2")).Result()
	must(err)
	check(ttl > 59*time.Minute, "renamed ttl: %v", ttl)

	// Expiry.
	must(rdb.Set(ctx, k("short"), "x", 300*time.Millisecond).Err())
	time.Sleep(350 * time.Millisecond)
	_, err = rdb.Get(ctx, k("short")).Result()
	check(errors.Is(err, redis.Nil), "expired: %v", err)

	info, err := rdb.Info(ctx).Result()
	must(err)
	fmt.Printf("ok: go-redis against bucketdb (%d bytes of INFO)\n", len(info))
}
