package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/brokers/errs"
	"github.com/RichardKnop/machinery/v1/brokers/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/RichardKnop/redsync"
	"github.com/gomodule/redigo/redis"
)

var (
	redisDelayedTasksKey = "delayed_tasks"
	luaScriptContent     = `
		-- 约定：返回 0 代表没有取到值。如需修改此值，可能需要同步修改程序中部分逻辑。 0 并不是一个很可靠的标志, 但是在本框架的应用场景下已经能满足需求了
		-- 从 meta queue 取 10 个队列名字
		redis.replicate_commands() -- 保证在 master-slave redis 架构中能正常运行
		local metaqueue_name = KEYS[1]
		local client_time = tonumber(KEYS[2])
		for first=10,1,-1 do
			local queues = redis.call('SRANDMEMBER', metaqueue_name, 1)
			if #queues == 0 then
				return 0
			end

			local queue = queues[1]
			local queue_expired_key = 'queue:expired:'..queue

			-- 循环读取取出的队列名字, 依次读取里面的内容,读取到内容就返回。如果所有队列都是空的则返回 0
			local queue_existed = redis.call('EXISTS', queue)

			if queue_existed == 1 then
				redis.call('DEL', queue_expired_key)
				local ret = redis.call('RPOP', queue)
				return ret
			end

			-- 更改某个队列读取不到的次数, 超过一定次数就从 metaqueue 中删除掉
			local queue_expired_key_existed = redis.call('EXISTS', queue_expired_key)
			if queue_expired_key_existed == 1 then
				local queue_expired_time = redis.call('GET', queue_expired_key)
				if client_time > tonumber(queue_expired_time) then
					redis.call('SMOVE', metaqueue_name, 'task:queue:history:collections', queue)
					redis.call('DEL', queue_expired_key)
				end
			else
				redis.call('SET', queue_expired_key, client_time+900)
				redis.call('EXPIRE', queue_expired_key, 3600)
			end
		end
		return 0
		`

	luaScript          *redis.Script
	ErrNoLuaScript     = errors.New("redigo, machinery: NO LUA SCRIPT FOUND")
	NilLuaResult       = "0" // 如需修改此值，需要同步修改 lua script
	ErrEmptyRoutingKey = errors.New("routing key should not been empty!")
	/*
			在系统启动时, 计算出当前时间和 redis server 上的时间差。如果 server 时间期间被改过或者在长期运行后
		redis-server 和服务所在机器存在 累积时间误差则不考虑
	*/
	timeDelta int
)

func getClientTime() int {
	t := time.Now().Second()
	return t + timeDelta
}

func syncRedisServerTime(conn redis.Conn) {
	n, err := conn.Do("TIME")
	if err != nil {
		log.FATAL.Printf(err.Error())
	}

	results, _ := redis.ByteSlices(n, nil)
	serverTime, _ := strconv.Atoi(string(results[0]))
	timeDelta = serverTime - time.Now().Second()

}

// Broker represents a Redis broker
type Broker struct {
	common.Broker
	common.RedisConnector
	host         string
	password     string
	db           int
	pool         *redis.Pool
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
}

// New creates new Broker instance
func New(cnf *config.Config, host, password, socketPath string, db int) iface.Broker {
	b := &Broker{Broker: common.NewBroker(cnf)}
	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	return b
}

// StartConsuming enters a loop and waits for incoming messages
func (b *Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor iface.TaskProcessor) (bool, error) {
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	if concurrency < 1 {
		concurrency = 1
	}

	b.Broker.StartConsuming(consumerTag, concurrency, taskProcessor)

	conn := b.open()
	defer conn.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")
	if err != nil {
		b.GetRetryFunc()(b.GetRetryStopChan())
		return b.GetRetry(), err
	}

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				close(deliveries)
				return
			case <-pool:
				task, _ := b.retryToGetNextTask()
				//TODO: should this error be ignored?
				if len(task) > 0 {
					deliveries <- task
				}
				pool <- struct{}{}
			}
		}
	}()

	// A goroutine to watch for delayed tasks and push them to deliveries
	// channel for consumption by the worker
	b.delayedWG.Add(1)
	go func() {
		defer b.delayedWG.Done()

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.GetStopChan():
				return
			default:
				task, err := b.nextDelayedTask(redisDelayedTasksKey)
				if err != nil {
					continue
				}

				signature := new(tasks.Signature)
				decoder := json.NewDecoder(bytes.NewReader(task))
				decoder.UseNumber()
				if err := decoder.Decode(signature); err != nil {
					log.ERROR.Print(errs.NewErrCouldNotUnmarshaTaskSignature(task, err))
				}

				if err := b.Publish(context.Background(), signature); err != nil {
					log.ERROR.Print(err)
				}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency, taskProcessor); err != nil {
		return b.GetRetry(), err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.GetRetry(), nil
}

// StopConsuming quits the loop
func (b *Broker) StopConsuming() {
	b.Broker.StopConsuming()
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()
	// Waiting for consumption to finish
	b.consumingWG.Wait()

	if b.pool != nil {
		b.pool.Close()
	}
}

// Publish places a new message on the default queue
func (b *Broker) Publish(ctx context.Context, signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	// 如果 routeingKey 为空则直接返回错误
	if signature.RoutingKey == "" {
		return ErrEmptyRoutingKey
	}

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", redisDelayedTasksKey, score, msg)
			return err
		}
	}

	// 约定使用 defaultQueue 做为数据交互！
	if _, err := conn.Do("RPUSH", signature.RoutingKey, msg); err != nil {
		return err
	}

	if _, err := conn.Do("SADD", b.GetConfig().DefaultQueue, signature.RoutingKey); err != nil {
		return err
	}

	return nil
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {
	conn := b.open()
	defer conn.Close()

	if queue == "" {
		queue = b.GetConfig().DefaultQueue
	}
	dataBytes, err := conn.Do("LRANGE", queue, 0, -1)
	if err != nil {
		return nil, err
	}
	results, err := redis.ByteSlices(dataBytes, err)
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(bytes.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *Broker) consume(deliveries <-chan []byte, concurrency int, taskProcessor iface.TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *Broker) consumeOne(delivery []byte, taskProcessor iface.TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errs.NewErrCouldNotUnmarshaTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		// 直接返回, 不再 reenqueue message
		return nil
	}

	log.DEBUG.Printf("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *Broker) nextTask() ([]byte, error) {
	conn := b.open()
	defer conn.Close()

	err := luaScript.SendHash(conn, b.GetConfig().DefaultQueue, getClientTime())
	if err != nil {
		log.ERROR.Printf("Failed to sendhash to redis, Reason: %s\n", err.Error())
		return []byte{}, err
	}

	err = conn.Flush()
	if err != nil {
		log.ERROR.Printf("redis conn flush error, Reason: %s\n", err.Error())
		return []byte{}, err
	}

	result, err := conn.Receive()
	if err != nil {
		if strings.Contains(err.Error(), "NOSCRIPT") {
			return []byte{}, ErrNoLuaScript
		}
		return []byte{}, err
	}

	if fmt.Sprintf("%v", result) == NilLuaResult {
		return []byte{}, redis.ErrNil
	}

	//需要尝试转化为 byte array
	switch v := result.(type) {
	case []uint8:
		return []byte(result.([]uint8)), nil
	default:
		log.ERROR.Printf("UNSupported return type %v\n", v)
		return []byte{}, nil
	}
}

func (b *Broker) retryToGetNextTask() ([]byte, error) {
	result, err := b.nextTask()
	if err == nil {
		return result, nil
	} else if err == redis.ErrNil {
		return []byte{}, redis.ErrNil
	} else if err == ErrNoLuaScript {
		conn := b.pool.Get()
		defer conn.Close()
		log.INFO.Printf("Will reload the lua script to redis\n")
		loadLuaScript(conn)
		return b.nextTask()
	} else {
		log.ERROR.Printf("Get task error, Reason: %s\n", err.Error())
		return []byte{}, redis.ErrNil
	}
}

// nextDelayedTask pops a value from the ZSET key using WATCH/MULTI/EXEC commands.
// https://github.com/gomodule/redigo/blob/master/redis/zpop_example_test.go
func (b *Broker) nextDelayedTask(key string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	defer func() {
		// Return connection to normal state on error.
		// https://redis.io/commands/discard
		if err != nil {
			conn.Do("DISCARD")
		}
	}()

	var (
		items [][]byte
		reply interface{}
	)

	var pollPeriod = 500 // default poll period for delayed tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.DelayedTasksPollPeriod
		// the default period is 0, which bombards redis with requests, despite
		// our intention of doing the opposite
		if configuredPollPeriod > 0 {
			pollPeriod = configuredPollPeriod
		}
	}

	for {
		// Space out queries to ZSET so we don't bombard redis
		// server with relentless ZRANGEBYSCOREs
		time.Sleep(time.Duration(pollPeriod) * time.Millisecond)
		if _, err = conn.Do("WATCH", key); err != nil {
			return
		}

		now := time.Now().UTC().UnixNano()

		// https://redis.io/commands/zrangebyscore
		items, err = redis.ByteSlices(conn.Do(
			"ZRANGEBYSCORE",
			key,
			0,
			now,
			"LIMIT",
			0,
			1,
		))
		if err != nil {
			return
		}
		if len(items) != 1 {
			err = redis.ErrNil
			return
		}

		_ = conn.Send("MULTI")
		_ = conn.Send("ZREM", key, items[0])
		reply, err = conn.Do("EXEC")
		if err != nil {
			return
		}

		if reply != nil {
			result = items[0]
			break
		}
	}

	return
}

// open returns or creates instance of Redis connection
func (b *Broker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New([]redsync.Pool{b.pool})
		// add by icecraft, load the redis lua script
		conn := b.pool.Get()
		defer conn.Close()
		log.INFO.Printf("Will load the lua script to redis\n")
		loadLuaScript(conn)
		syncRedisServerTime(conn)
	})

	return b.pool.Get()
}

func loadLuaScript(conn redis.Conn) {
	luaScript = redis.NewScript(2, luaScriptContent)
	if err := luaScript.Load(conn); err != nil {
		log.FATAL.Printf(err.Error())
	}
}
