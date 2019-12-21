package eventbus

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// PartitionOffsets represents the offsets for each partition.
type PartitionOffsets map[int32]int64

// MarshalJSON formats the numeric data as strings because eventbus-sub expects
// it.
func (po PartitionOffsets) MarshalJSON() ([]byte, error) {
	// https://github.com/heroku/eventbus/blob/master/PROTOCOL.md#eventbus-consumer-protocol
	data := make(map[string]string)

	for k, v := range po {
		data[strconv.Itoa(int(k))] = strconv.Itoa(int(v))
	}
	return json.Marshal(data)
}

type offsetStore interface {
	SetOffset(int32, int64) error
	GetOffsets() (*PartitionOffsets, error)
}

// InMemoryOffsetStore is mostly for testing purposes.
type InMemoryOffsetStore struct {
	offsets PartitionOffsets
}

// NewInMemoryOffsetStore creates a new InMemoryOffsetStore.
func NewInMemoryOffsetStore() *InMemoryOffsetStore {
	return &InMemoryOffsetStore{offsets: make(PartitionOffsets)}
}

// GetOffsets returns either nil, nil if we have no offsets, or the current set
// of recorded offsets and no error.
func (os InMemoryOffsetStore) GetOffsets() (*PartitionOffsets, error) {
	if len(os.offsets) == 0 {
		return nil, nil
	}
	return &os.offsets, nil
}

// SetOffset stores the offset against the partition and always returns a nil
// error.
func (os *InMemoryOffsetStore) SetOffset(partition int32, offset int64) error {
	os.offsets[partition] = offset
	return nil
}

// RedisOffsetStore uses a connection pool to record the offsets and partitions.
type RedisOffsetStore struct {
	prefix string
	pool   *redis.Pool
}

// NewRedisOffsetStore creates a new RedisOffsetStore.
func NewRedisOffsetStore(prefix string, p *redis.Pool) *RedisOffsetStore {
	return &RedisOffsetStore{prefix: prefix, pool: p}
}

// GetOffsets returns the current offsets stored in Redis and possibly an error.
func (rs RedisOffsetStore) GetOffsets() (*PartitionOffsets, error) {
	cmd, args := rs.getOffsetsCmd()
	c := rs.pool.Get()
	defer c.Close()

	return redisToPartitionOffsets(c.Do(cmd, args...))
}

// SetOffset stores the offset against the partition and returns errors returned
// from Redis.
func (rs RedisOffsetStore) SetOffset(partition int32, offset int64) error {
	cmd, args := rs.storeOffsetCmd(partition, offset)
	c := rs.pool.Get()
	defer c.Close()

	r, err := redis.Int(c.Do(cmd, args...))
	if !(r == 1 || r == 0) {
		return errors.New("failed to store offset")
	}

	return err
}

func (rs RedisOffsetStore) storeOffsetCmd(partition int32, offset int64) (string, []interface{}) {
	return "HSET", []interface{}{rs.key(), partition, offset}
}

func (rs RedisOffsetStore) key() string {
	return fmt.Sprintf("%s:offsets", rs.prefix)
}

func (rs RedisOffsetStore) getOffsetsCmd() (string, []interface{}) {
	return "HGETALL", []interface{}{rs.key()}
}

func redisToPartitionOffsets(result interface{}, err error) (*PartitionOffsets, error) {
	values, err := redis.Values(result, err)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, nil
	}
	if len(values)%2 != 0 {
		return nil, errors.New("redisToPartitionOffsets expects even number of values result")
	}
	m := make(PartitionOffsets, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := values[i].([]byte)
		if !ok {
			return nil, errors.New("unable to parse partition offsets")
		}
		value, err := redis.Int64(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		partition, err := strconv.ParseInt(string(key), 10, 32)
		if err != nil {
			return nil, err
		}
		m[int32(partition)] = value
	}
	return &m, nil
}
