package reporter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher implements the reporter.Publisher interface by appending
// structured error reports to a Redis list.
//
// Reports are stored with RPUSH, so consumers can process them in FIFO order
// using LPOP or BLPOP from the same list.
type RedisPublisher struct {
	client   *redis.Client
	listName string
}

// NewRedisPublisher creates and initializes a new Redis-backed reporter publisher.
// It keeps the historical constructor name for compatibility; the destination is
// now a Redis list, not channel broadcasting.
//
// Parameters:
//   - client: An active *redis.Client connection instance.
//   - listName: The target Redis list name where reports will be appended.
//
// Example:
//
//	pub := reporter.NewRedisPublisher(rdbClient, "app_error_logs")
func NewRedisPublisher(client *redis.Client, listName string) *RedisPublisher {
	return &RedisPublisher{
		client:   client,
		listName: listName,
	}
}

// NewRedisListPublisher creates a Redis-backed publisher that appends reports to
// a Redis list.
//
// It is equivalent to NewRedisPublisher and exists as the clearer constructor
// for new code.
func NewRedisListPublisher(client *redis.Client, listName string) *RedisPublisher {
	return NewRedisPublisher(client, listName)
}

// Publish serializes the structured CustomError report into a JSON payload
// and appends it to the configured Redis list using the RPUSH command.
//
// It satisfies the reporter.Publisher interface requirements. Returns an error
// if JSON marshalling fails or if the Redis server is unreachable.
func (p *RedisPublisher) Publish(ctx context.Context, report *CustomError) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("redis publisher marshal error: %w", err)
	}

	return p.client.RPush(ctx, p.listName, string(payload)).Err()
}
