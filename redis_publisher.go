package reporter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher implements the reporter.Publisher interface to dispatch
// structured error reports to a Redis destination.
//
// It supports real-time message broadcasting using Redis Pub/Sub mechanisms.
type RedisPublisher struct {
	client  *redis.Client
	channel string
}

// NewRedisPublisher creates and initializes a new Redis-backed reporter publisher.
//
// Parameters:
//   - client: An active *redis.Client connection instance.
//   - channel: The target Redis channel name where logs will be published.
//
// Example:
//
//	pub := reporter.NewRedisPublisher(rdbClient, "app_error_logs")
func NewRedisPublisher(client *redis.Client, channel string) *RedisPublisher {
	return &RedisPublisher{
		client:  client,
		channel: channel,
	}
}

// Publish serializes the structured CustomError report into a JSON payload
// and broadcasts it asynchronously to the configured Redis channel using the PUBLISH command.
//
// It satisfies the reporter.Publisher interface requirements. Returns an error
// if JSON marshalling fails or if the Redis server is unreachable.
func (p *RedisPublisher) Publish(ctx context.Context, report *CustomError) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("redis publisher marshal error: %w", err)
	}

	// Menggunakan Redis PUBLISH (Mekanisme Pub/Sub)
	return p.client.Publish(ctx, p.channel, string(payload)).Err()
}
