package source

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-spatial/tegola/dict"
	"github.com/go-spatial/tegola/internal/env"
	"github.com/go-spatial/tegola/internal/log"
	"github.com/redis/go-redis/v9"
)

const (
	ConfigKeyNetwork  = "network"
	ConfigKeyAddress  = "address"
	ConfigKeyPassword = "password"
	ConfigKeyDB       = "db"
	ConfigKeyTTL      = "ttl"
	ConfigKeySSL      = "ssl"
)

var (
	// default values
	defaultNetwork    = "tcp"
	defaultAddress    = "127.0.0.1:6379"
	defaultPassword   = ""
	defaultDB         = 0
	defaultSSL        = false
	defaultKeyPattern = "tegola_app:*"
)

// RedisConfigSource is a config source for loading and watching keys in Redis.
// It uses JSON instead of TOML
type RedisConfigSource struct {
	keyPattern string
	client     *redis.Client
}

func (s *RedisConfigSource) init(options env.Dict, baseDir string /*unused*/) (err error) {
	s.client, err = createClient(context.Background(), options)
	if err != nil {
		return fmt.Errorf("Failed connecting to redis: %s", err)
	}

	s.keyPattern, err = options.String("keyPattern", &defaultKeyPattern)
	if err != nil {
		return fmt.Errorf("Could not read keyPattern config: %s", err)
	}

	return nil
}

func (s *RedisConfigSource) Type() string {
	return "redis"
}

func (s *RedisConfigSource) LoadAndWatch(ctx context.Context) (ConfigWatcher, error) {
	appWatcher := ConfigWatcher{
		Updates:   make(chan App),
		Deletions: make(chan string),
	}

	go func() {
		defer appWatcher.Close()

		// Subscribe additions/removals/edits.
		pubsub := s.client.PSubscribe(ctx, "__keyevent*")
		defer pubsub.PUnsubscribe(ctx)

		for {
			msg, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				log.Errorf("Error receiving Redis event. %s", err)
				continue
			}

			// Does the key match the expected pattern?
			matched, err := filepath.Match(s.keyPattern, msg.Payload)
			if !matched {
				log.Debugf("Encountered a key which doesn't match expected pattern: %s. Skipping.", msg.Payload)
				continue
			}

			appKey := msg.Payload

			if strings.HasSuffix(msg.Channel, ":set") {
				// Handle set
				s.loadApp(ctx, msg.Payload, appWatcher.Updates)
			} else if strings.HasSuffix(msg.Channel, ":expired") || strings.HasSuffix(msg.Channel, ":del") {
				// Handle expired and delete
				appWatcher.Deletions <- appKey
			}
		}
	}()

	// Now load what's already in Redis.
	// It's important that we do this after starting the subscription so that nothing is missed. If we scanned the current contents of
	// Redis and then subscribed, it's possible keys could be added or removed or modified between the two operations and we'd never know.
	go func() {
		var cursor uint64
		for {
			var keys []string
			var err error
			keys, cursor, err = s.client.Scan(ctx, cursor, s.keyPattern, 10).Result()
			if err != nil {
				log.Error(err)
				break
			}

			for _, key := range keys {
				s.loadApp(ctx, key, appWatcher.Updates)
			}

			if cursor == 0 {
				break
			}
		}
	}()

	return appWatcher, nil
}

// CreateOptions creates redis.Options from an implicit or explicit c
// Copied from cache
func createOptions(c dict.Dicter) (opts *redis.Options, err error) {
	network, err := c.String(ConfigKeyNetwork, &defaultNetwork)
	if err != nil {
		return nil, err
	}

	addr, err := c.String(ConfigKeyAddress, &defaultAddress)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	if host == "" {
		return nil, fmt.Errorf("no host provided in '%s'", addr)
	}

	password, err := c.String(ConfigKeyPassword, &defaultPassword)
	if err != nil {
		return nil, err
	}

	db, err := c.Int(ConfigKeyDB, &defaultDB)
	if err != nil {
		return nil, err
	}

	o := &redis.Options{
		Network:     network,
		Addr:        addr,
		Password:    password,
		DB:          db,
		PoolSize:    2,
		DialTimeout: 3 * time.Second,
	}

	ssl, err := c.Bool(ConfigKeySSL, &defaultSSL)
	if err != nil {
		return nil, err
	}

	if ssl {
		o.TLSConfig = &tls.Config{ServerName: host}
	}

	return o, nil
}

func createClient(ctx context.Context, c dict.Dicter) (*redis.Client, error) {
	opts, err := createOptions(c)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	pong, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}
	if pong != "PONG" {
		return nil, fmt.Errorf("redis did not respond with 'PONG', '%s'", pong)
	}

	return client, nil
}

// loadApp reads the key in redis and loads the app into the updates channel.
func (s *RedisConfigSource) loadApp(ctx context.Context, key string, updates chan App) {
	matched, err := filepath.Match(s.keyPattern, key)
	if !matched || err != nil {
		log.Debugf("Encountered a key which doesn't match expected pattern: %s. Skipping. (%s)", key, err)
		return
	}

	bin, err := s.client.Get(ctx, key).Result()
	if err != nil {
		log.Errorf(`Could not get key "%s" from Redis: %s`, key, err)
		return
	}

	app := App{}
	err = json.Unmarshal([]byte(bin), &app)
	if err != nil {
		log.Errorf("Failed parsing JSON for app %s: %s", key, err)
		return
	}

	app.Key = key
	updates <- app
}
