package source

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/go-spatial/tegola/dict"
	"github.com/go-spatial/tegola/internal/env"
	"github.com/go-spatial/tegola/internal/log"
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
	defaultKeyPattern = "^tegola_app:([a-z0-9]+)$"
)

// RedisConfigSource is a config source for loading and watching keys in Redis.
// It uses JSON instead of TOML
type RedisConfigSource struct {
	// network  string
	// address  string
	// password string
	// db       string
	// ssl      bool
	keyRegex *regexp.Regexp
	client   *redis.Client
}

func (s *RedisConfigSource) init(options env.Dict, baseDir string /*unused*/) (err error) {
	s.client, err = createClient(options)
	if err != nil {
		return fmt.Errorf("Failed connecting to redis: %s", err)
	}

	keyPattern, err := options.String("keyPattern", &defaultKeyPattern)
	if err != nil {
		return fmt.Errorf("Could not read keyPattern config: %s", err)
	}
	s.keyRegex, err = regexp.Compile(keyPattern)
	if err != nil {
		return fmt.Errorf(`Could not compile keyPattern "%s": %s`, keyPattern, err)
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
		pubsub := s.client.PSubscribe("__keyevent*")
		defer pubsub.PUnsubscribe()

		for {
			msg, err := pubsub.ReceiveMessage()
			if err != nil {
				log.Errorf("Error receiving Redis event. %s", err)
				continue
			}

			// Does the key match the expected pattern?
			matches := s.keyRegex.FindStringSubmatch(msg.Payload)
			if len(matches) != 2 {
				log.Debugf("Encountered a key which doesn't match expected pattern: %s. Skipping.", msg.Payload)
				continue
			}

			appKey := matches[1]

			// Handle set
			if strings.HasSuffix(msg.Channel, ":set") {
				bin, err := s.client.Get(msg.Payload).Result()
				if err != nil {
					log.Errorf(`Could not get key "%s" from Redis: %s`, msg.Payload, err)
					continue
				}

				app := App{}
				log.Info(bin)
				err = json.Unmarshal([]byte(bin), &app)
				if err != nil {
					log.Errorf("Failed parsing JSON for app %s: %s", appKey, err)
					continue
				}

				app.Key = appKey
				appWatcher.Updates <- app

			} else if strings.HasSuffix(msg.Channel, ":expired") || strings.HasSuffix(msg.Channel, ":del") {
				// Handle expired/del
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

func createClient(c dict.Dicter) (*redis.Client, error) {
	opts, err := createOptions(c)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(opts)

	pong, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}
	if pong != "PONG" {
		return nil, fmt.Errorf("redis did not respond with 'PONG', '%s'", pong)
	}

	return client, nil
}
