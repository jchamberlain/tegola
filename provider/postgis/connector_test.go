package postgis

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-spatial/tegola/dict"
	"github.com/go-spatial/tegola/internal/ttools"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestConnector(t *testing.T) {
	ttools.ShouldSkip(t, TESTENV)

	type tcase struct {
		config           dict.Dicter
		env              map[string]string
		expectedConnMode connMode
		expectedSSLMode  SSLMode

		expectErrFn func(t *testing.T, err error)
	}

	fn := func(t *testing.T, tc tcase) {
		t.Helper()

		ctx := t.Context()
		if len(tc.env) != 0 {
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
		}

		c := newDefaultConnector(tc.config)

		_, _, connMeta, err := c.Connect(ctx)
		if err != nil && tc.expectErrFn == nil {
			t.Fatalf("expected connection to be establish but got: %s", err)
		}
		if err != nil && tc.expectErrFn != nil {
			tc.expectErrFn(t, err)
			return
		}

		if connMeta.Mode != tc.expectedConnMode {
			t.Fatalf("expected connMode: %s, but got: %s", tc.expectedConnMode, connMeta.Mode)
		}

		if tc.expectedSSLMode != "" {
			if connMeta.SSLMode != tc.expectedSSLMode {
				t.Fatalf("expected sslmode: %s, got: %s", tc.expectedSSLMode, connMeta.SSLMode)
			}
		}
	}

	tcases := map[string]tcase{
		"connect with uri in connModeURI": {
			config:           dict.Dict(DefaultConfig),
			env:              map[string]string{},
			expectedConnMode: connModeURI,
		},
		"connect via env vars in env mode": {
			config: dict.Dict(map[string]any{}),
			env: map[string]string{
				"PGUSER":     "postgres",
				"PGPASSWORD": "postgres",
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "tegola",
				"PGSSLMODE":  "disable",
			},
			expectedConnMode: connModeEnv,
			expectedSSLMode:  SSLModeDisable,
		},
		"errs with pgconn.ConnectError": {
			config: dict.Dict(map[string]any{}),
			env: map[string]string{
				"PGUSER":     "postgres",
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "tegola",
				"PGSSLMODE":  "disable",
			},
			expectedConnMode: connModeEnv,
			expectedSSLMode:  SSLModeDisable,
			expectErrFn: func(t *testing.T, err error) {
				var ce *pgconn.ConnectError
				if !errors.As(err, &ce) {
					t.Errorf("expected pgconn.ConnectError, got %T (%v)", err, err)
				}

				// also check if meta is wrapped
				validateIncludes := []string{
					"connection failed",
					"mode=", "triggers=", "host=",
					"db=", "user=", "sslmode=",
				}
				for _, str := range validateIncludes {
					if !strings.Contains(err.Error(), str) {
						t.Errorf("expected contains '%s', got %T (%v)", str, err, err)
					}
				}
			},
		},
		"errs with deadline exceeded": {
			config: dict.Dict(map[string]any{}),
			env: map[string]string{
				"PGUSER":     "postgres",
				"PGPASSWORD": "postgres",
				"PGHOST":     "localhost",
				"PGPORT":     "5432",
				"PGDATABASE": "tegola",
				"PGSSLMODE":  "prefer", // will force a failing connection
			},
			expectedConnMode: connModeEnv,
			expectedSSLMode:  SSLModePrefer,
			expectErrFn: func(t *testing.T, err error) {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected deadline exceceded, got %T (%v)", err, err)
				}
			},
		},
	}

	for name, tc := range tcases {
		t.Run(name, func(t *testing.T) {
			fn(t, tc)
		})
	}
}

func TestConnectTimeoutFromConfig(t *testing.T) {
	type tcase struct {
		config      dict.Dict
		expected    time.Duration
		expectedErr bool
	}

	testCases := map[string]tcase{
		"default":  {config: dict.Dict{}, expected: 10 * time.Second},
		"custom":   {config: dict.Dict{ConfigKeyConnectTimeout: "30s"}, expected: 30 * time.Second},
		"invalid":  {config: dict.Dict{ConfigKeyConnectTimeout: "thirty"}, expectedErr: true},
		"zero":     {config: dict.Dict{ConfigKeyConnectTimeout: "0s"}, expectedErr: true},
		"negative": {config: dict.Dict{ConfigKeyConnectTimeout: "-1s"}, expectedErr: true},
		"number":   {config: dict.Dict{ConfigKeyConnectTimeout: 30}, expectedErr: true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := connectTimeoutFromConfig(tc.config)
			if tc.expectedErr {
				if err == nil {
					t.Fatalf("expected error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}
