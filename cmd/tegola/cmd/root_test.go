package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dimfeld/httptreemux"

	"github.com/go-spatial/tegola/config"
	"github.com/go-spatial/tegola/config/source"
	"github.com/go-spatial/tegola/internal/env"
	"github.com/go-spatial/tegola/mapbox/tilejson"
	"github.com/go-spatial/tegola/provider"
	_ "github.com/go-spatial/tegola/provider/test"
	"github.com/go-spatial/tegola/server"
)

type initializerMock struct {
	initProvidersCalls chan bool
	initMapsCalls      chan bool
	unloadCalls        chan bool
}

func (i initializerMock) initProviders(providersConfig []env.Dict, maps []provider.Map, namespace string) (map[string]provider.TilerUnion, error) {
	i.initProvidersCalls <- true
	return map[string]provider.TilerUnion{}, nil
}

func (i initializerMock) initProvidersCalled() bool {
	select {
	case _, ok := <-i.initProvidersCalls:
		return ok
	case <-time.After(time.Millisecond):
		return false
	}
}

func (i initializerMock) initMaps(maps []provider.Map, providers map[string]provider.TilerUnion) error {
	i.initMapsCalls <- true
	return nil
}

func (i initializerMock) initMapsCalled() bool {
	select {
	case _, ok := <-i.initMapsCalls:
		return ok
	case <-time.After(time.Millisecond):
		return false
	}
}

func (i initializerMock) unload(app source.App) {
	i.unloadCalls <- true
}

func (i initializerMock) unloadCalled() bool {
	select {
	case _, ok := <-i.unloadCalls:
		return ok
	case <-time.After(time.Millisecond):
		return false
	}
}

func TestInitAppConfigSource(t *testing.T) {
	var (
		cfg config.Config
		err error
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Should return nil if app config source type not specified.
	cfg = config.Config{}
	err = initAppConfigSource(ctx, cfg)
	if err != nil {
		t.Errorf("Unexpected error when app config source type is not specified: %s", err)
	}

	// Should return error if unable to initialize source.
	cfg = config.Config{
		AppConfigSource: env.Dict{
			"type": "something_nonexistent",
		},
	}
	err = initAppConfigSource(ctx, cfg)
	if err == nil {
		t.Error("Should return an error if invalid source type provided")
	}

	cfg = config.Config{
		AppConfigSource: env.Dict{
			"type": "file",
			"dir":  "something_nonexistent",
		},
	}
	err = initAppConfigSource(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Errorf("Should return an error if unable to initialize source; expected an error about the directory, got %v", err)
	}
}

func TestWatchAppUpdates(t *testing.T) {
	loader := initializerMock{
		initProvidersCalls: make(chan bool),
		initMapsCalls:      make(chan bool),
		unloadCalls:        make(chan bool),
	}
	// watcher := mock.NewWatcherMock()
	watcher := source.ConfigWatcher{
		Updates:   make(chan source.App),
		Deletions: make(chan string),
	}
	defer watcher.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchAppUpdates(ctx, watcher, loader)

	// Should load new map+provider
	app := source.App{
		Providers: []env.Dict{},
		Maps:      []provider.Map{},
		Key:       "Test1",
	}
	// watcher.SendUpdate(app)
	watcher.Updates <- app
	if !loader.initProvidersCalled() {
		t.Error("Failed to initialize providers")
	}
	if !loader.initMapsCalled() {
		t.Error("Failed to initialize maps")
	}
	if loader.unloadCalled() {
		t.Error("Unexpected app unload")
	}

	// Should load updated map+provider
	// watcher.SendUpdate(app)
	watcher.Updates <- app
	if !loader.unloadCalled() {
		t.Error("Failed to unload old app")
	}
	if !loader.initProvidersCalled() {
		t.Error("Failed to initialize providers")
	}
	if !loader.initMapsCalled() {
		t.Error("Failed to initialize maps")
	}

	// Should unload map+provider
	// watcher.SendDeletion("Test1")
	watcher.Deletions <- "Test1"
	if !loader.unloadCalled() {
		t.Error("Failed to unload old app")
	}
}

func TestGetMapNames(t *testing.T) {
	app := source.App{
		Maps: []provider.Map{
			{Name: "First Map"},
			{Name: "Second Map"},
		},
	}
	expected := []string{"First Map", "Second Map"}
	names := getMapNames(app)
	if !reflect.DeepEqual(expected, names) {
		t.Errorf("Expected map names %v; found %v", expected, names)
	}
}

func TestGetProviderNames(t *testing.T) {
	var (
		app      source.App
		names    []string
		expected []string
	)

	// Happy path
	app = source.App{
		Providers: []env.Dict{
			{"name": "First Provider"},
			{"name": "Second Provider"},
		},
	}
	expected = []string{"First Provider", "Second Provider"}
	names = getProviderNames(app)
	if !reflect.DeepEqual(expected, names) {
		t.Errorf("Expected provider names %v; found %v", expected, names)
	}

	// Error
	app = source.App{
		Providers: []env.Dict{
			{},
			{"name": "Second Provider"},
		},
	}
	expected = []string{"Second Provider"}
	names = getProviderNames(app)
	if !reflect.DeepEqual(expected, names) {
		t.Errorf("Expected provider names %v; found %v", expected, names)
	}
}

func TestAppReloadInvalidatesCapabilities(t *testing.T) {
	const mapName = "reload-test-map"

	newApp := func(attribution string) source.App {
		return source.App{
			Key: "reload-test-app",
			Providers: []env.Dict{
				{"name": "reload-test-provider", "type": "test"},
			},
			Maps: []provider.Map{
				{
					Name:        mapName,
					Attribution: env.String(attribution),
					Layers: []provider.MapLayer{
						{ProviderLayer: "reload-test-provider.test-layer"},
					},
				},
			},
		}
	}

	loader := appInitializer{}
	load := func(app source.App) {
		t.Helper()
		if err := config.ValidateApp(&app); err != nil {
			t.Fatalf("invalid app: %v", err)
		}
		providers, err := loader.initProviders(app.Providers, app.Maps, app.Key)
		if err != nil {
			t.Fatalf("error initializing providers: %v", err)
		}
		if err = loader.initMaps(app.Maps, providers); err != nil {
			t.Fatalf("error initializing maps: %v", err)
		}
	}

	getCapabilities := func() (int, tilejson.TileJSON) {
		t.Helper()
		r, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/capabilities/"+mapName+".json", nil)
		r = r.WithContext(httptreemux.AddParamsToContext(r.Context(), map[string]string{"map_name": mapName + ".json"}))

		w := httptest.NewRecorder()
		server.HandleMapCapabilities{}.ServeHTTP(w, r)

		var tileJSON tilejson.TileJSON
		if w.Code == http.StatusOK {
			if err := json.NewDecoder(w.Body).Decode(&tileJSON); err != nil {
				t.Fatalf("error decoding tilejson: %v", err)
			}
		}
		return w.Code, tileJSON
	}

	assertAttribution := func(expected string) {
		t.Helper()
		code, tileJSON := getCapabilities()
		if code != http.StatusOK {
			t.Fatalf("capabilities request failed: got %v expected %v", code, http.StatusOK)
		}
		if tileJSON.Attribution == nil {
			t.Fatalf("missing attribution, expected %v", expected)
		}
		if *tileJSON.Attribution != expected {
			t.Errorf("got attribution %v expected %v", *tileJSON.Attribution, expected)
		}
	}

	v1 := newApp("v1")
	load(v1)
	assertAttribution("v1")

	// Reloading the app should serve the new version, not the cached one.
	loader.unload(v1)
	v2 := newApp("v2")
	load(v2)
	defer loader.unload(v2)
	assertAttribution("v2")

	// Once the app is unloaded, its map should no longer be served.
	loader.unload(v2)
	if code, _ := getCapabilities(); code == http.StatusOK {
		t.Errorf("unloaded map should not be served from cache")
	}
}
