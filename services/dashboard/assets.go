package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/azylman/aerial/pkg/httpkit"
)

type StaticAsset struct {
	Data        []byte
	Hash        string
	ETag        string
	ContentType string
}

type AssetRegistry struct {
	mu     sync.RWMutex
	assets map[string]StaticAsset
}

func buildAssetMap(fsys fs.FS, versionToken string) (map[string]StaticAsset, error) {
	assets := make(map[string]StaticAsset)

	// 1. Compute content hashes for individual assets
	var appJSHash, styleCSSHash string
	if data, err := fs.ReadFile(fsys, "app.js"); err == nil {
		h := sha256.Sum256(data)
		appJSHash = hex.EncodeToString(h[:])
	}
	if data, err := fs.ReadFile(fsys, "style.css"); err == nil {
		h := sha256.Sum256(data)
		styleCSSHash = hex.EncodeToString(h[:])
	}

	// Determine asset version strings to inject
	jsVersion := versionToken
	cssVersion := versionToken
	if versionToken == "" || versionToken == "latest" || versionToken == "dev" || versionToken == "unknown" {
		if len(appJSHash) >= 10 {
			jsVersion = appJSHash[:10]
		} else {
			jsVersion = "v1"
		}
		if len(styleCSSHash) >= 10 {
			cssVersion = styleCSSHash[:10]
		} else {
			cssVersion = "v1"
		}
	}

	// 2. Walk the filesystem and populate registry
	err := fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		rawBytes, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}

		cleanPath := filepath.ToSlash(path)
		cleanPath = strings.TrimPrefix(cleanPath, "./")

		var finalBytes []byte
		if cleanPath == "index.html" {
			htmlStr := string(rawBytes)
			reJS := regexp.MustCompile(`src="app\.js(?:\?[^"]*)?"`)
			htmlStr = reJS.ReplaceAllString(htmlStr, fmt.Sprintf(`src="app.js?v=%s"`, jsVersion))

			reCSS := regexp.MustCompile(`href="style\.css(?:\?[^"]*)?"`)
			htmlStr = reCSS.ReplaceAllString(htmlStr, fmt.Sprintf(`href="style.css?v=%s"`, cssVersion))

			finalBytes = []byte(htmlStr)
		} else {
			finalBytes = rawBytes
		}

		hash := sha256.Sum256(finalBytes)
		hashStr := hex.EncodeToString(hash[:])
		etag := fmt.Sprintf(`"%s"`, hashStr[:16])

		assets[cleanPath] = StaticAsset{
			Data:        finalBytes,
			Hash:        hashStr,
			ETag:        etag,
			ContentType: GetMimeType(cleanPath),
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return assets, nil
}

func NewAssetRegistry(fsys fs.FS, versionToken string) (*AssetRegistry, error) {
	assets, err := buildAssetMap(fsys, versionToken)
	if err != nil {
		return nil, err
	}
	return &AssetRegistry{
		assets: assets,
	}, nil
}

func (ar *AssetRegistry) Reload(fsys fs.FS, versionToken string) error {
	built, err := buildAssetMap(fsys, versionToken)
	if err != nil {
		return err
	}
	ar.mu.Lock()
	ar.assets = built
	ar.mu.Unlock()
	return nil
}

func (ar *AssetRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	reqPath := strings.TrimPrefix(r.URL.Path, "/dashboard")
	reqPath = strings.TrimPrefix(reqPath, "/")
	if reqPath == "" || reqPath == "index.html" {
		reqPath = "index.html"
	}

	ar.mu.RLock()
	asset, exists := ar.assets[reqPath]
	ar.mu.RUnlock()
	if !exists {
		http.NotFound(w, r)
		return
	}

	// Bifurcated Cache Policy:
	// - index.html / unversioned: no-cache, must-revalidate + ETag
	// - Versioned assets (?v=...): public, max-age=31536000, immutable + ETag
	isVersioned := r.URL.Query().Get("v") != "" && reqPath != "index.html"
	if isVersioned {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache, must-revalidate")
	}

	w.Header().Set("ETag", asset.ETag)
	w.Header().Set("Content-Type", asset.ContentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(asset.Data)))

	// Check conditional validation (If-None-Match)
	if MatchesETag(r.Header.Get("If-None-Match"), asset.ETag, asset.Hash) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	httpkit.WriteResponse(w, r, "dashboard", asset.Data)
}
