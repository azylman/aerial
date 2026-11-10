package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"path"
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

// The React console is built by Vite straight into the static root (see Dockerfile): index.html
// is the SPA shell served for every extensionless client-side route, and everything under assets/
// carries a content hash in its filename, so it is cached forever.
const (
	indexPath       = "index.html"
	assetsPrefix    = "assets/"
	immutableCache  = "public, max-age=31536000, immutable"
	revalidateCache = "no-cache, must-revalidate"
)

type AssetRegistry struct {
	mu     sync.RWMutex
	assets map[string]StaticAsset
}

func buildAssetMap(fsys fs.FS) (map[string]StaticAsset, error) {
	assets := make(map[string]StaticAsset)
	err := fs.WalkDir(fsys, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		hashStr := hex.EncodeToString(hash[:])
		assets[name] = StaticAsset{
			Data:        data,
			Hash:        hashStr,
			ETag:        fmt.Sprintf(`"%s"`, hashStr[:16]),
			ContentType: GetMimeType(name),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return assets, nil
}

func NewAssetRegistry(fsys fs.FS) (*AssetRegistry, error) {
	assets, err := buildAssetMap(fsys)
	if err != nil {
		return nil, err
	}
	return &AssetRegistry{
		assets: assets,
	}, nil
}

func (ar *AssetRegistry) Reload(fsys fs.FS) error {
	built, err := buildAssetMap(fsys)
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

	reqPath := strings.TrimPrefix(r.URL.Path, "/console")
	reqPath = strings.TrimPrefix(reqPath, "/")
	if reqPath == "" {
		reqPath = indexPath
	}

	ar.mu.RLock()
	asset, exists := ar.assets[reqPath]
	if !exists && isClientRoute(reqPath) {
		reqPath = indexPath
		asset, exists = ar.assets[reqPath]
	}
	ar.mu.RUnlock()
	if !exists {
		http.NotFound(w, r)
		return
	}

	// Vite content-hashed assets/* never change under the same name; everything else (the SPA
	// shell, favicons) revalidates against its ETag.
	if strings.HasPrefix(reqPath, assetsPrefix) {
		w.Header().Set("Cache-Control", immutableCache)
	} else {
		w.Header().Set("Cache-Control", revalidateCache)
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
	httpkit.WriteResponse(w, r, "console", asset.Data)
}

// isClientRoute reports whether a missing path is a console deep link that should receive the
// SPA shell: extensionless and not a (stale) hashed asset request, which must 404 rather than
// hand the browser HTML for a module script.
func isClientRoute(reqPath string) bool {
	return !strings.HasPrefix(reqPath, assetsPrefix) && path.Ext(reqPath) == ""
}
