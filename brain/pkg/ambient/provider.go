package ambient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"github.com/azylman/aerial/brain/pkg/config"
)

// MCPInvoker defines the contract for executing an MCP tool.
type MCPInvoker interface {
	Execute(ctx context.Context, name string, args map[string]interface{}) (string, error)
}

// ProviderConfig wraps config.AmbientContextConfig.
type ProviderConfig = config.AmbientContextConfig

// Provider fetches tool data and renders ambient context using Go templates.
type Provider struct {
	cfg           config.AmbientContextConfig
	invoker       MCPInvoker
	cache         string
	expiresAt     time.Time
	hardExpiresAt time.Time
	refreshing    int32
	mu            sync.RWMutex
	sf            singleflight.Group
	templateStr   string
	parsedTmpl    *template.Template
	nowFunc       func() time.Time
	cacheTTL      time.Duration
}

// NewProvider creates and initializes a new ambient context Provider.
func NewProvider(cfg config.AmbientContextConfig, invoker MCPInvoker) (*Provider, error) {
	tmplStr := cfg.Template
	if tmplStr == "" && cfg.TemplatePath != "" {
		data, err := os.ReadFile(cfg.TemplatePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read template file: %w", err)
		}
		tmplStr = string(data)
	}

	funcMap := template.FuncMap{
		"default": func(def any, val any) any {
			if val == nil {
				return def
			}
			switch v := val.(type) {
			case string:
				if v == "" || v == "<no value>" {
					return def
				}
			case bool:
				if !v {
					return def
				}
			default:
				rv := reflect.ValueOf(val)
				if !rv.IsValid() {
					return def
				}
				switch rv.Kind() {
				case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
					if rv.Len() == 0 {
						return def
					}
				default:
					if rv.IsZero() {
						return def
					}
				}
			}
			return val
		},
		"trim": func(s any) string {
			if s == nil {
				return ""
			}
			str := fmt.Sprint(s)
			if str == "<no value>" {
				return ""
			}
			return strings.TrimSpace(str)
		},
		"lower": func(s any) string {
			if s == nil {
				return ""
			}
			str := fmt.Sprint(s)
			if str == "<no value>" {
				return ""
			}
			return strings.ToLower(str)
		},
		"upper": func(s any) string {
			if s == nil {
				return ""
			}
			str := fmt.Sprint(s)
			if str == "<no value>" {
				return ""
			}
			return strings.ToUpper(str)
		},
		"json": func(v any) string {
			if v == nil {
				return ""
			}
			b, err := json.Marshal(v)
			if err != nil {
				return ""
			}
			return string(b)
		},
	}

	parsedTmpl, err := template.New("ambient").
		Option("missingkey=zero").
		Funcs(funcMap).
		Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ambient template: %w", err)
	}

	ttl := 30 * time.Second
	if cfg.CacheTTL != "" {
		if d, err := time.ParseDuration(cfg.CacheTTL); err == nil && d > 0 {
			ttl = d
		}
	}

	return &Provider{
		cfg:         cfg,
		invoker:     invoker,
		templateStr: tmplStr,
		parsedTmpl:  parsedTmpl,
		nowFunc:     time.Now,
		cacheTTL:    ttl,
	}, nil
}

// SetNowFunc overrides the clock function (used primarily in tests).
func (p *Provider) SetNowFunc(fn func() time.Time) {
	if fn == nil {
		p.nowFunc = time.Now
	} else {
		p.nowFunc = fn
	}
}

func (p *Provider) now() time.Time {
	return p.nowFunc()
}

// Prime warms the ambient context cache eagerly.
func (p *Provider) Prime(ctx context.Context) error {
	_, err := p.fetchAndRender(ctx)
	return err
}

func (p *Provider) fetchAndRender(ctx context.Context) (string, error) {
	res, err, _ := p.sf.Do("ambient_fetch", func() (interface{}, error) {
		curNow := p.now()
		p.mu.RLock()
		if !p.expiresAt.IsZero() && curNow.Before(p.expiresAt) {
			cached := p.cache
			p.mu.RUnlock()
			return cached, nil
		}
		p.mu.RUnlock()

		timeout := 250 * time.Millisecond
		if p.cfg.TimeoutMs > 0 {
			timeout = time.Duration(p.cfg.TimeoutMs) * time.Millisecond
		}
		toolCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		g, groupCtx := errgroup.WithContext(toolCtx)
		data := make(map[string]any)
		var dataMu sync.Mutex

		for _, tool := range p.cfg.Tools {
			t := tool
			g.Go(func() error {
				var val any
				if p.invoker == nil {
					val = map[string]any{"error": "nil mcp invoker"}
				} else {
					resText, err := p.invoker.Execute(groupCtx, t.Tool, t.Arguments)
					if err != nil {
						val = map[string]any{"error": err.Error()}
					} else {
						var parsed any
						if err := json.Unmarshal([]byte(resText), &parsed); err == nil {
							val = parsed
						} else {
							val = map[string]any{"raw": resText, "text": resText}
						}
					}
				}
				dataMu.Lock()
				data[t.Name] = val
				dataMu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}

		data["now"] = p.now().Format("15:04:05")

		var buf bytes.Buffer
		if err := p.parsedTmpl.Execute(&buf, data); err != nil {
			return "", fmt.Errorf("ambient template execution error: %w", err)
		}

		rendered := strings.ReplaceAll(buf.String(), "<no value>", "")
		trimmed := strings.TrimSpace(rendered)
		var formatted string
		if trimmed != "" {
			formatted = fmt.Sprintf("<ambient_context>\n%s\n</ambient_context>", trimmed)
		}

		p.mu.Lock()
		p.cache = formatted
		now := p.now()
		p.expiresAt = now.Add(p.cacheTTL)
		p.hardExpiresAt = now.Add(2 * p.cacheTTL)
		p.mu.Unlock()

		return formatted, nil
	})

	if err != nil {
		return "", err
	}
	resStr, ok := res.(string)
	if !ok {
		return "", fmt.Errorf("unexpected ambient result type: %T", res)
	}
	return resStr, nil
}

// Retrieve returns the rendered ambient context string, caching results in RAM for CacheTTL.
func (p *Provider) Retrieve(ctx context.Context) (string, error) {
	now := p.now()
	p.mu.RLock()
	cached := p.cache
	fresh := !p.expiresAt.IsZero() && now.Before(p.expiresAt)
	withinHardTTL := !p.hardExpiresAt.IsZero() && now.Before(p.hardExpiresAt)
	p.mu.RUnlock()

	if fresh {
		return cached, nil
	}

	if withinHardTTL && cached != "" {
		if atomic.CompareAndSwapInt32(&p.refreshing, 0, 1) {
			go func() {
				defer atomic.StoreInt32(&p.refreshing, 0)
				timeout := 250 * time.Millisecond
				if p.cfg.TimeoutMs > 0 {
					timeout = time.Duration(p.cfg.TimeoutMs) * time.Millisecond
				}
				bgCtx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if _, err := p.fetchAndRender(bgCtx); err != nil {
					log.Printf("[Ambient] SWR background refresh warning: %v", err)
				}
			}()
		}
		return cached, nil
	}

	return p.fetchAndRender(ctx)
}
