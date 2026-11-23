package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// copyFixture copies testdata/<fixture> (nothing when fixture is empty) into
// a temp dir and writes overlay files on top.
func copyFixture(t *testing.T, fixture string, overlay map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if fixture != "" {
		src := filepath.Join("testdata", fixture)
		err := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			rel, err := filepath.Rel(src, p)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			return writeFile(filepath.Join(root, rel), string(data))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for rel, content := range overlay {
		if err := writeFile(filepath.Join(root, filepath.FromSlash(rel)), content); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func writeFile(p, content string) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(content), 0o644)
}

func sel(all bool, names ...string) gate {
	if names == nil {
		names = []string{}
	}
	return gate{All: all, Selected: names}
}

var (
	allImages  = sel(true, "app", "tool", "static")
	allModules = sel(true, "app", "lib", "tool", "tool/nested")
	noImages   = sel(false)
	noModules  = sel(false)
)

var (
	repoCatalog = []image{
		{Service: "app", Select: selectRule{GoRoots: []string{"app", "lib/cli"}, Inputs: []string{"app/**", "web/**"}}},
		{Service: "tool", Select: selectRule{GoRoots: []string{"tool"}, Inputs: []string{"tool/**"}}},
		{Service: "static", Select: selectRule{Inputs: []string{"static/**", "web/style.css"}}},
	}
	rootCatalog       = []image{{Service: "root", Select: selectRule{GoRoots: []string{"."}, Inputs: []string{"Dockerfile"}}}}
	nestedRootCatalog = []image{{Service: "nested", Select: selectRule{GoRoots: []string{"services/nested"}, Inputs: []string{"services/nested/**"}}}}
	staticOnly        = []image{{Service: "static", Select: selectRule{Inputs: []string{"static/**"}}}}
	badRoot           = []image{{Service: "bad", Select: selectRule{GoRoots: []string{"nowhere"}}}}
)

// selectAffectedCase is one TestSelectAffected row.
type selectAffectedCase struct {
	name     string
	fixture  string
	overlay  map[string]string
	subdir   string // run against root/subdir (a missing dir exercises scan errors)
	catalog  []image
	changed  []string
	forceAll bool
	want     result
}

// selectAffectedCases lives at package level to keep TestSelectAffected under funlen.
var selectAffectedCases = []selectAffectedCase{
	{
		name: "shared package rebuilds only importers", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/text/text.go"},
		want: result{Images: sel(false, "app"), Test: sel(false, "app", "lib"), Lint: sel(false, "app", "lib"), Reasons: []string{}},
	},
	{
		name: "build-tagged import still counts", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/tagged/tagged_windows.go"},
		want: result{Images: sel(false, "app"), Test: sel(false, "app", "lib"), Lint: sel(false, "app", "lib"), Reasons: []string{}},
	},
	{
		name: "embed asset dir maps to its package", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/embedded/assets/page.txt"},
		want: result{Images: sel(false, "app"), Test: sel(false, "lib"), Lint: sel(false, "lib"), Reasons: []string{}},
	},
	{
		name: "file in sub-package also affects enclosing package", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/embedded/sub/data.txt"},
		want: result{Images: sel(false, "app"), Test: sel(false, "lib"), Lint: sel(false, "lib"), Reasons: []string{}},
	},
	{
		name: "external test import selects tests not images", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/onlytest/onlytest.go"},
		want: result{Images: noImages, Test: sel(false, "app", "lib"), Lint: sel(false, "app", "lib"), Reasons: []string{}},
	},
	{
		name: "test file runs only its own module", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/text/text_test.go"},
		want: result{Images: noImages, Test: sel(false, "lib"), Lint: sel(false, "lib"), Reasons: []string{}},
	},
	{
		name: "test file in image input dir selects no image", fixture: "repo", catalog: repoCatalog, changed: []string{"app/x_test.go"},
		want: result{Images: noImages, Test: sel(false, "app"), Lint: sel(false, "app"), Reasons: []string{}},
	},
	{
		name: "testdata file selects no image", fixture: "repo", catalog: repoCatalog, changed: []string{"app/testdata/f.json"},
		want: result{Images: noImages, Test: sel(false, "app"), Lint: sel(false, "app"), Reasons: []string{}},
	},
	{
		name: "js test files select no image", fixture: "repo", catalog: repoCatalog, changed: []string{"static/a.test.ts", "web/__tests__/b.js"},
		want: result{Images: noImages, Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "selection tool test file selects nothing globally", fixture: "repo", catalog: repoCatalog, changed: []string{"scripts/affected/x_test.go"},
		want: result{Images: noImages, Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "test file beside prod file still selects image", fixture: "repo", catalog: repoCatalog, changed: []string{"app/x_test.go", "app/main.go"},
		want: result{Images: sel(false, "app"), Test: sel(false, "app"), Lint: sel(false, "app"), Reasons: []string{}},
	},
	{
		name: "prod embed of testdata selects all on test-only change", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/text/testdata/a.json"},
		overlay: map[string]string{"lib/text/embed.go": "package text\n\nimport \"embed\"\n\n//go:embed testdata\nvar FS embed.FS\n"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{
			"non-test file embeds testdata, test-only changes may reach a binary: lib/text/embed.go",
		}},
	},
	{
		name: "testutil embed of testdata is exempt", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/testutil/testdata/a.json"},
		overlay: map[string]string{"lib/testutil/testutil.go": "package testutil\n\nimport \"embed\"\n\n//go:embed testdata\nvar FS embed.FS\n"},
		want:    result{Images: noImages, Test: sel(false, "lib"), Lint: sel(false, "lib"), Reasons: []string{}},
	},
	{
		name: "prod embed of testdata ignored without test-only change", fixture: "repo", catalog: repoCatalog, changed: []string{"web/style.css"},
		overlay: map[string]string{"lib/text/embed.go": "package text\n\nimport \"embed\"\n\n//go:embed testdata\nvar FS embed.FS\n"},
		want:    result{Images: sel(false, "app", "static"), Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "go.sum affects every package of its module", fixture: "repo", catalog: repoCatalog, changed: []string{"lib/go.sum"},
		want: result{Images: sel(false, "app"), Test: sel(false, "app", "lib"), Lint: sel(false, "app", "lib"), Reasons: []string{}},
	},
	{
		name: "explicit non-Go input", fixture: "repo", catalog: repoCatalog, changed: []string{"web/style.css"},
		want: result{Images: sel(false, "app", "static"), Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "own dir glob and own module", fixture: "repo", catalog: repoCatalog, changed: []string{"tool/README.md"},
		want: result{Images: sel(false, "tool"), Test: sel(false, "tool"), Lint: sel(false, "tool"), Reasons: []string{}},
	},
	{
		name: "unimported helper package in a go root module", fixture: "repo", catalog: repoCatalog, changed: []string{"app/internal/spare/spare.go"},
		overlay: map[string]string{"app/internal/spare/spare.go": "package spare\n"},
		want:    result{Images: noImages, Test: sel(false, "app"), Lint: sel(false, "app"), Reasons: []string{}},
	},
	{
		name: "nested module is not its parent", fixture: "repo", catalog: repoCatalog, changed: []string{"tool/nested/nested.go"},
		want: result{Images: sel(false, "tool"), Test: sel(false, "tool", "tool/nested"), Lint: sel(false, "tool", "tool/nested"), Reasons: []string{}},
	},
	{
		name: "code outside every module selects nothing", fixture: "repo", catalog: repoCatalog, changed: []string{"deep/a/b/deep.go", "lib/testdata/fixture.go"},
		want: result{Images: noImages, Test: sel(false, "lib"), Lint: sel(false, "lib"), Reasons: []string{}},
	},
	{
		name: "pkg output tracks pkg paths", fixture: "repo", catalog: repoCatalog, changed: []string{"pkg/README.md"},
		want: result{Pkg: true, Images: noImages, Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "coverage script selects every test module", fixture: "repo", catalog: repoCatalog, changed: []string{"scripts/check-coverage.sh"},
		want: result{Images: noImages, Test: allModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "lint config selects every lint module", fixture: "repo", catalog: repoCatalog, changed: []string{".golangci.yml"},
		want: result{Images: noImages, Test: noModules, Lint: allModules, Reasons: []string{}},
	},
	{
		name: "force all", fixture: "repo", catalog: repoCatalog, changed: []string{"web/style.css"}, forceAll: true,
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"force_all requested"}},
	},
	{
		name: "empty changed list", fixture: "repo", catalog: repoCatalog,
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"empty changed-file list"}},
	},
	{
		name: "workflow change", fixture: "repo", catalog: repoCatalog, changed: []string{".github/workflows/docker-publish.yml"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"workflow changed: .github/workflows/docker-publish.yml"}},
	},
	{
		name: "dockerignore change", fixture: "repo", catalog: repoCatalog, changed: []string{".dockerignore"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"root .dockerignore changed: .dockerignore"}},
	},
	{
		name: "tool change", fixture: "repo", catalog: repoCatalog, changed: []string{"scripts/affected/catalog.json"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"selection tool changed: scripts/affected/catalog.json"}},
	},
	{
		name: "workspace file", fixture: "repo", catalog: repoCatalog, changed: []string{"go.work"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"workspace file changed: go.work"}},
	},
	{
		name: "go.mod of an undiscovered module", fixture: "repo", catalog: repoCatalog, changed: []string{"deep/a/b/go.mod"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"module file outside a known module: deep/a/b/go.mod"}},
	},
	{
		name: "unresolvable repo imports", fixture: "repo", catalog: repoCatalog, changed: []string{"web/style.css"},
		overlay: map[string]string{"app/internal/core/core.go": "package core\n\nimport (\n\t_ \"example.com/fx/app/missing\"\n\t_ \"example.com/fx/ghost\"\n)\n\nconst Name = \"core\"\n"},
		want: result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{
			"unresolvable import app/internal/core: example.com/fx/app/missing",
			"unresolvable import app/internal/core: example.com/fx/ghost",
		}},
	},
	{
		name: "parse error", fixture: "repo", catalog: repoCatalog, changed: []string{"web/style.css"},
		overlay: map[string]string{"app/bad.go": "package main\n\nimport (\n"},
		want:    result{Images: allImages, Test: allModules, Lint: allModules, Reasons: []string{"parse app/bad.go: app/bad.go:3:10: expected ')', found 'EOF'"}},
	},
	{
		name: "unknown go root", fixture: "repo", catalog: badRoot, changed: []string{"web/style.css"},
		want: result{Images: sel(true, "bad"), Test: allModules, Lint: allModules, Reasons: []string{`catalog bad: go root "nowhere" is neither a module nor a package`}},
	},
	{
		name: "missing root", subdir: "missing", catalog: staticOnly, changed: []string{"static/a"},
		want: result{Images: sel(true, "static"), Test: sel(true), Lint: sel(true), Reasons: []string{"lstat $ROOT/missing: no such file or directory"}},
	},
	{
		name: "go.mod without module directive", overlay: map[string]string{"m/go.mod": "go 1.27.3\n"}, catalog: staticOnly, changed: []string{"static/a"},
		want: result{Images: sel(true, "static"), Test: sel(true), Lint: sel(true), Reasons: []string{"$ROOT/m/go.mod: no module directive"}},
	},
	{
		name: "repo without modules", overlay: map[string]string{"static/index.html": ""}, catalog: staticOnly, changed: []string{"static/index.html"},
		want: result{Images: sel(false, "static"), Test: noModules, Lint: noModules, Reasons: []string{}},
	},
	{
		name: "root module", fixture: "rootmod", catalog: rootCatalog, changed: []string{"internal/x/x.go"},
		want: result{Images: sel(false, "root"), Test: sel(false, "."), Lint: sel(false, "."), Reasons: []string{}},
	},
	{
		name: "root module non-Go file", fixture: "rootmod", catalog: rootCatalog, changed: []string{"README.md"},
		want: result{Images: sel(false, "root"), Test: sel(false, "."), Lint: sel(false, "."), Reasons: []string{}},
	},
	{
		name: "root module nested package root fallback", fixture: "rootmod", catalog: nestedRootCatalog,
		overlay: map[string]string{
			"services/nested/cmd/nested/main.go": "package main\n\nimport \"example.com/rootmod/internal/x\"\n\nfunc main() { _ = x.X }\n",
		},
		changed: []string{"internal/x/x.go"},
		want:    result{Images: sel(false, "nested"), Test: sel(false, "."), Lint: sel(false, "."), Reasons: []string{}},
	},
}

func TestSelectAffected(t *testing.T) {
	t.Parallel()
	for _, tc := range selectAffectedCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := copyFixture(t, tc.fixture, tc.overlay)
			want := tc.want
			want.Reasons = append([]string{}, tc.want.Reasons...)
			for i, r := range want.Reasons {
				want.Reasons[i] = strings.ReplaceAll(r, "$ROOT", root)
			}
			got := selectAffected(filepath.Join(root, tc.subdir), tc.catalog, tc.changed, tc.forceAll)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("selectAffected() =\n%+v\nwant\n%+v", got, want)
			}
		})
	}
}

func TestParseGoMod(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		files       map[string]string
		dir         string
		wantPath    string
		wantReplace map[string]string
		wantErr     string
	}{
		{
			name: "single-line and block replaces", dir: "services/a",
			files:       map[string]string{"go.mod": "module \"example.com/a\" // quoted\n\nreplace example.com/pkg => ../../pkg\nreplace (\n\texample.com/b v1.0.0 => ../b v1.0.0\n\texample.com/c => example.com/c v1.2.3\n\texample.com/here => .\n\tbroken line\n)\nreplace => ./nothing\n"},
			wantPath:    "example.com/a",
			wantReplace: map[string]string{"example.com/pkg": "pkg", "example.com/b": "services/b", "example.com/here": "services/a"},
		},
		{name: "missing module directive", dir: ".", files: map[string]string{"go.mod": "go 1.27.3\n"}, wantErr: "no module directive"},
		{name: "go.mod is a directory", dir: ".", files: map[string]string{"go.mod/x": ""}, wantErr: "is a directory"},
		{name: "go.mod missing", dir: ".", files: map[string]string{}, wantErr: "no such file or directory"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := copyFixture(t, "", tc.files)
			m, err := parseGoMod(filepath.Join(root, "go.mod"), tc.dir)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("parseGoMod() error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if m.path != tc.wantPath || !reflect.DeepEqual(m.replace, tc.wantReplace) {
				t.Errorf("parseGoMod() = %q %v, want %q %v", m.path, m.replace, tc.wantPath, tc.wantReplace)
			}
		})
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }

func TestRun(t *testing.T) {
	t.Parallel()
	repoOutput := "pkg=false\napp=true\ntool=false\nstatic=true\nimages=[\"app\",\"static\"]\n"
	tests := []struct {
		name             string
		overlay          map[string]string
		args             []string
		stdin            string
		stdout           io.Writer
		wantCode         int
		wantOut          []string
		wantErr          string
		wantGitHubOutput string
	}{
		{
			name: "selection with github output", args: []string{"-catalog", "$ROOT/catalog.json", "-github-output", "$ROOT/out.txt"}, stdin: "./web/style.css\r\n\n",
			wantOut: []string{`"selected": [` + "\n      \"app\",\n      \"static\"\n    ]"}, wantGitHubOutput: repoOutput,
		},
		{
			name: "default catalog path", overlay: map[string]string{"scripts/affected/catalog.json": `[{"service":"tool","select":{"go_roots":["tool"]}}]`}, stdin: "tool/main.go\n",
			wantOut: []string{"\"images\": {\n    \"all\": false,\n    \"selected\": [\n      \"tool\"\n    ]"},
		},
		{name: "force all flag", args: []string{"-catalog", "$ROOT/catalog.json", "-force-all"}, stdin: "web/style.css\n", wantOut: []string{`"force_all requested"`}},
		{
			name: "graph", args: []string{"-catalog", "$ROOT/catalog.json", "-graph"},
			wantOut: []string{"\"tool\": [\n      \"tool\"\n    ]", "\"app\": [\n      \"app\",\n      \"app/internal/core\",\n      \"lib/onlytest\",\n      \"lib/tagged\",\n      \"lib/text\"\n    ]"},
		},
		{
			name: "graph on unparsable repo", overlay: map[string]string{"tool/bad.go": "package"}, args: []string{"-catalog", "$ROOT/catalog.json", "-graph"},
			wantCode: 1, wantErr: "parse tool/bad.go",
		},
		{name: "unknown flag", args: []string{"-bogus"}, wantCode: 2, wantErr: "flag provided but not defined: -bogus"},
		{name: "missing default catalog", wantCode: 1, wantErr: "scripts/affected/catalog.json: no such file or directory"},
		{name: "invalid catalog JSON", overlay: map[string]string{"bad.json": "{"}, args: []string{"-catalog", "$ROOT/bad.json"}, wantCode: 1, wantErr: "parse $ROOT/bad.json"},
		{
			name: "unsupported glob", overlay: map[string]string{"bad.json": `[{"service":"x","select":{"inputs":["a/*.go"]}}]`}, args: []string{"-catalog", "$ROOT/bad.json"},
			wantCode: 1, wantErr: `x: input "a/*.go": only exact paths and dir/** are supported`,
		},
		{name: "changed line too long", args: []string{"-catalog", "$ROOT/catalog.json"}, stdin: strings.Repeat("a", 70000), wantCode: 1, wantErr: "token too long"},
		{
			name: "github output not writable", args: []string{"-catalog", "$ROOT/catalog.json", "-github-output", "$ROOT/no/such/dir/out"}, stdin: "web/style.css\n",
			wantCode: 1, wantErr: "no such file or directory",
		},
		{name: "stdout write failure", args: []string{"-catalog", "$ROOT/catalog.json"}, stdin: "web/style.css\n", stdout: failWriter{}, wantCode: 1, wantErr: "write failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := copyFixture(t, "repo", tc.overlay)
			args := []string{"-root", root}
			for _, a := range tc.args {
				args = append(args, strings.ReplaceAll(a, "$ROOT", root))
			}
			var out, errOut bytes.Buffer
			stdout := tc.stdout
			if stdout == nil {
				stdout = &out
			}
			code := run(args, strings.NewReader(tc.stdin), stdout, &errOut)
			if code != tc.wantCode {
				t.Fatalf("run() = %d, want %d (stderr %q)", code, tc.wantCode, errOut.String())
			}
			for _, w := range tc.wantOut {
				if !strings.Contains(out.String(), w) {
					t.Errorf("stdout missing %q:\n%s", w, out.String())
				}
			}
			if wantErr := strings.ReplaceAll(tc.wantErr, "$ROOT", root); !strings.Contains(errOut.String(), wantErr) {
				t.Errorf("stderr = %q, want it to contain %q", errOut.String(), wantErr)
			}
			if tc.wantGitHubOutput != "" {
				got, err := os.ReadFile(filepath.Join(root, "out.txt"))
				if err != nil || string(got) != tc.wantGitHubOutput {
					t.Errorf("github output = %q (%v), want %q", got, err, tc.wantGitHubOutput)
				}
			}
		})
	}
}
