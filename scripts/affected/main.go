// Command affected selects the container images to build and the Go modules
// to test and lint for a changed-file list, from the repository's real Go
// import graph plus each image's explicit non-Go Docker inputs.
//
// Usage: git diff --name-only A B | affected -root ../.. [-force-all] [-github-output FILE]
//
// Every rule over-selects on doubt: an unparsable file, an unresolvable repo
// import, a workflow or tool change, or an empty list selects everything.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("affected", flag.ContinueOnError)
	fl.SetOutput(stderr)
	root := fl.String("root", ".", "repository root")
	catalogPath := fl.String("catalog", "", "image catalog JSON (default <root>/scripts/affected/catalog.json)")
	forceAll := fl.Bool("force-all", false, "select every image and module")
	githubOutput := fl.String("github-output", "", "append pkg, per-service and images outputs to this GITHUB_OUTPUT file")
	showGraph := fl.Bool("graph", false, "print each image's build closure and each module's test closure instead of a selection")
	if err := fl.Parse(args); err != nil {
		return 2
	}
	if *catalogPath == "" {
		*catalogPath = filepath.Join(*root, "scripts", "affected", "catalog.json")
	}
	cat, err := loadCatalog(*catalogPath)
	if err != nil {
		return fail(stderr, err)
	}
	if *showGraph {
		return printGraph(*root, cat, stdout, stderr)
	}
	changed, err := readChanged(stdin)
	if err != nil {
		return fail(stderr, err)
	}
	res := selectAffected(*root, cat, changed, *forceAll)
	if *githubOutput != "" {
		if err := writeGitHubOutput(*githubOutput, cat, res); err != nil {
			return fail(stderr, err)
		}
	}
	return writeJSON(stdout, stderr, res)
}

// readChanged reads one repo-relative path per line.
func readChanged(r io.Reader) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSuffix(sc.Text(), "\r")
		line = strings.TrimPrefix(line, "./")
		if strings.TrimSpace(line) != "" {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}

func writeGitHubOutput(file string, cat []image, res result) error {
	var b strings.Builder
	fmt.Fprintf(&b, "pkg=%t\n", res.Pkg)
	for _, img := range cat {
		fmt.Fprintf(&b, "%s=%t\n", img.Service, slices.Contains(res.Images.Selected, img.Service))
	}
	images, err := json.Marshal(res.Images.Selected)
	if err != nil {
		return err
	}
	fmt.Fprintf(&b, "images=%s\n", images)
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(b.String()); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

func printGraph(root string, cat []image, stdout, stderr io.Writer) int {
	r, err := loadRepo(root)
	var g *graph
	if err == nil {
		g, err = buildGraph(r, cat)
	}
	if err != nil {
		return fail(stderr, err)
	}
	out := struct {
		Images     map[string][]string `json:"images"`
		Modules    map[string][]string `json:"modules"`
		Unresolved []string            `json:"unresolved"`
	}{map[string][]string{}, map[string][]string{}, g.unresolved}
	for k, v := range g.images {
		out.Images[k] = sortedKeys(v)
	}
	for k, v := range g.modules {
		out.Modules[k] = sortedKeys(v)
	}
	return writeJSON(stdout, stderr, out)
}

func writeJSON(stdout, stderr io.Writer, v any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fail(stderr, err)
	}
	return 0
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func fail(stderr io.Writer, err error) int {
	if _, werr := io.WriteString(stderr, "affected: "+err.Error()+"\n"); werr != nil {
		return 1
	}
	return 1
}
