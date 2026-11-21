package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Route/handler ordering check and -fix sorter, ported from mirrormere.
// Runs when -fix is set or the config enables check_ordering. -fix leaves
// runs that carry comments (between, beside or directly above items) alone
// and keeps reporting them, because comments are not part of an item's span
// and would be dropped or left behind by the sort.

type orderingItem struct {
	key     string
	start   int
	end     int
	line    int
	endLine int
}

type orderingRun struct {
	kind  string
	items []orderingItem
	sep   string
}

func isMuxRouteCall(stmt ast.Stmt, fset *token.FileSet) (orderingItem, bool) {
	exprStmt, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return orderingItem{}, false
	}
	call, ok := exprStmt.X.(*ast.CallExpr)
	if !ok {
		return orderingItem{}, false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (sel.Sel.Name != "Handle" && sel.Sel.Name != "HandleFunc") {
		return orderingItem{}, false
	}
	if len(call.Args) == 0 {
		return orderingItem{}, false
	}
	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return orderingItem{}, false
	}
	val, err := strconv.Unquote(lit.Value)
	if err != nil {
		val = lit.Value
	}
	return orderingItem{
		key:     val,
		start:   fset.Position(stmt.Pos()).Offset,
		end:     fset.Position(stmt.End()).Offset,
		line:    fset.Position(stmt.Pos()).Line,
		endLine: fset.Position(stmt.End()).Line,
	}, true
}

func isHandlerField(f *ast.Field, fset *token.FileSet) (orderingItem, bool) {
	if len(f.Names) == 0 {
		return orderingItem{}, false
	}
	name := f.Names[0].Name
	if !strings.HasSuffix(name, "Handler") {
		return orderingItem{}, false
	}
	return orderingItem{
		key:     name,
		start:   fset.Position(f.Pos()).Offset,
		end:     fset.Position(f.End()).Offset,
		line:    fset.Position(f.Pos()).Line,
		endLine: fset.Position(f.End()).Line,
	}, true
}

func isHandlerKeyValue(expr ast.Expr, fset *token.FileSet) (orderingItem, bool) {
	kv, ok := expr.(*ast.KeyValueExpr)
	if !ok {
		return orderingItem{}, false
	}
	ident, ok := kv.Key.(*ast.Ident)
	if !ok || !strings.HasSuffix(ident.Name, "Handler") {
		return orderingItem{}, false
	}
	return orderingItem{
		key:     ident.Name,
		start:   fset.Position(kv.Pos()).Offset,
		end:     fset.Position(kv.End()).Offset,
		line:    fset.Position(kv.Pos()).Line,
		endLine: fset.Position(kv.End()).Line,
	}, true
}

func collectStructHandlerRuns(f *ast.File, fset *token.FileSet) []orderingRun {
	var runs []orderingRun
	ast.Inspect(f, func(n ast.Node) bool {
		st, ok := n.(*ast.StructType)
		if !ok || st.Fields == nil {
			return true
		}
		var cur []orderingItem
		for _, field := range st.Fields.List {
			item, ok := isHandlerField(field, fset)
			if !ok {
				if len(cur) >= 2 {
					runs = append(runs, orderingRun{kind: "handler field", items: cur, sep: "\n\t"})
				}
				cur = nil
				continue
			}
			cur = append(cur, item)
		}
		if len(cur) >= 2 {
			runs = append(runs, orderingRun{kind: "handler field", items: cur, sep: "\n\t"})
		}
		return true
	})
	return runs
}

func collectCompositeHandlerRuns(f *ast.File, fset *token.FileSet) []orderingRun {
	var runs []orderingRun
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		var cur []orderingItem
		for _, elt := range cl.Elts {
			item, ok := isHandlerKeyValue(elt, fset)
			if !ok {
				if len(cur) >= 2 {
					runs = append(runs, orderingRun{kind: "handler composite literal", items: cur, sep: ",\n\t"})
				}
				cur = nil
				continue
			}
			cur = append(cur, item)
		}
		if len(cur) >= 2 {
			runs = append(runs, orderingRun{kind: "handler composite literal", items: cur, sep: ",\n\t"})
		}
		return true
	})
	return runs
}

func collectRouteRuns(f *ast.File, fset *token.FileSet) []orderingRun {
	var runs []orderingRun
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "setupRoutes" || fn.Body == nil {
			continue
		}
		var cur []orderingItem
		for _, stmt := range fn.Body.List {
			item, ok := isMuxRouteCall(stmt, fset)
			if !ok {
				if len(cur) >= 2 {
					runs = append(runs, orderingRun{kind: "route statement", items: cur, sep: "\n\t"})
				}
				cur = nil
				continue
			}
			cur = append(cur, item)
		}
		if len(cur) >= 2 {
			runs = append(runs, orderingRun{kind: "route statement", items: cur, sep: "\n\t"})
		}
	}
	return runs
}

func collectFileOrderingRuns(f *ast.File, fset *token.FileSet) []orderingRun {
	structRuns := collectStructHandlerRuns(f, fset)
	compositeRuns := collectCompositeHandlerRuns(f, fset)
	routeRuns := collectRouteRuns(f, fset)
	runs := make([]orderingRun, 0, len(structRuns)+len(compositeRuns)+len(routeRuns))
	runs = append(runs, structRuns...)
	runs = append(runs, compositeRuns...)
	runs = append(runs, routeRuns...)
	return runs
}

func findUnsortedRuns(runs []orderingRun, rel string) (violations []string, unsorted []orderingRun) {
	for _, r := range runs {
		var isUnsorted bool
		for i := 1; i < len(r.items); i++ {
			if r.items[i].key < r.items[i-1].key {
				isUnsorted = true
				break
			}
		}
		if isUnsorted {
			violations = append(violations, fmt.Sprintf("%s:%d: contiguous %s run is unsorted; run scripts/code-health with -fix to auto-sort", rel, r.items[0].line, r.kind))
			unsorted = append(unsorted, r)
		}
	}
	return violations, unsorted
}

func applyOrderingFixes(src []byte, runs []orderingRun) ([]byte, error) {
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].items[0].start > runs[j].items[0].start
	})
	curSrc := src
	for _, r := range runs {
		first := r.items[0]
		last := r.items[len(r.items)-1]
		sortedItems := make([]orderingItem, len(r.items))
		copy(sortedItems, r.items)
		sort.SliceStable(sortedItems, func(a, b int) bool {
			return sortedItems[a].key < sortedItems[b].key
		})
		sortedPieces := make([]string, 0, len(sortedItems))
		for _, item := range sortedItems {
			sortedPieces = append(sortedPieces, string(curSrc[item.start:item.end]))
		}
		joined := strings.Join(sortedPieces, r.sep)
		newSrc := append([]byte{}, curSrc[:first.start]...)
		newSrc = append(newSrc, []byte(joined)...)
		newSrc = append(newSrc, curSrc[last.end:]...)
		curSrc = newSrc
	}
	return format.Source(curSrc)
}

func checkOrFixFileOrdering(fullPath, rel string, fix bool) ([]string, error) {
	src, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, err
	}
	if isGeneratedSource(src) {
		return nil, nil
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, fullPath, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	runs := collectFileOrderingRuns(f, fset)
	if len(runs) == 0 {
		return nil, nil
	}

	violations, unsorted := findUnsortedRuns(runs, rel)
	if !fix || len(unsorted) == 0 {
		return violations, nil
	}

	var fixable []orderingRun
	violations = nil
	for _, r := range unsorted {
		if runHasComments(r, f, fset) {
			violations = append(violations, fmt.Sprintf("%s:%d: contiguous %s run is unsorted and carries comments; -fix skips it, sort it by hand", rel, r.items[0].line, r.kind))
			continue
		}
		fixable = append(fixable, r)
	}
	if len(fixable) == 0 {
		return violations, nil
	}

	formatted, err := applyOrderingFixes(src, fixable)
	if err != nil {
		return nil, fmt.Errorf("format %s after sorting: %w", rel, err)
	}

	if !bytes.Equal(formatted, src) {
		if err := os.WriteFile(fullPath, formatted, 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", rel, err)
		}
	}

	return violations, nil
}

// runHasComments reports whether any comment sits on a line from the one
// directly above the run's first item through the run's last line.
func runHasComments(r orderingRun, f *ast.File, fset *token.FileSet) bool {
	from := r.items[0].line - 1
	to := r.items[len(r.items)-1].endLine
	for _, cg := range f.Comments {
		if fset.Position(cg.End()).Line >= from && fset.Position(cg.Pos()).Line <= to {
			return true
		}
	}
	return false
}
