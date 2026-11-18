// Copyright IBM Corp. 2013, 2026
// SPDX-License-Identifier: MPL-2.0

package core

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestExecutionBoundary(t *testing.T) {
	allowed := map[string]bool{"bytes": true, "cmp": true, "encoding/json": true, "errors": true, "math": true, "math/rand/v2": true, "net/netip": true, "slices": true, "time": true, "github.com/modulecollective/lightpool/wire": true}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(".", entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range file.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if !allowed[path] {
				t.Errorf("boundary import: %s in %s", path, entry.Name())
			}
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.GoStmt, *ast.ChanType, *ast.SendStmt, *ast.SelectStmt:
				t.Errorf("execution facility %T in %s", v, entry.Name())
			case *ast.SelectorExpr:
				if id, ok := v.X.(*ast.Ident); ok {
					switch id.Name {
					case "time":
						if v.Sel.Name != "Duration" && v.Sel.Name != "Second" && v.Sel.Name != "Millisecond" {
							t.Errorf("ambient time: %s", v.Sel.Name)
						}
					case "rand":
						if v.Sel.Name != "New" && v.Sel.Name != "NewPCG" && v.Sel.Name != "Rand" {
							t.Errorf("global randomness: %s", v.Sel.Name)
						}
					case "wire":
						if v.Sel.Name == "Encode" || v.Sel.Name == "Decode" || v.Sel.Name == "Read" {
							t.Errorf("codec facility in core: %s", v.Sel.Name)
						}
					}
				}
			}
			return true
		})
	}
}
