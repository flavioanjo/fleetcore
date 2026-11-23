package gateway

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/flavioanjo/fleetcore/"

// productionImports returns the module-internal imports of the non-test Go
// files in dir.
func productionImports(t *testing.T, dir string) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no Go files in %s (%v)", dir, err)
	}
	out := map[string]string{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, src, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range af.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if strings.HasPrefix(path, module) {
				out[strings.TrimPrefix(path, module)] = f
			}
		}
	}
	return out
}

// TestIngestionIsDecoupledFromFusion guards the boundary between the
// ingestion pipeline and the fusion engine (docs/integration.md): ingestion
// packages may use only the domain, the protocol and each other, and fusion
// production code must not import any ingestion package.
func TestIngestionIsDecoupledFromFusion(t *testing.T) {
	allowed := map[string]bool{
		"internal/telemetry": true, "internal/protocol": true,
		"internal/eventstore": true, "internal/sessions": true,
		"internal/sqlitedb": true, // shared SQLite configuration (no domain logic)
		// Protocol v2 (docs/protocol-v2.md) and its cryptographic primitives.
		"internal/reliable": true, "internal/security": true,
	}
	for _, dir := range []string{".", "../eventstore", "../sessions", "../../cmd/fleet-gateway"} {
		for imp, file := range productionImports(t, dir) {
			if !allowed[imp] && imp != "internal/gateway" {
				t.Errorf("%s imports %s; ingestion may only import %v", file, imp, allowed)
			}
		}
	}
	// The protocol packages themselves depend only on the domain, the v1
	// codec and each other, so allowing them above opens no indirect path
	// from ingestion to fusion or processing.
	protocolOnly := map[string]bool{"internal/telemetry": true, "internal/protocol": true, "internal/security": true}
	for _, dir := range []string{"../reliable", "../security"} {
		for imp, file := range productionImports(t, dir) {
			if !protocolOnly[imp] {
				t.Errorf("%s imports %s; protocol packages may only import %v", file, imp, protocolOnly)
			}
		}
	}
	for imp, file := range productionImports(t, "../fusion") {
		switch imp {
		case "internal/gateway", "internal/eventstore", "internal/sessions":
			t.Errorf("%s imports ingestion package %s", file, imp)
		}
	}
}
