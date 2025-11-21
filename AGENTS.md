# Agent Guidelines for sqlg

## Build, Lint & Test Commands
- **Run all tests**: `go test -v ./...`
- **Run single test**: `go test -v -run TestName`
- **Build**: `go build ./...`
- **Format**: `gofumpt -w .` (stricter than gofmt)
- **Lint**: `golangci-lint run` (if available, see parent gosoline/.golangci.yml for rules)

## Code Style & Conventions
- **Package structure**: Main code in `sqlg` package; tests use `package sqlg_test` with imports
- **Imports**: Standard library first, then third-party, then internal (separated by blank lines)
- **Formatting**: Use `gofumpt` (stricter than gofmt); max line length 240 chars
- **Types**: Prefer `any` over `interface{}` (revive: use-any rule)
- **Naming**: Exported types/functions use PascalCase; unexported use camelCase; interfaces often end in -er
- **Error handling**: Always check errors; wrap with `fmt.Errorf("context: %w", err)`; blank error checks not allowed
- **Struct tags**: Use backticks for tags (e.g., `cfg:"name" default:"value"`)
- **Type aliases**: Group at top of file in `type (...)` blocks
- **Immutability**: Query builder methods return new instances via shallow copy (see copyQuery pattern)
- **Fluent APIs**: Chain methods returning same type pointer for builder pattern
- **Function returns**: Name return parameters for complex functions (gocritic: unnamedResult)
- **Tests**: Use `package sqlg_test`; import `sqlg` package; use `github.com/stretchr/testify/assert` and `require` for assertions
- **Comments**: Exported items must have doc comments starting with the item name
