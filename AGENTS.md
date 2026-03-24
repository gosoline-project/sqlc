# AGENTS.md

## Scope
- This file applies to the `github.com/gosoline-project/sqlc` repository.
- The module targets Go `1.24.0`.
- The primary library package is `sqlc`.
- `examples/` contains `package main` example programs.
- `mocks/` contains generated mockery output and should be treated as generated code.

## Rule Files Present In This Repo
- No `.cursorrules` file was found.
- No `.cursor/rules/` directory was found.
- No `.github/copilot-instructions.md` file was found.
- The only repository-local agent guidance file is this `AGENTS.md`.

## Repository Layout
- Root `*.go` files implement the public SQL client, query builders, parser, drivers, JSON helpers, and scan/bind support.
- `examples/` holds runnable usage samples and is excluded from local `golangci-lint` formatting/linting rules.
- `mocks/` holds generated `testify/mock` types.
- Tests mostly live in the repository root beside the code they cover.

## Build Commands
- Build everything: `go build ./...`
- Build only the root library package: `go build .`
- Build examples too: `go build ./...` already covers them.
- There is no repo-local `Makefile`, `Taskfile`, or `justfile`; use direct Go commands.

## Test Commands
- Run all tests: `go test -v ./...`
- Run only the root package tests: `go test -v .`
- Run a single test in the root package: `go test -v -run '^TestName$' .`
- Run a single test across all packages: `go test -v ./... -run '^TestName$'`
- Run a group of tests by prefix/regex: `go test -v -run 'TestNamedExec_' .`
- Re-run one package without test cache: `go test -count=1 -v .`
- Get coverage quickly: `go test -cover ./...`
- When changing query builders, parser, scan helpers, or bind logic, run the closest targeted test first and then `go test -v ./...`.

## Lint And Format Commands
- Format code: `gofumpt -w .`
- Check formatting without writing: `gofumpt -l .`
- Run linter from repo root: `golangci-lint run`
- The linter configuration is in `.golangci.yml`.
- Lint runs with build tags `integration` and `fixtures`.
- Lint uses `modules-download-mode: readonly`; do not casually change `go.mod` or `go.sum` during lint-only work.

## Linter Rules That Matter Most Here
- `gofumpt` formatting is enforced.
- `errcheck` is enabled and `check-blank: true`; do not ignore returned errors with `_`.
- `lll` is enabled with a max line length of `240`.
- `nolintlint` requires specific `nolint` directives and explanations.
- `revive` enforces `use-any`; prefer `any` over `interface{}`.
- `godox` flags `TODO`, `BUG`, and `FIXME` comments.
- `gocritic`, `govet`, `staticcheck`, and `unused` are enabled.
- Test files get a few relaxed rules (`dogsled`, `goconst`, `lll`).
- `examples/` is excluded from lint and formatter enforcement in `.golangci.yml`.

## Working Style For Agents
- Prefer small, surgical changes that match existing patterns.
- Keep public APIs stable unless the task explicitly asks for a breaking change.
- Preserve existing builder semantics, especially immutability and chaining behavior.
- Preserve generated files unless you are intentionally regenerating them.
- If you touch generated mocks manually, call that out clearly because those edits may be overwritten later.

## Package And File Conventions
- Production code uses `package sqlc`.
- Most black-box tests use `package sqlc_test` and import the module.
- A few tests stay in `package sqlc` when they need internal access; follow the nearest existing pattern.
- Example programs use `package main` under `examples/`.
- Group related declarations with `type (...)`, `const (...)`, or `var (...)` blocks when it improves readability.

## Imports
- Keep imports grouped in standard Go order: standard library, blank line, third-party, blank line, internal aliases if needed.
- Let `gofumpt` manage final import formatting.
- Use import aliases only when they remove ambiguity or avoid name collisions.
- In black-box tests, importing `github.com/gosoline-project/sqlc` directly is the common pattern.

## Formatting
- Run `gofumpt -w .` after substantive Go edits.
- Keep lines below the configured `240` character limit.
- Prefer the repository's existing whitespace style rather than adding custom alignment.
- Avoid trailing noise in comments and `nolint` directives.

## Types And API Design
- Prefer `any` over `interface{}`.
- The codebase uses generics where they improve type safety, especially builder wrappers such as `FromG[T]`, `IntoG[T]`, `UpdateG[T]`, and `DeleteG[T]`.
- Preserve existing public type names and method names unless the task requires otherwise.
- Small interfaces are common; use concise capability-oriented names.
- Type aliases and grouped type declarations are normal in this repository.

## Naming
- Exported identifiers use PascalCase.
- Unexported identifiers use camelCase.
- Test names follow Go conventions and often use `TestThing_Scenario`.
- Keep names explicit around SQL concepts: `QueryBuilder`, `Querier`, `Assignment`, `Placeholder`, `IdentifierQuote`, `WithClient`, `WithConfig`.
- Match existing terminology like `Records`, `ValuesRows`, `NamedExec`, `ToSql`, and `ToNamedSql` instead of inventing synonyms.

## Error Handling
- Always handle returned errors.
- Wrap errors with context using `fmt.Errorf("context: %w", err)` when adding useful call-site information.
- Preserve existing error wording patterns such as `can not ...`, `failed to ...`, or `could not ...` when extending nearby code.
- Do not hide rollback/commit failures; this repo explicitly reports transactional cleanup errors.
- Returning `nil, nil` is unusual and should only be done intentionally with a specific justification, as seen in `json.go`.

## Return Values
- Named return values are used in many complex functions and builder serialization helpers.
- Follow the local style when a function naturally builds up `query`, `params`, and `err` over multiple steps.
- For simple functions, regular unnamed returns are still fine.
- Keep return ordering consistent with existing builder methods: SQL string, params slice, error.

## Comments And Docs
- Exported types, functions, and methods should have doc comments.
- Start doc comments with the identifier name.
- Public builder APIs often include short usage examples in the doc comment; preserve that style for new exported APIs.
- Keep comments factual and implementation-focused.
- Do not add obvious comments for trivial code.

## Query Builder Conventions
- Query builders are intentionally immutable.
- Builder methods typically create a copied builder, modify the copy, and return it.
- Preserve the `copyQuery` pattern instead of mutating receivers in place.
- When copying builders, also copy slices or nested builder state as needed, not just top-level pointers.
- Preserve fluent chaining semantics for `WithClient`, `WithConfig`, `Where`, `Join`, `Columns`, `Values`, and related methods.

## SQL Generation Conventions
- Keep values parameterized rather than interpolated into SQL strings.
- Preserve placeholder ordering exactly.
- Use `QueryBuilderConfig` for placeholder and identifier-quote behavior.
- Quote identifiers consistently through the existing helper/config path rather than ad hoc string concatenation.
- Raw SQL expression support already exists through types like `Expression` and `Assignment`; reuse those patterns.

## Transactions And Context
- `Client.WithTx` and transaction wrappers preserve context-aware behavior; keep that intact.
- When extending transactional code, return the original functional error unless rollback itself fails and must be surfaced.
- `tx` types also implement context-like methods (`Deadline`, `Done`, `Err`, `Value`); do not break that behavior.

## Tests
- Prefer black-box tests in `package sqlc_test` unless internals must be accessed.
- Use `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require`.
- Use `require` for fatal preconditions and `assert` for follow-up checks.
- `go-sqlmock` is the common approach for SQL expectations.
- Use `regexp.QuoteMeta(...)` for expected SQL strings in sqlmock expectations.
- Reuse helpers like `newTestClientWithDriver` when possible.
- Clean up mock expectations with `t.Cleanup`.
- Table-driven tests and `t.Run(...)` are common and fit the existing style.

## Generated Code
- Files in `mocks/` start with `Code generated by mockery; DO NOT EDIT.`
- Avoid hand-editing generated mocks unless the task explicitly asks for it.
- If generated code must change, prefer regeneration over manual patching.

## Nolint Usage
- Keep `nolint` directives rare.
- When needed, make them specific, not blanket suppressions.
- Add the explanation required by `nolintlint`.
- Follow nearby examples like `//nolint:nilnil // this is the expected behaviour by the driver package`.

## Practical Defaults For Agents
- Use direct Go tooling from the repo root.
- Run the narrowest relevant test first.
- Finish with `gofumpt -w .` and `go test -v ./...` after meaningful code changes.
- Run `golangci-lint run` when the change touches shared/public code, error handling, or formatting-sensitive paths.
- Mention clearly if you could not run lint or full tests in your environment.
