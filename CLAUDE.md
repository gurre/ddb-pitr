# CLAUDE.md

## Golang

- Replace code as needed to create a elegant implementation. No backwards compatibility is required.
- Make sure tests and linter passes, otherwise fix the tests. Run tests with -race flag.
- Use Dependency inversion principle and declare local interfaces for dependencies.
- Comment code based on behavior and design constraints, exported methods should have usage examples.
- Consider service boundries and separation of concern. Packages must not have leaky abstractions.
- Add comments to all tests. Explain what it does and motivate why the test should exist.
- Lint and test code before calling done.
  - golangci-lint run --config ./.golangci.yml ./...
  - go test -memprofile=mem.out ./... && go tool pprof -top -alloc_objects mem.out
- Identify hot paths and create selected benchmarks.
- Profile memory with `go test -bench=. -memprofile=mem.out && go tool pprof -top -alloc_objects mem.out`.
- Clean-up once done, remove unused and temporary code, if you have started background processes make sure they are terminated.
- Check package local documentation for drift. Remove irrelevant, overly verbose and code blobs doom to become outdated from docs.
- Always align Go structs. It's free to implement and often leads to better memory efficiency without changing any logic—only field order needs to be adjusted. https://goperf.dev/01-common-patterns/fields-alignment/
- Use pointers when assigning to Go interfaces.
- Use `go doc` cli to research Go libraries and their API.
- Avoid returning nil values from functions, instead return empty structs or slices.
- Prefer duplicated code over convoluted DRY refactoring.
- Amundsen's Maxim states that when designing a Web API you must treat your data model, object model, resource model and message model as distinct layers.

## Performance Optimization

Focus on reducing allocations in hot paths: use `sync.Pool` for short-lived objects, preallocate slices/maps when size is known, and avoid interface boxing of large structs.
Always align struct fields from largest to smallest for better memory efficiency. For networking, drain HTTP response bodies, set reasonable timeouts, and use context for cancellation.
Batch operations when possible and use buffered I/O for repeated small writes. Avoid premature optimization—measure first with benchmarks and profiling, then optimize based on data.

## Tests

- IMPORTANT: ALWAYS test the actual implementation through imports. NEVER duplicate the implementation

Use this checklist when implementing tests:

- Each test has a clear, descriptive name describing the behavior
- Each test verifies one specific behavior
- Tests only public interfaces (no unexported fields/methods)
- No tests for standard library behavior (JSON marshaling, etc.)
- No tests for unexported helper functions
- Minimal assertions per test (ideally 1-2)
- Common setup extracted to helper functions marked with `t.Helper()`
- Test mocks/doubles moved to end of file or separate file
- All tests still pass after refactoring

### Rules

- Run integration tests using integration/run.sh.
- Dont edit integration/main_test.go if not directly asked to.
- Never use mocks in integration tests
- Use integration/run.sh to run integration tests
- Always print CLI commands on a single line, avoid \\n.
- Do a code review of the uncommited changes, make sure changes align with the intended goal, only first then make a git commit using git cli. Use max 3 lines for the message and don't mention claude code or cursor.
- Read README.md
- Use go doc to get a glimpse of the codebase, prefer it over grep, eg `go doc ./internal/event` or `go doc ./internal/flow/batch`.
- When making changes to postgres schemas use the migration scripts in postgres/.
