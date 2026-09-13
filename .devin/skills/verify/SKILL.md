---
name: verify
description: Build the frontend, then compile, vet, and test the Go module
triggers:
  - user
  - model
allowed-tools:
  - exec
  - read
---

Run the full verification sequence for Hyrule-Hub. Use this before
committing or when asked to check that the codebase builds.

```bash
# 1. The Svelte UI must be built first — main.go does //go:embed ui/dist
#    and ui/dist is gitignored, so a clean checkout fails without this.
cd ui && npm ci && npm run build && cd ..

# 2. Compile everything (includes cmd/faken64).
go build ./...

# 3. Vet.
go vet ./...

# 4. Tests with the race detector — the bridge/hub are concurrent code.
go test -race -count=1 ./...
```

If `go build` fails with `pattern ui/dist: no matching files found`, the
frontend build was skipped — run step 1.

Report the result of each step and stop at the first failure.
