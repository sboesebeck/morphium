# Test Runner (`runtests.sh`)

Morphium ships with a repository-local test runner script `./runtests.sh` that makes it easier to:

- run the suite with different drivers (`pooled`, `single`, `inmem`)
- run against a real MongoDB via `--uri`
- run in parallel (`--parallel N`) with per-slot database isolation
- rerun only failing tests (`--rerunfailed`)

## Common Commands

### Real MongoDB (pooled driver)

Run the full suite against an external replica set:

```bash
./runtests.sh --driver pooled --uri mongodb://mongo1.example,mongo2.example/morphium_tests --exclude-tags server,inmemory --parallel 2 --restart
```

Notes:
- Use `--restart` to wipe `test.log/` and start fresh.
- Use `--exclude-tags server` to skip MorphiumServer-specific tests when you only want MongoDB.
- Use `--exclude-tags inmemory` if you want to avoid in-memory-only suites.

### InMemoryDriver (fast local)

```bash
./runtests.sh --driver inmem --parallel 4 --restart
```

### Rerun only failed tests

```bash
./runtests.sh --rerunfailed --parallel 2
```

## Local MorphiumServer Cluster

If you have a MorphiumServer replica set running locally on `localhost:27017,27018,27019`, use:

```bash
./runtests.sh --morphiumserver-local --parallel 2 --restart
```

This mode sets:
- `--driver pooled`
- `--uri mongodb://localhost:27017,localhost:27018,localhost:27019/morphium_tests`

If you want to skip specific categories (e.g. if you’re testing “MongoDB compatibility” only), add excludes explicitly:

```bash
./runtests.sh --morphiumserver-local --exclude-tags server --parallel 2 --restart
```

## Debugging Failures

- Aggregated logs are written to `test.log/<fully.qualified.Test>.log`.
- Slot logs are written to `test.log/slot_<N>/` during parallel runs.
- Use `./runtests.sh --stats` for a live overview (slots running, failures, progress).

## Parallel Execution Details

With `--parallel N`, the runner assigns each slot its own database prefix via `-Dmorphium.database=morphium_test_<slot>`.
This prevents different JVMs from dropping or overwriting each other’s data during heavy parallel suites.

If you run tests manually with Maven, it’s recommended to set an explicit DB as well:

```bash
mvn -Pexternal -Dmorphium.driver=pooled -Dmorphium.uri=mongodb://... -Dmorphium.database=morphium_test_manual test
```
