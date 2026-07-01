# DBOS Rust Chaos Tests

`tests/chaos.rs` ports the Go SDK chaos suite from `chaos_tests/chaos_test.go`.
The tests are ignored by default because they intentionally stop and start a
Postgres instance while workflows are running.

Run them explicitly:

```sh
DBOS_CHAOS_DATABASE_URL='postgres://postgres:dbos@localhost:5432/dbos?sslmode=disable' \
DBOS_CHAOS_POSTGRES_STOP_CMD='dbos postgres stop' \
DBOS_CHAOS_POSTGRES_START_CMD='dbos postgres start' \
cargo test -p dbos-rust --all-features --test chaos -- --ignored --test-threads=1
```

Useful overrides:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `DBOS_CHAOS_WORKFLOW_COUNT` | `10000` | Step workflow iterations |
| `DBOS_CHAOS_RECV_COUNT` | `10000` | Send/recv workflow iterations |
| `DBOS_CHAOS_EVENT_COUNT` | `5000` | Event workflow iterations |
| `DBOS_CHAOS_QUEUE_COUNT` | `30` | Queue workflow iterations |
| `DBOS_CHAOS_DOWN_MAX_MS` | `2000` | Maximum Postgres downtime per cycle |
| `DBOS_CHAOS_UP_MIN_MS` | `5000` | Minimum uptime before the next stop |
| `DBOS_CHAOS_UP_MAX_MS` | `40000` | Maximum uptime before the next stop |
| `DBOS_POSTGRES_RECONNECT_TIMEOUT_MS` | `5000` | Store reconnect deadline per operation |

For a non-destructive smoke run against Postgres without restarts, set
`DBOS_CHAOS_NO_RESTARTS=1` and omit the start/stop commands.
