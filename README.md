# dbos-rust

Rust SDK surface for DBOS durable workflows.

## Features

- Durable workflow registration, execution, recovery, cancellation, and inspection.
- Durable steps, queues, schedules, messages, events, streams, and debouncing.
- System database backends for Postgres and Turso via feature gates.
- Admin and Conductor management surfaces behind optional features.
- `fast-telemetry` metrics, spans, snapshots, and Prometheus export are enabled by default.

## Crate Features

- `postgres` - Postgres system database backend. Enabled by default.
- `turso` - Turso system database backend.
- `admin` - Built-in admin HTTP server.
- `conductor` - DBOS Conductor WebSocket integration.
- `full` - Enables all optional public features.

## Development

```bash
cargo fmt --check
cargo check --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Chaos tests are opt-in because they require an external Postgres instance and restart commands. See [`docs/chaos-tests.md`](docs/chaos-tests.md).

## License

MIT
