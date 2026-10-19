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

PostgreSQL retains its existing transport behavior by default. Set
`DbosConfig::with_postgres_ca_file(path)` or use
`SystemDatabaseHandle::postgres_with_ca` to require TLS with the standard rustls
certificate-chain, expiry and hostname checks. The URL must name exactly one
certificate hostname. Explicit trust overrides URL `sslmode=disable`/`prefer`
with `require`; missing or invalid trust, a wrong hostname, an untrusted signer
and plaintext refusal fail closed. The CA must be a bounded regular file owned
by root or the service and not writable by group/others. The immutable profile
is reused on every reconnect; no custom accept-any verifier or plaintext fallback
is provided. This authenticates the database server, not a workflow or tenant.

`infra-tests` enables the PostgreSQL TLS fixtures, which require Docker. Run
`cargo test --features postgres,infra-tests postgres_transport` to exercise real
positive, reconnect, wrong-host, wrong-CA and downgrade cases. Compiling those
fixtures does not establish that they executed successfully.

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
