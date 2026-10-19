use super::*;

fn private_file(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("trusted fixture mode");
    }
    #[cfg(not(unix))]
    let _ = path;
}
fn profile(host: &str, port: u16) -> String {
    format!("host={host} hostaddr=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres sslmode=disable")
}

#[test]
fn explicit_trust_pins_required_tls_and_never_accepts_missing_or_ambiguous_authority() {
    let dir = tempfile::tempdir().expect("trust directory");
    let ca = dir.path().join("ca.pem");
    assert!(PostgresTransport::new(&profile("localhost", 5432), Some(&ca)).is_err());
    std::fs::write(&ca, "").expect("empty trust");
    private_file(&ca);
    assert!(PostgresTransport::new(&profile("localhost", 5432), Some(&ca)).is_err());
    std::fs::write(&ca, "-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----").expect("malformed trust");
    assert!(PostgresTransport::new(&profile("localhost", 5432), Some(&ca)).is_err());
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("synthetic CA");
    std::fs::write(&ca, cert.cert.pem()).expect("actual trust anchor");
    private_file(&ca);
    let transport = PostgresTransport::new(&profile("localhost", 5432), Some(&ca)).expect("verified profile");
    let PostgresTransport::Verified { config, .. } = transport else {
        panic!("explicit CA must never select legacy transport")
    };
    assert_eq!(config.get_ssl_mode(), SslMode::Require);
    assert!(PostgresTransport::new("host=first.example,second.example user=postgres", Some(&ca)).is_err());
    assert!(PostgresTransport::new("user=postgres", Some(&ca)).is_err());
    assert!(matches!(
        PostgresTransport::new(&profile("localhost", 5432), None).expect("unchanged default"),
        PostgresTransport::Legacy(_)
    ));
}

#[cfg(unix)]
#[test]
fn writable_and_symlinked_trust_cannot_authorize_a_profile() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().expect("trust directory");
    let ca = dir.path().join("ca.pem");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("synthetic CA");
    std::fs::write(&ca, cert.cert.pem()).expect("trust fixture");
    private_file(&ca);
    assert!(trust_roots(&ca).is_ok());
    std::fs::set_permissions(&ca, std::fs::Permissions::from_mode(0o666)).expect("untrusted mutable file");
    assert!(trust_roots(&ca).is_err());
    private_file(&ca);
    let link = dir.path().join("link");
    symlink(&ca, &link).expect("trust symlink");
    assert!(trust_roots(&link).is_err());
}

#[cfg(feature = "infra-tests")]
mod native {
    use super::*;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};

    async fn tls_server() -> (ContainerAsync<Postgres>, tempfile::TempDir, std::path::PathBuf) {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("TLS certificate");
        let dir = tempfile::tempdir().expect("trust directory");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&ca, certificate.cert.pem()).expect("trusted signer");
        private_file(&ca);
        let server = Postgres::default()
            .with_copy_to("/tmp/dbos-server.crt", certificate.cert.pem().into_bytes())
            .with_copy_to("/tmp/dbos-server.key", certificate.signing_key.serialize_pem().into_bytes())
            .with_cmd(["bash", "-c", "chown postgres:postgres /tmp/dbos-server.key && chmod 0600 /tmp/dbos-server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/dbos-server.crt -c ssl_key_file=/tmp/dbos-server.key"])
            .start().await.expect("actual TLS-enabled PostgreSQL");
        (server, dir, ca)
    }

    #[tokio::test]
    async fn native_store_reconnects_with_verified_tls_and_refuses_wrong_host_or_ca() {
        let (server, _dir, ca) = tls_server().await;
        let port = server.get_host_port_ipv4(5432).await.expect("TLS PostgreSQL port");
        let url = profile("localhost", port);
        let transport = PostgresTransport::new(&url, Some(&ca)).expect("immutable checked profile");
        // Independent oracle: actual transport, then close and reconnect through
        // the same pinned profile; a profile flag alone cannot satisfy this.
        for _ in 0..2 {
            let client = transport.connect().await.expect("actual verified handshake");
            assert!(
                client
                    .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", &[])
                    .await
                    .expect("supplemental TLS oracle")
                    .get::<_, bool>(0)
            );
        }
        let store =
            crate::SystemDatabaseHandle::postgres_with_ca(&url, "dbos_tls", &ca).await.expect("production system-store constructor");
        let store = store.into_arc();
        store.migrate().await.expect("real store migration through verified TLS");
        store.set_patch("verified", true).await.expect("real durable write");
        assert_eq!(store.get_patch("verified").await.expect("real durable read"), Some(true));
        let mut config = crate::DbosConfig::new("tls-profile").with_database_url(&url).with_postgres_ca_file(&ca);
        config.database_schema = "dbos_tls_context".into();
        crate::DbosContext::new(config).await.expect("actual Context constructor retains required verified profile");
        assert!(PostgresTransport::new(&profile("wrong.example", port), Some(&ca)).expect("wrong-host profile").connect().await.is_err());
        let wrong_host = crate::DbosConfig::new("wrong-host").with_database_url(profile("wrong.example", port)).with_postgres_ca_file(&ca);
        assert!(
            crate::DbosContext::new(wrong_host).await.is_err(),
            "actual Context store cannot bypass hostname validation"
        );
        let wrong = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("untrusted signer");
        let wrong_ca = ca.with_file_name("wrong.pem");
        std::fs::write(&wrong_ca, wrong.cert.pem()).expect("wrong trust fixture");
        private_file(&wrong_ca);
        assert!(PostgresTransport::new(&url, Some(&wrong_ca)).expect("wrong-CA profile").connect().await.is_err());
    }

    #[tokio::test]
    async fn native_required_tls_refuses_plaintext_and_default_legacy_stays_compatible() {
        let server = Postgres::default().start().await.expect("actual plaintext-only PostgreSQL");
        let port = server.get_host_port_ipv4(5432).await.expect("plaintext port");
        let dir = tempfile::tempdir().expect("trust directory");
        let ca = dir.path().join("ca.pem");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).expect("trust anchor");
        std::fs::write(&ca, cert.cert.pem()).expect("trust file");
        private_file(&ca);
        let url = profile("localhost", port);
        assert!(PostgresTransport::new(&url, Some(&ca)).expect("Require profile despite URL Disable").connect().await.is_err());
        let config = crate::DbosConfig::new("downgrade").with_database_url(&url).with_postgres_ca_file(&ca);
        assert!(
            crate::DbosContext::new(config).await.is_err(),
            "actual Context store refuses plaintext despite URL sslmode=disable"
        );
        let client = PostgresTransport::new(&url, None).expect("legacy profile").connect().await.expect("unchanged opt-out plaintext");
        assert_eq!(client.query_one("SELECT 1::int", &[]).await.expect("legacy query").get::<_, i32>(0), 1);
    }
}
