//! Immutable PostgreSQL transport selected when the system store is created.
//!
//! A trusted CA opts this store into required TLS. Reconnects retain that exact
//! verified profile and cannot consult changed environment or fall back to NoTls.
use crate::error::{DbosError, Result};
use crate::observability::log_database_warning;
use rustls::RootCertStore;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;
use tokio_postgres::{
    Client, Config,
    config::{Host, SslMode},
};
use tokio_postgres_rustls::MakeRustlsConnect;

pub(crate) enum PostgresTransport {
    Legacy(Config),
    Verified { config: Config, connector: MakeRustlsConnect },
}

impl PostgresTransport {
    pub(crate) fn new(database_url: &str, ca_file: Option<&Path>) -> Result<Self> {
        let mut config: Config = database_url.parse().map_err(DbosError::from)?;
        let Some(ca) = ca_file else {
            return Ok(Self::Legacy(config));
        };
        // Explicit trust always requires TLS, even if an inherited URL requested
        // Disable/Prefer. A server that refuses TLS cannot receive credentials.
        config.ssl_mode(SslMode::Require);
        match config.get_hosts() {
            [Host::Tcp(host)] if !host.is_empty() => {
                rustls::pki_types::ServerName::try_from(host.to_owned())
                    .map_err(|_| DbosError::initialization("invalid verified PostgreSQL certificate hostname"))?;
            }
            _ => {
                return Err(DbosError::initialization(
                    "verified PostgreSQL requires one explicit certificate hostname; no Unix or host fallback",
                ));
            }
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|_| DbosError::initialization("verified PostgreSQL TLS protocol configuration failed"))?
            .with_root_certificates(trust_roots(ca)?)
            .with_no_client_auth();
        Ok(Self::Verified { config, connector: MakeRustlsConnect::new(tls) })
    }

    pub(crate) async fn connect(&self) -> Result<Client> {
        match self {
            Self::Legacy(config) => {
                let (client, connection) = config.connect(tokio_postgres::NoTls).await.map_err(DbosError::from)?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        log_database_warning("dbos postgres connection task exited", &error);
                    }
                });
                Ok(client)
            }
            Self::Verified { config, connector } => {
                let (client, connection) = config.connect(connector.clone()).await.map_err(DbosError::from)?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        log_database_warning("dbos verified postgres connection task exited", &error);
                    }
                });
                // This result comes only after actual required-TLS PostgreSQL
                // authentication. No config flag or server SQL assertion mints it.
                Ok(client)
            }
        }
    }
}

fn trust_roots(path: &Path) -> Result<RootCertStore> {
    const LIMIT: u64 = 1024 * 1024;
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(path).map_err(|_| DbosError::initialization("verified PostgreSQL CA file is unavailable"))?;
    let metadata = file.metadata().map_err(|_| DbosError::initialization("verified PostgreSQL CA metadata is unavailable"))?;
    if !metadata.is_file() || metadata.len() > LIMIT {
        return Err(DbosError::initialization("verified PostgreSQL CA must be a bounded regular file"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no inputs, pointer accesses or failure sentinel.
        let uid = unsafe { libc::geteuid() };
        if metadata.mode() & 0o022 != 0 || ![0, uid].contains(&metadata.uid()) {
            return Err(DbosError::initialization(
                "verified PostgreSQL CA must be owned by root or the service and not writable by group/others",
            ));
        }
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut file)
        .take(LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| DbosError::initialization("verified PostgreSQL CA could not be read"))?;
    if bytes.len() as u64 > LIMIT {
        return Err(DbosError::initialization("verified PostgreSQL CA exceeds its bound"));
    }
    let certificates = rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| DbosError::initialization("invalid verified PostgreSQL CA PEM"))?;
    if certificates.is_empty() || certificates.len() > 64 {
        return Err(DbosError::initialization("verified PostgreSQL CA requires 1 through 64 trust anchors"));
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots.add(certificate).map_err(|_| DbosError::initialization("invalid verified PostgreSQL trust anchor"))?;
    }
    Ok(roots)
}

#[cfg(test)]
mod tests;
