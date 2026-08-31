//! What the MySQL source can fail at.
//!
//! Same shape as the PostgreSQL source's taxonomy, for the same reason: the
//! reconnect policy has to ask whether another attempt could behave
//! differently, and only the error knows. The sentence an operator reads is
//! the variant's `context`, and the failure underneath travels in `source`, so
//! the binary can print the whole chain rather than its outermost sentence.

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum MySqlError {
    /// The server could not be reached, or an established connection dropped.
    #[error("{context}")]
    Connect {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// The server refused this account, or the handshake could not agree on a
    /// plugin both sides speak.
    #[error("{context}")]
    Auth {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// A packet, a resultset or a binlog event did not hold what the protocol
    /// says it must.
    #[error("{context}")]
    Protocol {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// A catalogue read, or a statement the pipeline needs, failed.
    #[error("{context}")]
    Catalog {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// A server or pipeline configuration that no attempt can satisfy.
    #[error("{0}")]
    Config(String),

    /// The engine went away, so there is nobody left to send events to.
    #[error("change channel closed")]
    ChannelClosed,

    /// The engine stopped while the initial load was still feeding it, which
    /// says what the load was doing rather than only that a channel closed.
    #[error("{0}")]
    LoadInterrupted(String),

    #[error(transparent)]
    Tls(#[from] pg2osync_tls::TlsError),

    #[error(transparent)]
    Decode(#[from] crate::binlog::DecodeError),

    #[error(transparent)]
    Core(#[from] pg2osync_core::CoreError),

    #[error("{0}")]
    Io(#[from] std::io::Error),
}

impl MySqlError {
    /// Whether a later attempt could behave differently.
    ///
    /// Only a configuration nothing can satisfy is hopeless: a dropped socket,
    /// a truncated packet or a catalogue read that lost its server have all
    /// succeeded before and can again. Retrying the hopeless case merely
    /// delays the report by the whole backoff schedule.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, Self::Config(_))
    }

    /// Whether the server was never reached.
    ///
    /// What the setup phase asks, where `is_retryable` is too generous: before
    /// a stream has ever run, a catalogue read that fails is far more often a
    /// table that is not there than a server that went away mid-read, and an
    /// account the server refuses is refused just as firmly on the next
    /// attempt. Opening a connection is one operation from the outside and a
    /// TCP connect followed by a handshake from the inside, so a `Connect` that
    /// carries a verdict underneath is judged by that verdict.
    pub fn is_unreachable(&self) -> bool {
        match self {
            Self::Connect {
                source: Some(inner),
                ..
            } => inner
                .downcast_ref::<MySqlError>()
                .is_none_or(MySqlError::is_unreachable),
            Self::Connect { .. } | Self::Io(_) => true,
            _ => false,
        }
    }

    pub fn connect(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Connect {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn auth(context: impl Into<String>) -> Self {
        Self::Auth {
            context: context.into(),
            source: None,
        }
    }

    pub fn protocol(context: impl Into<String>) -> Self {
        Self::Protocol {
            context: context.into(),
            source: None,
        }
    }

    pub fn catalog(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Catalog {
            context: context.into(),
            source: Some(source.into()),
        }
    }
}

/// The sentence an operator needs, attached to whatever failed underneath.
///
/// A bare socket or protocol error names a symptom and not the operation that
/// hit it, which is the part an operator can act on; the kind chosen here is
/// also what tells a caller whether the operation is worth repeating.
pub(crate) trait Context<T> {
    fn connect_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
    fn auth_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
    fn catalog_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn connect_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| MySqlError::connect(context(), e))
    }

    fn auth_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| MySqlError::Auth {
            context: context(),
            source: Some(Box::new(e)),
        })
    }

    fn catalog_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| MySqlError::catalog(context(), e))
    }
}

pub type Result<T, E = MySqlError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    /// What `admin_connection` does to whatever the handshake returned.
    fn wrapped(inner: MySqlError) -> MySqlError {
        MySqlError::connect("mysql admin connection failed", inner)
    }

    #[test]
    fn only_a_server_that_was_never_reached_is_worth_waiting_for() {
        assert!(
            wrapped(MySqlError::connect(
                "mysql tcp connect failed",
                std::io::Error::from(std::io::ErrorKind::ConnectionRefused),
            ))
            .is_unreachable()
        );
        assert!(
            !wrapped(MySqlError::auth("access denied for user 'repl'")).is_unreachable(),
            "an account the server refuses is refused on every attempt"
        );
        assert!(
            !wrapped(MySqlError::Config(
                "sslmode=require was requested but the server does not offer TLS".into()
            ))
            .is_unreachable()
        );
        assert!(
            !MySqlError::Catalog {
                context: "sourcedb.orders is not there".into(),
                source: None,
            }
            .is_unreachable()
        );
    }

    #[test]
    fn a_stream_that_dropped_retries_wider_than_a_setup_does() {
        // credentials that were accepted once can be refused mid-failover
        // while the promoted server is still warming up, and by then the
        // pipeline has a position to resume from
        let refused = wrapped(MySqlError::auth("access denied for user 'repl'"));
        assert!(refused.is_retryable());
        assert!(!refused.is_unreachable());
    }
}
