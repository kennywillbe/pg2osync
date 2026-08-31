//! What the PostgreSQL source can fail at.
//!
//! The variants exist to be matched, not only printed: the reconnect policy
//! asks whether another attempt could behave differently, and only the error
//! itself knows. The sentence an operator reads is the variant's `context`,
//! and the failure underneath travels in `source`, so the binary can print
//! the whole chain rather than the outermost sentence alone.

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// The source could not be reached, or an established connection dropped.
    #[error("{context}")]
    Connect {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// The server was reached and answered with an error of its own: a
    /// password it will not accept, a database that is not there. Separate
    /// from `Connect` because the two want opposite treatment before a stream
    /// exists — one is worth waiting for, the other is a verdict.
    #[error("{context}")]
    Refused {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// A catalogue read, or a statement that prepares the objects the pipeline
    /// needs, failed.
    #[error("{context}")]
    Catalog {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// The stream carried something that cannot be turned into a document, or
    /// an invariant of the decoding bookkeeping broke.
    #[error("{context}")]
    Protocol {
        context: String,
        #[source]
        source: Option<BoxError>,
    },

    /// A configuration the source cannot satisfy. No attempt will do better.
    #[error("{0}")]
    Config(String),

    /// The engine went away, so there is nobody left to send events to.
    #[error("change channel closed")]
    ChannelClosed,

    #[error(transparent)]
    Tls(#[from] pg2osync_tls::TlsError),

    #[error(transparent)]
    Build(#[from] crate::docbuild::BuildError),

    #[error(transparent)]
    Core(#[from] pg2osync_core::CoreError),
}

impl SourceError {
    /// Whether a later attempt could behave differently.
    ///
    /// Only a configuration the source cannot satisfy is hopeless: a dropped
    /// connection, a catalogue read that lost its server mid-restart or a
    /// frame that arrived truncated have all succeeded before and can again.
    /// Retrying the hopeless case merely delays the report by the whole
    /// backoff schedule.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, Self::Config(_))
    }

    /// Whether the server was never reached.
    ///
    /// What the setup phase asks, where `is_retryable` is too generous: before
    /// a stream has ever run, a catalogue read that fails is far more often a
    /// table that is not there than a server that went away mid-read, and an
    /// answer the server sent — a password it refuses — is the same answer
    /// every attempt gets. Waiting is only ever right for something nothing
    /// answered.
    pub fn is_unreachable(&self) -> bool {
        matches!(self, Self::Connect { .. })
    }

    pub fn connect(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Connect {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn refused(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Refused {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn catalog(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Catalog {
            context: context.into(),
            source: Some(source.into()),
        }
    }

    pub fn protocol(context: impl Into<String>) -> Self {
        Self::Protocol {
            context: context.into(),
            source: None,
        }
    }

    pub fn protocol_from(context: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::Protocol {
            context: context.into(),
            source: Some(source.into()),
        }
    }
}

/// The sentence an operator needs, attached to whatever failed underneath.
///
/// A bare database or transport error names a symptom and not the operation
/// that hit it, which is the part an operator can act on; the kind chosen here
/// is also what tells a caller whether the operation is worth repeating.
pub(crate) trait Context<T> {
    fn connect_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
    fn catalog_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
    fn protocol_ctx(self, context: impl FnOnce() -> String) -> Result<T>;
}

impl<T, E> Context<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn connect_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| SourceError::connect(context(), e))
    }

    fn catalog_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| SourceError::catalog(context(), e))
    }

    fn protocol_ctx(self, context: impl FnOnce() -> String) -> Result<T> {
        self.map_err(|e| SourceError::protocol_from(context(), e))
    }
}

pub type Result<T, E = SourceError> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_server_that_was_never_reached_is_worth_waiting_for() {
        assert!(
            SourceError::Connect {
                context: "connection failed".into(),
                source: None,
            }
            .is_unreachable()
        );
        assert!(
            !SourceError::Refused {
                context: "password authentication failed".into(),
                source: None,
            }
            .is_unreachable(),
            "an answer the server sent is the answer every attempt gets"
        );
        assert!(
            !SourceError::Catalog {
                context: "relation \"public.orders\" does not exist".into(),
                source: None,
            }
            .is_unreachable()
        );
        assert!(!SourceError::Config("no primary key".into()).is_unreachable());
    }

    #[test]
    fn a_stream_that_dropped_retries_wider_than_a_setup_does() {
        // a server that answered has still lost the connection the stream was
        // on, and the position it left is resumable
        let refused = SourceError::Refused {
            context: "the database system is starting up".into(),
            source: None,
        };
        assert!(refused.is_retryable());
        assert!(!refused.is_unreachable());
    }
}
