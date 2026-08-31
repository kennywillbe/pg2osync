//! The pg2osync Kubernetes operator.
//!
//! A separate binary and a separate dependency tree on purpose: kube-rs and
//! k8s-openapi are a large surface, and the pipeline that runs in the pod must
//! not carry a Kubernetes client to run anywhere else.

mod controller;
mod crd;
mod render;
mod resources;

use clap::{Parser, Subcommand};
use kube::CustomResourceExt;
use tracing_subscriber::EnvFilter;

use crate::resources::Images;

#[derive(Parser)]
#[command(
    version,
    about = "Reconciles Pg2osync resources into running pipelines"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Watch this namespace and reconcile every Pg2osync in it.
    Run {
        /// The pipeline image the pods run. Pinned by the operator's own
        /// Deployment rather than defaulted here, so an upgrade is a manifest
        /// change somebody reviewed.
        #[arg(long, env = "PG2OSYNC_PIPELINE_IMAGE")]
        pipeline_image: String,
        /// The image of the sidecar that sends SIGHUP in `signal` mode.
        #[arg(
            long,
            env = "PG2OSYNC_RELOAD_SIDECAR_IMAGE",
            default_value = "busybox:1.37"
        )]
        reload_sidecar_image: String,
    },
    /// Print the CustomResourceDefinition, which is what
    /// `deploy/operator/crd.yaml` holds.
    Crd,
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Controller(#[from] controller::Error),
    #[error("could not reach the Kubernetes API: {0}")]
    Client(#[from] kube::Error),
    #[error("could not render the definition: {0}")]
    Yaml(#[from] serde_saphyr::SerializeError),
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli = Cli::parse();
    match cli.command {
        Command::Crd => {
            print!("{}", crd::Pg2osync::crd_yaml()?);
            Ok(())
        }
        Command::Run {
            pipeline_image,
            reload_sidecar_image,
        } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new("pg2osync_operator=info,kube=warn")),
                )
                .init();
            // kube builds its own TLS config, so the choice has to be
            // process-wide here rather than passed into a builder. An error
            // means something already installed one, which is just as good.
            let _ = rustls::crypto::ring::default_provider().install_default();
            let client = kube::Client::try_default().await?;
            controller::run(
                client,
                Images {
                    pipeline: pipeline_image,
                    reload_sidecar: reload_sidecar_image,
                },
            )
            .await?;
            Ok(())
        }
    }
}

impl crd::Pg2osync {
    /// The definition as it is installed, and as the checked-in manifest holds
    /// it.
    fn crd_yaml() -> Result<String, serde_saphyr::SerializeError> {
        serde_saphyr::to_string(&crd::Pg2osync::crd())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_checked_in_definition_is_the_one_the_operator_generates() {
        // The manifest is what a cluster installs; a schema change that only
        // reached the binary would deploy a resource the API server rejects.
        assert_eq!(
            crd::Pg2osync::crd_yaml().expect("the definition renders"),
            include_str!("../../../deploy/operator/crd.yaml"),
            "run `cargo run -p pg2osync-operator -- crd > deploy/operator/crd.yaml`"
        );
    }
}
