//! The reconcile loop.
//!
//! One `Pg2osync` becomes one ConfigMap, one headless Service, one Deployment
//! of a single replica and — where prometheus-operator is installed — one
//! ServiceMonitor. Everything is applied server-side under one field manager,
//! so a reconcile that changes nothing writes nothing.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::api::{Api, DeleteParams, DynamicObject, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, Resource, ResourceExt};
use tracing::{error, info, warn};

use crate::crd::{Invalid, Pg2osync, Pg2osyncStatus};
use crate::render::render_files;
use crate::resources::{self, Images, MANAGER};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("the API server refused a write: {0}")]
    Api(#[from] kube::Error),
    #[error("the resource has no namespace")]
    NoNamespace,
}

pub struct Context {
    pub client: Client,
    pub images: Images,
    /// Whether `ServiceMonitor` resolved at startup. A cluster that installs
    /// prometheus-operator afterwards needs the operator restarted, which is
    /// cheaper than a discovery round trip per reconcile.
    pub service_monitor_available: bool,
}

async fn reconcile(cr: Arc<Pg2osync>, ctx: Arc<Context>) -> Result<Action, Error> {
    let namespace = cr.namespace().ok_or(Error::NoNamespace)?;
    let name = cr.name_any();
    let crs: Api<Pg2osync> = Api::namespaced(ctx.client.clone(), &namespace);

    let files = match render_files(&cr.spec) {
        Ok(files) => files,
        Err(Invalid(why)) => {
            warn!(%namespace, %name, "refusing the spec: {why}");
            // Nothing to retry: only an edit can make this spec renderable.
            set_status(&crs, &cr, false, 0, Some(why)).await?;
            return Ok(Action::await_change());
        }
    };
    let sources = files.len() as i32;
    let checksum = resources::checksum(&files);

    let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &namespace);
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let params = PatchParams::apply(MANAGER).force();

    let config_map = resources::config_map(&cr, files);
    config_maps
        .patch(&name, &params, &Patch::Apply(&config_map))
        .await?;
    services
        .patch(&name, &params, &Patch::Apply(&resources::service(&cr)))
        .await?;
    let deployment = resources::deployment(&cr, &ctx.images, &checksum);
    let deployment = deployments
        .patch(&name, &params, &Patch::Apply(&deployment))
        .await?;

    reconcile_service_monitor(&cr, &ctx, &namespace, &name, &params).await?;

    let ready = deployment
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0)
        > 0;
    let message = (!ready).then(|| "the Deployment has no ready replica".to_string());
    set_status(&crs, &cr, ready, sources, message).await?;

    // Readiness is the Deployment's, and a pod becoming ready is not an event
    // on the Deployment the controller watches, so the status is refreshed on
    // a timer rather than left stale.
    Ok(Action::requeue(Duration::from_secs(60)))
}

async fn reconcile_service_monitor(
    cr: &Pg2osync,
    ctx: &Context,
    namespace: &str,
    name: &str,
    params: &PatchParams,
) -> Result<(), Error> {
    if !ctx.service_monitor_available {
        if cr.spec.service_monitor {
            // Fail-open: a missing prometheus-operator makes the pipeline
            // unscraped, not undeployed.
            warn!(%namespace, %name, "serviceMonitor is set but the cluster has no ServiceMonitor kind; skipping it");
        }
        return Ok(());
    }
    let resource = kube::api::ApiResource::from_gvk(&resources::service_monitor_gvk());
    let monitors: Api<DynamicObject> =
        Api::namespaced_with(ctx.client.clone(), namespace, &resource);
    if cr.spec.service_monitor {
        monitors
            .patch(name, params, &Patch::Apply(&resources::service_monitor(cr)))
            .await?;
    } else if let Err(e) = monitors.delete(name, &DeleteParams::default()).await
        && !matches!(&e, kube::Error::Api(err) if err.code == 404)
    {
        return Err(e.into());
    }
    Ok(())
}

async fn set_status(
    crs: &Api<Pg2osync>,
    cr: &Pg2osync,
    ready: bool,
    sources: i32,
    message: Option<String>,
) -> Result<(), Error> {
    let status = Pg2osyncStatus {
        observed_generation: cr.meta().generation,
        ready,
        sources,
        message,
    };
    crs.patch_status(
        &cr.name_any(),
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "status": status })),
    )
    .await?;
    Ok(())
}

fn error_policy(cr: Arc<Pg2osync>, error: &Error, _ctx: Arc<Context>) -> Action {
    error!(name = %cr.name_any(), "reconcile failed: {error}");
    Action::requeue(Duration::from_secs(30))
}

/// Watch the namespace the operator runs in until the process is stopped.
pub async fn run(client: Client, images: Images) -> Result<(), Error> {
    let service_monitor_available =
        match kube::discovery::pinned_kind(&client, &resources::service_monitor_gvk()).await {
            Ok(_) => true,
            Err(e) => {
                info!(
                    "no ServiceMonitor kind in this cluster ({e}); serviceMonitor will be skipped"
                );
                false
            }
        };

    let crs: Api<Pg2osync> = Api::default_namespaced(client.clone());
    let context = Arc::new(Context {
        client: client.clone(),
        images,
        service_monitor_available,
    });

    Controller::new(crs, watcher::Config::default())
        .owns(
            Api::<Deployment>::default_namespaced(client.clone()),
            watcher::Config::default(),
        )
        .owns(
            Api::<ConfigMap>::default_namespaced(client),
            watcher::Config::default(),
        )
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((object, _)) => info!(name = %object.name, "reconciled"),
                Err(e) => warn!("reconcile did not run: {e}"),
            }
        })
        .await;
    Ok(())
}
