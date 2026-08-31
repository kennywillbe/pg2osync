//! The objects one `Pg2osync` owns.
//!
//! Every one of them carries a controller owner reference, so deleting the
//! custom resource deletes them: the operator has no cleanup path of its own
//! to get wrong. What it does not own — the replication slot on the source
//! database — is in `docs/operator.md`.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvFromSource, EnvVar,
    EnvVarSource, HTTPGetAction, ObjectFieldSelector, PodSecurityContext, PodSpec, PodTemplateSpec,
    Probe, ResourceRequirements, SeccompProfile, SecretEnvSource, SecurityContext, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ApiResource, DynamicObject, GroupVersionKind};
use kube::{Resource, ResourceExt};
use sha2::{Digest, Sha256};

use crate::crd::{Pg2osync, ReloadMode};

pub const CONFIG_DIR: &str = "/etc/pg2osync";
pub const METRICS_PORT: i32 = 9100;
pub const MANAGER: &str = "pg2osync-operator";

/// The images the operator hands the pod. Both are pinned by the operator's
/// own Deployment rather than defaulted in code, so an upgrade is a manifest
/// change a human can see.
#[derive(Clone, Debug)]
pub struct Images {
    pub pipeline: String,
    pub reload_sidecar: String,
}

/// Prometheus' own kind, which is a CRD rather than a built-in type.
pub fn service_monitor_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("monitoring.coreos.com", "v1", "ServiceMonitor")
}

fn selector_labels(cr: &Pg2osync) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("app.kubernetes.io/name".to_string(), "pg2osync".to_string()),
        ("app.kubernetes.io/instance".to_string(), cr.name_any()),
    ])
}

fn labels(cr: &Pg2osync) -> BTreeMap<String, String> {
    let mut labels = selector_labels(cr);
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        MANAGER.to_string(),
    );
    labels
}

fn meta(cr: &Pg2osync) -> ObjectMeta {
    ObjectMeta {
        name: Some(cr.name_any()),
        namespace: cr.namespace(),
        labels: Some(labels(cr)),
        // None only for an object with no name or no uid; this one came back
        // from a watch on the API server, which assigns both before anything
        // can observe it.
        owner_references: Some(vec![
            cr.controller_owner_ref(&()).expect("a named resource"),
        ]),
        ..Default::default()
    }
}

/// What the pod restarts on in `restart` mode: the rendered files, not the
/// spec, so a change that renders the same config does not drain a pipeline.
pub fn checksum(files: &BTreeMap<String, String>) -> String {
    let mut hasher = Sha256::new();
    for (name, body) in files {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(body.as_bytes());
        hasher.update(b"\0");
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub fn config_map(cr: &Pg2osync, files: BTreeMap<String, String>) -> ConfigMap {
    ConfigMap {
        metadata: meta(cr),
        data: Some(files),
        ..Default::default()
    }
}

/// Headless, because nothing load-balances a single instance — it exists so a
/// ServiceMonitor has endpoints to select and a port-forward has a name.
pub fn service(cr: &Pg2osync) -> Service {
    Service {
        metadata: meta(cr),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(selector_labels(cr)),
            ports: Some(vec![ServicePort {
                name: Some("metrics".to_string()),
                port: METRICS_PORT,
                target_port: Some(IntOrString::String("metrics".to_string())),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn probe(path: &str, period: i32, failures: i32) -> Probe {
    Probe {
        http_get: Some(HTTPGetAction {
            path: Some(path.to_string()),
            port: IntOrString::String("metrics".to_string()),
            ..Default::default()
        }),
        period_seconds: Some(period),
        failure_threshold: Some(failures),
        ..Default::default()
    }
}

fn container_security_context() -> SecurityContext {
    SecurityContext {
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(true),
        capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn config_mount() -> VolumeMount {
    VolumeMount {
        name: "config".to_string(),
        mount_path: CONFIG_DIR.to_string(),
        read_only: Some(true),
        ..Default::default()
    }
}

/// Watches the mounted directory — not one file: the kubelet swaps the whole
/// `..data` symlink, and any file in the set may be the one that changed — and
/// signals the pipeline, which validates the new file itself and refuses in
/// place what a running process cannot take.
fn reload_sidecar(image: &str) -> Container {
    let script = format!(
        r#"seen=""
while :; do
  now="$(stat -c %Y {CONFIG_DIR} 2>/dev/null || echo missing)"
  if [ -n "$seen" ] && [ "$now" != "$seen" ]; then
    echo "the mounted configuration changed; sending SIGHUP"
    pkill -HUP -x pg2osync || echo "no pg2osync process to signal"
  fi
  seen="$now"
  sleep 10
done
"#
    );
    Container {
        name: "reload".to_string(),
        image: Some(image.to_string()),
        command: Some(vec!["/bin/sh".to_string(), "-c".to_string()]),
        args: Some(vec![script]),
        resources: Some(ResourceRequirements {
            requests: Some(BTreeMap::from([
                ("cpu".to_string(), Quantity("5m".to_string())),
                ("memory".to_string(), Quantity("8Mi".to_string())),
            ])),
            limits: Some(BTreeMap::from([(
                "memory".to_string(),
                Quantity("32Mi".to_string()),
            )])),
            ..Default::default()
        }),
        security_context: Some(container_security_context()),
        volume_mounts: Some(vec![config_mount()]),
        ..Default::default()
    }
}

pub fn deployment(cr: &Pg2osync, images: &Images, checksum: &str) -> Deployment {
    let signal = cr.spec.reload_on_change == ReloadMode::Signal;

    let mut env: Vec<EnvVar> = cr
        .spec
        .env
        .iter()
        .map(|(name, value)| EnvVar {
            name: name.clone(),
            value: Some(value.clone()),
            ..Default::default()
        })
        .collect();
    env.push(EnvVar {
        // identifies the writer in the checkpoint document
        name: "PG2OSYNC_INSTANCE_ID".to_string(),
        value_from: Some(EnvVarSource {
            field_ref: Some(ObjectFieldSelector {
                field_path: "metadata.name".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    });

    let pipeline = Container {
        name: "pg2osync".to_string(),
        image: Some(images.pipeline.clone()),
        // A directory even for a single file: one code path here and one in
        // the pipeline, and adding a second source later is an edit to the
        // spec rather than a change of command line.
        args: Some(vec![
            "run".to_string(),
            "--config-dir".to_string(),
            CONFIG_DIR.to_string(),
        ]),
        env: Some(env),
        env_from: Some(
            cr.spec
                .secret_refs
                .iter()
                .map(|name| EnvFromSource {
                    secret_ref: Some(SecretEnvSource {
                        name: name.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect(),
        ),
        ports: Some(vec![ContainerPort {
            name: Some("metrics".to_string()),
            container_port: METRICS_PORT,
            ..Default::default()
        }]),
        // The initial load of a large table happens before the endpoint
        // answers, so the startup probe has to outlast it; liveness alone
        // would restart the pod forever.
        startup_probe: Some(probe("/healthz", 10, 60)),
        liveness_probe: Some(probe("/healthz", 30, 3)),
        readiness_probe: Some(probe("/healthz", 10, 3)),
        security_context: Some(container_security_context()),
        volume_mounts: Some(vec![config_mount()]),
        ..Default::default()
    };

    let mut containers = vec![pipeline];
    if signal {
        containers.push(reload_sidecar(&images.reload_sidecar));
    }

    let mut annotations = BTreeMap::new();
    if !signal {
        annotations.insert("checksum/config".to_string(), checksum.to_string());
    }

    Deployment {
        metadata: meta(cr),
        spec: Some(DeploymentSpec {
            // This object owns these slots. Two processes streaming one slot
            // fight over its position and undo each other's progress, so a
            // second replica is not more throughput, it is two pipelines
            // corrupting one checkpoint.
            replicas: Some(1),
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_string()),
                ..Default::default()
            }),
            selector: LabelSelector {
                match_labels: Some(selector_labels(cr)),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(selector_labels(cr)),
                    annotations: (!annotations.is_empty()).then_some(annotations),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers,
                    // the sidecar has to be able to see — and signal — the
                    // pipeline process
                    share_process_namespace: signal.then_some(true),
                    security_context: Some(PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(10001),
                        fs_group: Some(10001),
                        seccomp_profile: Some(SeccompProfile {
                            type_: "RuntimeDefault".to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    // Shared by every source in the pod: SIGTERM drains them
                    // in parallel, but the budget is the whole set's.
                    termination_grace_period_seconds: Some(30),
                    volumes: Some(vec![Volume {
                        name: "config".to_string(),
                        config_map: Some(ConfigMapVolumeSource {
                            name: cr.name_any(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Built untyped because prometheus-operator's kinds are not in k8s-openapi,
/// and pulling a crate in for one object would tie the operator's release to
/// theirs.
pub fn service_monitor(cr: &Pg2osync) -> DynamicObject {
    let resource = ApiResource::from_gvk(&service_monitor_gvk());
    DynamicObject::new(&cr.name_any(), &resource)
        .within(&cr.namespace().unwrap_or_default())
        .data(serde_json::json!({
            "spec": {
                "selector": { "matchLabels": selector_labels(cr) },
                "endpoints": [{ "port": "metrics", "interval": "30s" }],
            }
        }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::Pg2osyncSpec;

    fn cr(spec: serde_json::Value) -> Pg2osync {
        let mut cr = Pg2osync::new(
            "tenant-a",
            serde_json::from_value::<Pg2osyncSpec>(spec).expect("a valid spec"),
        );
        cr.metadata.namespace = Some("pipelines".to_string());
        cr.metadata.uid = Some("11111111-2222-3333-4444-555555555555".to_string());
        cr
    }

    fn minimal() -> serde_json::Value {
        serde_json::json!({ "config": { "source": { "url_env": "URL" } } })
    }

    #[test]
    fn everything_the_operator_creates_is_garbage_collected_with_the_resource() {
        let cr = cr(minimal());
        let owners = meta(&cr).owner_references.expect("an owner");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0].kind, "Pg2osync");
        assert_eq!(owners[0].controller, Some(true));
    }

    #[test]
    fn restart_mode_carries_the_checksum_and_signal_mode_does_not() {
        let files = BTreeMap::from([("pg2osync.toml".to_string(), "[source]\n".to_string())]);
        let sum = checksum(&files);
        let images = Images {
            pipeline: "ghcr.io/kennywillbe/pg2osync:test".to_string(),
            reload_sidecar: "busybox:1.37".to_string(),
        };

        let restart = deployment(&cr(minimal()), &images, &sum);
        let pod = restart.spec.expect("a spec").template;
        let annotations = pod.metadata.expect("metadata").annotations.expect("a sum");
        assert_eq!(annotations["checksum/config"], sum);
        assert_eq!(pod.spec.expect("a pod").containers.len(), 1);

        let mut spec = minimal();
        spec["reloadOnChange"] = serde_json::json!("signal");
        let signal = deployment(&cr(spec), &images, &sum);
        let pod = signal.spec.expect("a spec").template;
        assert!(pod.metadata.expect("metadata").annotations.is_none());
        let pod = pod.spec.expect("a pod");
        assert_eq!(pod.containers.len(), 2);
        assert_eq!(pod.share_process_namespace, Some(true));
    }

    #[test]
    fn the_pipeline_reads_the_mounted_directory() {
        let images = Images {
            pipeline: "img".to_string(),
            reload_sidecar: "busybox:1.37".to_string(),
        };
        let deployment = deployment(&cr(minimal()), &images, "sum");
        let container = deployment
            .spec
            .expect("a spec")
            .template
            .spec
            .expect("a pod")
            .containers[0]
            .clone();
        assert_eq!(
            container.args,
            Some(vec![
                "run".to_string(),
                "--config-dir".to_string(),
                CONFIG_DIR.to_string()
            ])
        );
    }

    #[test]
    fn a_config_that_renders_the_same_does_not_move_the_checksum() {
        let a = BTreeMap::from([("a.toml".to_string(), "[source]\n".to_string())]);
        let b = BTreeMap::from([("a.toml".to_string(), "[source]\n".to_string())]);
        assert_eq!(checksum(&a), checksum(&b));
        let c = BTreeMap::from([("a.toml".to_string(), "[target]\n".to_string())]);
        assert_ne!(checksum(&a), checksum(&c));
    }
}
