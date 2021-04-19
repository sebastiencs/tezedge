// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use shiplift::Docker;
use slog::{error, info, Logger};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::configuration::{AlertThresholds, DeployMonitoringEnvironment};
use crate::constants::MEASUREMENTS_MAX_CAPACITY;
use crate::deploy_with_compose::{
    cleanup_docker, restart_sandbox, restart_stack, stop_with_compose,
};
use crate::monitors::alerts::Alerts;
use crate::monitors::deploy::DeployMonitor;
use crate::monitors::resource::{
    ResourceMonitor, ResourceUtilization, ResourceUtilizationStorageMap,
};
use crate::rpc;
use crate::slack::SlackServer;

pub mod alerts;
pub mod deploy;
pub mod resource;

pub fn start_deploy_monitoring(
    compose_file_path: PathBuf,
    slack: Option<SlackServer>,
    interval: u64,
    log: Logger,
    running: Arc<AtomicBool>,
    cleanup_data: bool,
) -> JoinHandle<()> {
    let docker = Docker::new();
    let deploy_monitor =
        DeployMonitor::new(compose_file_path, docker, slack, log.clone(), cleanup_data);
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = deploy_monitor.monitor_stack().await {
                error!(log, "Deploy monitoring error: {}", e);
            }
            sleep(Duration::from_secs(interval)).await;
        }
    })
}

pub fn start_sandbox_monitoring(
    compose_file_path: PathBuf,
    slack: Option<SlackServer>,
    interval: u64,
    log: Logger,
    running: Arc<AtomicBool>,
    cleanup_data: bool,
) -> JoinHandle<()> {
    let docker = Docker::new();
    let deploy_monitor =
        DeployMonitor::new(compose_file_path, docker, slack, log.clone(), cleanup_data);
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = deploy_monitor.monitor_sandbox_launcher().await {
                error!(log, "Sandbox launcher monitoring error: {}", e);
            }
            sleep(Duration::from_secs(interval)).await;
        }
    })
}

pub fn start_resource_monitoring(
    interval: u64,
    log: Logger,
    running: Arc<AtomicBool>,
    resource_utilization: ResourceUtilizationStorageMap,
    alert_thresholds: AlertThresholds,
    slack: Option<SlackServer>,
) -> JoinHandle<()> {
    let alerts = Alerts::new(alert_thresholds);
    let mut resource_monitor =
        ResourceMonitor::new(resource_utilization, None, alerts, log.clone(), slack);
    tokio::spawn(async move {
        while running.load(Ordering::Acquire) {
            if let Err(e) = resource_monitor.take_measurement().await {
                error!(log, "Resource monitoring error: {}", e);
            }
            sleep(Duration::from_secs(interval)).await;
        }
    })
}

pub async fn shutdown_and_cleanup(
    compose_file_path: &PathBuf,
    slack: Option<SlackServer>,
    log: &Logger,
    cleanup_data: bool,
) -> Result<(), failure::Error> {
    if let Some(slack_server) = slack {
        slack_server.send_message("Manual shuttdown ").await?;
    }
    info!(log, "Manual shutdown");

    stop_with_compose(compose_file_path);
    cleanup_docker(cleanup_data);

    Ok(())
}

pub async fn start_stack(
    compose_file_path: &PathBuf,
    alert_thresholds: AlertThresholds,
    slack: Option<SlackServer>,
    log: &Logger,
    cleanup_data: bool,
) -> Result<(), failure::Error> {
    info!(log, "Starting tezedge stack");

    // cleanup possible dangling containers/volumes and start the stack
    restart_stack(compose_file_path, log, cleanup_data).await;
    if let Some(slack_server) = slack {
        slack_server.send_message("Tezedge stack started").await?;
        slack_server
            .send_message(&format!("Alert thresholds set to: {}", alert_thresholds))
            .await?;
    }
    Ok(())
}

pub async fn start_sandbox(
    compose_file_path: &PathBuf,
    slack: Option<SlackServer>,
    log: &Logger,
) -> Result<(), failure::Error> {
    info!(log, "Starting tezedge stack");

    // cleanup possible dangling containers/volumes and start the stack
    restart_sandbox(compose_file_path, log).await;
    if let Some(slack_server) = slack {
        slack_server
            .send_message("Tezedge sandbox launcher started")
            .await?;
    }
    Ok(())
}

pub async fn spawn_sandbox(
    env: DeployMonitoringEnvironment,
    slack_server: Option<SlackServer>,
    log: &Logger,
    running: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    start_sandbox(&env.compose_file_path, slack_server.clone(), &log)
        .await
        .expect("Sandbox failed to start");

    info!(log, "Creating docker image monitor");
    let deploy_handle = start_sandbox_monitoring(
        env.compose_file_path.clone(),
        slack_server.clone(),
        env.image_monitor_interval,
        log.clone(),
        running.clone(),
        env.cleanup_volumes,
    );
    vec![deploy_handle]
}

pub async fn spawn_node_stack(
    env: DeployMonitoringEnvironment,
    slack_server: Option<SlackServer>,
    log: &Logger,
    running: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    start_stack(
        &env.compose_file_path,
        env.alert_thresholds,
        slack_server.clone(),
        &log,
        env.cleanup_volumes,
    )
    .await
    .expect("Stack failed to start");

    info!(log, "Creating docker image monitor");
    let deploy_handle = start_deploy_monitoring(
        env.compose_file_path.clone(),
        slack_server.clone(),
        env.image_monitor_interval,
        log.clone(),
        running.clone(),
        env.cleanup_volumes,
    );

    // TODO: TE-499 - (multiple nodes) rework this to load from a config, where all the nodes all defined
    // create a thread safe VecDeque for each node's resource utilization data
    let mut storage_map = HashMap::new();
    storage_map.insert(
        "tezedge",
        Arc::new(RwLock::new(VecDeque::<ResourceUtilization>::with_capacity(
            MEASUREMENTS_MAX_CAPACITY,
        ))),
    );
    storage_map.insert(
        "ocaml",
        Arc::new(RwLock::new(VecDeque::<ResourceUtilization>::with_capacity(
            MEASUREMENTS_MAX_CAPACITY,
        ))),
    );

    info!(log, "Creating reosurces monitor");
    let resources_handle = start_resource_monitoring(
        env.resource_monitor_interval,
        log.clone(),
        running.clone(),
        storage_map.clone(),
        env.alert_thresholds,
        slack_server.clone(),
    );

    info!(log, "Starting rpc server on port {}", &env.rpc_port);
    let rpc_server_handle = rpc::spawn_rpc_server(env.rpc_port, log.clone(), storage_map.clone());

    vec![deploy_handle, resources_handle, rpc_server_handle]
}
