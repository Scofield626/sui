// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, sync::Arc};

use tokio::{sync::mpsc, task::JoinHandle};

use super::core::{ProxyCore, ProxyId};
use crate::{
    config::ValidatorConfig,
    error::NodeResult,
    executor::api::Executor,
    metrics::Metrics,
    networking::client::NetworkClient,
};

/// Default channel size for communication between components.
const DEFAULT_CHANNEL_SIZE: usize = 1000;

use std::marker::PhantomData;

pub struct ProxyNode<E: Executor> {
    _phantom: PhantomData<E>,
    /// The handles for the core components.
    core_handles: Vec<std::thread::JoinHandle<NodeResult<()>>>,
    /// The handle for the network client.
    _network_handles: Vec<JoinHandle<io::Result<()>>>,
    /// The  metrics for the proxy
    _metrics: Arc<Metrics>,
}

impl<E: Executor + Clone + Send + Sync + 'static> ProxyNode<E> 
where 
E::Transaction: Send + Sync + serde::Serialize + serde::de::DeserializeOwned,
E::Store: Send + Sync,
E::ExecutionResults: Send + Sync + serde::Serialize + serde::de::DeserializeOwned 
{
    pub async fn start(
        proxy_id: ProxyId,
        executor: E,
        config: &ValidatorConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut core_handles = Vec::new();
        let mut network_handles = Vec::new();

        for i in 0..config.validator_parameters.collocated_pre_executors.proxy {
            let id = format!("{proxy_id}-{i}");
            let (tx_transactions, rx_transactions) = mpsc::channel(DEFAULT_CHANNEL_SIZE);
            let (tx_proxy_results, rx_proxy_results) = mpsc::channel(DEFAULT_CHANNEL_SIZE);

            let store = Arc::new(executor.create_in_memory_store());
            executor.load_state_for_shared_objects().await;
            // NOTE: If we run in multi-threaded mode, it is unclear why we need more than on ProxyCore.
            let core_handle = ProxyCore::new(
                id,
                executor.clone(),
                config.validator_parameters.proxy_mode.clone(),
                store,
                rx_transactions,
                tx_proxy_results,
                metrics.clone(),
            )
            .spawn_with_threads();
            core_handles.push(core_handle);

            let network_handle = NetworkClient::new(
                config.proxy_server_address,
                tx_transactions,
                rx_proxy_results,
            )
            .spawn();
            network_handles.push(network_handle);
        }

        Self {
            core_handles,
            _network_handles: network_handles,
            _metrics: metrics,
            _phantom: PhantomData,
        }
    }

    /// Collect the results from the validator.
    pub fn await_completion(self) {
        for handle in self.core_handles {
            match handle.join() {
                Ok(_) => tracing::info!("Thread completed successfully!"),
                Err(e) => panic!("Thread panicked: {e:?}"),
            }
        }
    }
}
