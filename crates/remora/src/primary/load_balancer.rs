// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, ops::Deref, sync::Arc};

use dashmap::DashMap;
use rustc_hash::FxHashMap;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    effects::TransactionEffectsAPI,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::api::{
        ExecutableTransaction, ExecutionResults, Executor, ExecutorIndex, NewStates,
        PrimaryToProxyMessage, PrimaryToProxyTxn, RemoraTransaction, StateStore, Store, Timestamp,
    },
    metrics::Metrics,
};

/// A load balancer is responsible for distributing transactions to proxies.
pub struct LoadBalancer<E: Executor> {
    /// The executor is only used to assigned shared object versions.
    executor: E,
    /// The object store.
    store: Store<E>,
    /// Receive handles to forward transactions to proxies. When a new client connects,
    /// this channel receives a sender from the network layer which is used to forward
    /// transactions to the proxies.
    rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
    /// Holds senders to forward transactions to proxies.
    proxy_connections:
        Arc<DashMap<ExecutorIndex, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>>,
    /// The receiver for committed transactions
    rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
    /// Keeps track of every attempt to forward a transaction to a proxy.
    index: ExecutorIndex,
    /// The sender to a local executor if no pre-executor is available.
    tx_executor_local: Sender<RemoraTransaction<E>>,
    /// The receiver of new effects from local executor and needs to forward to proxies.
    rx_states_sync: Receiver<ExecutionResults<E>>,
    /// The already updated states to the proxies to avoid
    /// blind forwarding of consecutive xshard txns.
    updated_states_to_proxy: Arc<DashMap<ObjectID, SequenceNumber>>,
    /// The metrics for the validator.
    metrics: Arc<Metrics>,
}

const FIB_CONSTANT: u64 = 11400714819323198485; // Golden ratio * 2^64

/// Fibonacci Hashing for ObjectID → Proxy Index Mapping (Fast & Even Distribution)
pub fn lb_hash(proxy_cnt: usize, object_id: &ObjectID) -> ExecutorIndex {
    let mut hash = 0u64;
    for chunk in object_id.chunks(8) {
        let mut chunk_array = [0u8; 8];
        chunk_array[..chunk.len()].copy_from_slice(chunk);
        let num = u64::from_ne_bytes(chunk_array);
        hash ^= num; // XOR to spread entropy
    }

    // Apply Fibonacci hashing for fast and even distribution
    let proxy_count = proxy_cnt.max(1); // Avoid div by zero
    ((hash.wrapping_mul(FIB_CONSTANT)) >> (64 - proxy_count.ilog2())) as usize % proxy_count
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        executor: E,
        store: Store<E>,
        rx_proxy_connections: Receiver<Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        rx_committed_txns: Receiver<Vec<RemoraTransaction<E>>>,
        tx_executor_local: Sender<RemoraTransaction<E>>,
        rx_states_sync: Receiver<ExecutionResults<E>>,
        updated_states_to_proxy: Arc<DashMap<ObjectID, SequenceNumber>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            executor,
            store,
            rx_proxy_connections,
            proxy_connections: Arc::new(DashMap::new()),
            rx_committed_txns,
            index: 0,
            tx_executor_local,
            rx_states_sync,
            updated_states_to_proxy,
            metrics,
        }
    }

    /// Get assigned proxies for shared objects in a transaction.
    fn get_proxies_for_shared_objects(
        proxy_connections: Arc<
            DashMap<ExecutorIndex, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        shared_object_ids: &[ObjectID],
    ) -> HashSet<ExecutorIndex> {
        shared_object_ids
            .iter()
            .map(|id| lb_hash(proxy_connections.len(), id))
            .collect()
    }

    /// Prepare state updates based on sharding
    fn prepare_state_updates(
        &mut self,
        execution_result: ExecutionResults<E>,
    ) -> FxHashMap<ExecutorIndex, NewStates> {
        // HashMap to hold the updates for each executor
        let mut updates_by_executor: FxHashMap<ExecutorIndex, NewStates> = FxHashMap::default();

        tracing::info!(
            "primary: prepared updates {:?}",
            execution_result
                .new_state
                .clone()
                .unwrap()
                .iter()
                .map(|(&oid, o)| (oid, o.compute_object_reference().1))
                .collect::<Vec<_>>()
        );
        for (object_id, object) in execution_result.new_state.unwrap() {
            // filter out gas objects
            if object_id != execution_result.updates.as_ref().unwrap().gas_object().0 .0 {
                let executor_id = lb_hash(self.proxy_connections.len(), &object_id);
                let entry = updates_by_executor.entry(executor_id).or_default();
                entry.insert(object_id, object.clone());

                // update the updated_states_to_proxy metadata
                let object_version = object.compute_object_reference().1;
                match self.updated_states_to_proxy.get_mut(&object_id) {
                    Some(mut already_updated_v) => {
                        *already_updated_v = object_version;
                    }
                    None => {
                        self.updated_states_to_proxy
                            .insert(object_id, object_version);
                    }
                }
            }
        }

        updates_by_executor
    }

    /// Determines the correct forwarding target for a transaction.
    async fn forward_txn_to_proxy(
        transaction: RemoraTransaction<E>,
        mut proxy_connections: Arc<
            DashMap<ExecutorIndex, Sender<PrimaryToProxyMessage<<E as Executor>::Transaction>>>,
        >,
        tx_executor_local: Sender<RemoraTransaction<E>>,
        store: Store<E>,
        executor: E,
        updated_states_to_proxy: Arc<DashMap<ObjectID, SequenceNumber>>,
    ) {
        // If no proxies exist, send to the local executor.
        if proxy_connections.is_empty() {
            if tx_executor_local.send(transaction).await.is_err() {
                tracing::warn!("Failed to send transaction to the local executor");
            }
            return;
        }

        let ctx = executor.context();
        let store = store.clone();
        let objs =
            E::get_objects_for_dependency_tracking(ctx.clone(), store.clone(), transaction.clone());
        tracing::info!("transaction from consensus {:?}", objs.clone());

        let mut index = 0;
        if objs.is_empty() {
            // No shared objects, use round-robin for proxy selection.
            let proxy_index = index % proxy_connections.len();
            index += 1;

            if proxy_connections
                .get(&proxy_index)
                .unwrap()
                .send(PrimaryToProxyMessage::Txn(PrimaryToProxyTxn {
                    executor_cnt: proxy_connections.len(),
                    executor_idx: proxy_index,
                    txn: transaction,
                }))
                .await
                .is_ok()
            {
                tracing::debug!("Sent transaction to proxy {}", proxy_index);
            } else {
                tracing::warn!(
                    "Failed to send transaction to proxy {}, trying other proxies",
                    proxy_index
                );
                if proxy_connections.contains_key(&proxy_index) {
                    proxy_connections.remove(&proxy_index);
                    tracing::info!("Removed proxy connection at index {}", proxy_index);
                }
            }
            return;
        }

        let shared_object_ids: Vec<ObjectID> = objs.iter().map(|(id, _)| *id).collect();

        let assigned_proxies =
            Self::get_proxies_for_shared_objects(proxy_connections.clone(), &shared_object_ids);

        // To avoid blindly forwarding the consecutive xshard transactions
        // which very likely the proxies don't have the up-to-date view yet
        // so that these transactions will be bounced back again to the primary
        let mut should_forward = true;
        if assigned_proxies.len() > 1 {
            for (oid, required_v) in objs.iter() {
                if *required_v > SequenceNumber::from(2) {
                    if let Some(already_updated_v) = updated_states_to_proxy.get_mut(oid) {
                        if *already_updated_v < *required_v {
                            // the required states is ahead of proxy states
                            should_forward = false;
                        }
                    }
                }
            }
        }

        if !should_forward {
            tracing::info!("LB: sending to local executor {:?}", objs.clone());
            if tx_executor_local.send(transaction).await.is_err() {
                tracing::warn!("Failed to send transaction to local executor");
            }
        } else {
            if assigned_proxies.len() == 1 {
                tracing::info!("LB: sending to one proxy {:?}", objs.clone());
            } else {
                tracing::info!("LB: sending xshard {:?}", objs.clone());
            }

            // update view
            if assigned_proxies.len() == 1 && should_forward {
                for (oid, v) in objs.iter() {
                    match updated_states_to_proxy.get_mut(&oid) {
                        Some(mut already_updated_v) => {
                            *already_updated_v = v.next();
                        }
                        None => {
                            updated_states_to_proxy.insert(*oid, v.next());
                        }
                    }
                }
            }

            for &proxy_index in &assigned_proxies {
                if let Err(err) = proxy_connections
                    .get(&proxy_index)
                    .unwrap()
                    .send(PrimaryToProxyMessage::Txn(PrimaryToProxyTxn {
                        executor_cnt: proxy_connections.len(),
                        executor_idx: proxy_index,
                        txn: transaction.clone(),
                    }))
                    .await
                {
                    tracing::warn!(
                        "Failed to send transaction to proxy {}: {:?}",
                        proxy_index,
                        err
                    );
                }
            }
        }
    }

    /// Run the load balancer.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        Store<E>: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tracing::info!("Load balancer started");
        let mut txn_cnt = 0;

        loop {
            tokio::select! {
                Some(connection) = self.rx_proxy_connections.recv() => {
                    self.proxy_connections.insert(self.proxy_connections.len(), connection);
                        tracing::info!("Added a new proxy connection");
                    }

                Some(transactions) = self.rx_committed_txns.recv() => {
                    let executor = self.executor.clone();
                    let store = self.store.clone();
                    let tx_executor_local = self.tx_executor_local.clone();
                    let updated_states_to_proxy = self.updated_states_to_proxy.clone();
                    let metrics = self.metrics.clone();
                    let proxy_connections = self.proxy_connections.clone();

                    // offloading to another task to avoid blocking the channel below
                    tokio::spawn(async move {
                        // Assign shared objects version.
                        executor
                            .assign_shared_object_versions(
                                &transactions.iter().map(|tx| tx.deref().clone()).collect::<Vec<_>>()
                            )
                            .await;

                        txn_cnt += 1;
                        if txn_cnt == 1 {
                            metrics.register_start_time();
                        }

                        for transaction in transactions {
                            Self::forward_txn_to_proxy(transaction,
                                proxy_connections.clone(),
                                tx_executor_local.clone(),
                                store.clone(),
                                executor.clone(),
                                updated_states_to_proxy.clone()).await;
                        }
                    });
                }

                Some(result) = self.rx_states_sync.recv() => {
                    // send states updates to the proxy
                    if self.proxy_connections.is_empty() {
                        tracing::debug!("Skip states updating given no available other executors");
                        continue;
                    }

                    let states_updates = self.prepare_state_updates(result);
                    for (proxy_index, update) in states_updates {
                        match self.proxy_connections.get(&proxy_index).unwrap().send(PrimaryToProxyMessage::States(update)).await {
                            Ok(()) => {
                                tracing::debug!("Sent updates to proxy {}", proxy_index);
                            }
                            Err(_) => {
                                tracing::warn!("Failed to send states to proxy {}", proxy_index);
                                if self.proxy_connections.contains_key(&proxy_index) {
                                    self.proxy_connections.remove(&proxy_index);
                                    tracing::info!("Removed proxy connection at index {}", proxy_index);
                                }
                            }
                        }
                    }
                }

                else => Err(NodeError::ShuttingDown)?,
            }
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send,
        Store<E>: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tokio::spawn(async move { self.run().await })
    }
}
