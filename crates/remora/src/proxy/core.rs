// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use sui_types::base_types::{ObjectID, SequenceNumber};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Notify,
    },
    task::JoinHandle,
};

use crate::{
    error::{NodeError, NodeResult},
    executor::{
        api::{
            ExecutionResults, Executor, PrimaryToProxyMessage,
            RemoraTransaction, StateStore, Store,
        },
        versioned_dependency_controller::VersionedDependencyController,
    },
    metrics::Metrics,
    primary::load_balancer::lb_hash,
};

pub type ProxyId = String;

#[derive(Clone, Copy)]
pub enum ProxyMode {
    SingleThreaded,
    MultiThreaded,
}

/// A proxy is responsible for pre-executing transactions.
pub struct ProxyCore<E: Executor> {
    /// The ID of the proxy.
    id: ProxyId,
    /// The executor for the transactions.
    executor: E,
    /// The mode of proxy (parallel or sequential).
    mode: ProxyMode,
    /// The object store.
    store: Store<E>,
    /// The receiver for transactions.
    rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
    /// The sender for transactions with results.
    tx_results: Sender<ExecutionResults<E>>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: Option<Arc<VersionedDependencyController>>,
    /// The  metrics for the proxy
    metrics: Arc<Metrics>,
}

impl<E: Executor> ProxyCore<E> {
    /// Create a new proxy.
    pub fn new(
        id: ProxyId,
        executor: E,
        mode: ProxyMode,
        store: Store<E>,
        rx_transactions: Receiver<PrimaryToProxyMessage<<E as Executor>::Transaction>>,
        tx_results: Sender<ExecutionResults<E>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let dependency_controller = match mode {
            ProxyMode::MultiThreaded => Some(Arc::new(VersionedDependencyController::new())),
            ProxyMode::SingleThreaded => None,
        };

        Self {
            id,
            executor,
            mode,
            store,
            rx_transactions,
            tx_results,
            dependency_controller,
            metrics,
        }
    }

    /// Run the proxy.
    pub async fn run(&mut self) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        tracing::info!("Proxy {} started", self.id);
        match self.mode {
            ProxyMode::SingleThreaded => {
                while let Some(message) = self.rx_transactions.recv().await {
                    match message {
                        PrimaryToProxyMessage::Txn(transaction) => {
                            let transaction = transaction.txn;

                            // Assign shared objects version.
                            self.executor
                                .assign_shared_object_versions(&[transaction.deref().clone()])
                                .await;

                            self.metrics.increase_proxy_load(&self.id);

                            let ctx = self.executor.context().clone();
                            let store = self.store.clone();
                            if !E::pre_execute_check_objects(store.clone(), &transaction) {
                                E::optimistically_pre_generate_objects(store.clone(), &transaction);
                            }

                            let execution_result =
                                E::execute(ctx, store.clone(), transaction).await;

                            self.metrics.decrease_proxy_load(&self.id);
                            if self.tx_results.send(execution_result).await.is_err() {
                                tracing::warn!(
                                    "Failed to send execution result, stopping proxy {}",
                                    self.id
                                );
                                break;
                            }
                        }

                        PrimaryToProxyMessage::States(states) => {
                            self.store.commit_new_objects(states);
                        }
                    }
                }
            }

            ProxyMode::MultiThreaded => {
                let mut task_id = 0;
                loop {
                    tokio::select! {
                        Some(message) = self.rx_transactions.recv() => {
                            match message {
                                PrimaryToProxyMessage::Txn(transaction) => {
                                    let executor_idx = transaction.executor_idx;
                                    let executor_cnt = transaction.executor_cnt;
                                    let transaction = transaction.txn;

                                    // Assign shared objects version.
                                    self.executor.assign_shared_object_versions(&[transaction.deref().clone()]).await;

                                    if task_id == 0 {
                                        self.metrics.register_start_time();
                                    }
                                    task_id += 1;
                                    self.metrics.increase_proxy_load(&self.id);

                                    if !E::pre_execute_check_objects(self.store.clone(), &transaction) {
                                        E::optimistically_pre_generate_objects(self.store.clone(), &transaction);
                                    }

                                    let (objs, prior_handles, current_handles, xshard) = self.get_dependencies(transaction.clone(), task_id, executor_idx, executor_cnt);
                                    self.schedule_txn_parallel(transaction, objs, prior_handles, current_handles, xshard).await.expect("Failed to schedule transaction");
                                }

                                PrimaryToProxyMessage::States(states) => {
                                    let objs = states.iter().map(|(oid, o)| (*oid, o.compute_object_reference().1)).collect();
                                    let (prior_handles, current_handles) = self.dependency_controller.clone().unwrap().get_prior_dependency_and_update(task_id, objs);
                                    let store = self.store.clone();
                                    tokio::spawn(async move {
                                        for prior_notify in prior_handles {
                                            prior_notify.notified().await;
                                        }
                                        store.commit_new_objects(states);
                                        for notify in current_handles {
                                            notify.notify_one();
                                        }
                                    });
                                }
                            }
                        }
                        else => Err(NodeError::ShuttingDown)?
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_dependencies(
        &mut self,
        transaction: RemoraTransaction<E>,
        task_id: u64,
        executor_index: usize,
        executor_cnt: usize,
    ) -> (
        Vec<(ObjectID, SequenceNumber)>,
        Vec<Arc<Notify>>,
        Vec<Arc<Notify>>,
        bool, // xshard
    ) {
        let objs = E::get_objects_for_dependency_tracking(
            self.executor.context().clone(),
            self.store.clone(),
            transaction.clone(),
        );

        let filtered_objs: Vec<_> = objs
        .iter()
        // Filter out the objs that do not belong to this proxy
        .filter(|(id, _seq)| lb_hash(executor_cnt, id) == executor_index)
        .cloned()
        .collect();

        let (prior_handles, current_handles) = self
            .dependency_controller
            .clone()
            .unwrap()
            .get_prior_dependency_and_update(task_id, filtered_objs.clone());

        (objs.clone(), prior_handles, current_handles, (filtered_objs.len() < objs.len()))
    }

    pub async fn schedule_txn_parallel(
        &mut self,
        transaction: RemoraTransaction<E>,
        objs: Vec<(ObjectID, SequenceNumber)>,
        prior_handles: Vec<Arc<Notify>>,
        current_handles: Vec<Arc<Notify>>,
        xshard: bool,
    ) -> NodeResult<()>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let store = self.store.clone();
        let id = self.id.clone();
        let tx_results = self.tx_results.clone();
        let ctx = self.executor.context().clone();
        let metrics = self.metrics.clone();
        let dependency_controller = self.dependency_controller.clone().unwrap().clone();
        tokio::spawn(async move {
            for prior_notify in prior_handles {
                prior_notify.notified().await;
            }

            let mut latest_states = BTreeMap::new();

            if xshard {
                for (oid, _) in objs.iter() {
                    latest_states.insert(*oid, store.read_object(oid).unwrap().unwrap());
                }
            }

            let execution_result = if !xshard {
                dependency_controller.remove_dependency(objs);
                E::execute(ctx, store, transaction.clone()).await
            } else {
                tracing::warn!("Proxy skipped execution due to xshard txn");
                ExecutionResults::<E>::new(transaction.clone(), None, Some(latest_states))
            };

            tx_results
                .send(execution_result)
                .await
                .map_err(|_| NodeError::ShuttingDown)?;

            for notify in current_handles {
                notify.notify_one();
            }

            metrics.decrease_proxy_load(&id);
            metrics.update_metrics(transaction.timestamp());
            Ok::<_, NodeError>(())
        });
        Ok(())
    }

    /// Sptransaction_awn the proxy in a new task.
    pub fn spawn(mut self) -> JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::Transaction: Send,
    {
        tokio::spawn(async move { self.run().await })
    }

    pub fn spawn_with_threads(mut self) -> std::thread::JoinHandle<NodeResult<()>>
    where
        E: Send + 'static,
        Store<E>: Send + Sync,
        RemoraTransaction<E>: Send + Sync,
        ExecutionResults<E>: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
    {
        let num_threads = num_cpus::get();

        // spawn the custom runtime in a dedicated thread to ensure active
        std::thread::spawn(move || {
            // Build a custom Tokio runtime with the specified number of worker threads
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(num_threads)
                .enable_all()
                .build()
                .unwrap();

            // Block on the runtime to keep it alive and process tasks
            rt.block_on(async move {
                let _ = self.run().await;
            });
            Ok::<_, NodeError>(())
        })
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkParameters,
        executor::{
            api::{Executor, PrimaryToProxyMessage, RemoraTransaction},
            fake::FakeExecutor,
            sui::SuiExecutor,
        },
        metrics::Metrics,
        proxy::core::{ProxyCore, ProxyMode},
    };

    async fn pre_execute<E: Executor + Send + 'static>(
        mode: ProxyMode,
        executor: E,
        config: BenchmarkParameters,
    ) where
        <E as Executor>::ExecutionResults: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::ExecutionContext: Send + Sync,
        <E as Executor>::Store: Send + Sync,
    {
        let (tx_proxy, rx_proxy) = mpsc::channel(100);
        let (tx_results, mut rx_results) = mpsc::channel(100);

        let store = Arc::new(executor.init_store());
        let metrics = Arc::new(Metrics::new_for_tests());
        let proxy_id = "0".to_string();
        let proxy = ProxyCore::<E>::new(
            proxy_id, executor, mode, store, rx_proxy, tx_results, metrics,
        );

        // Send transactions to the proxy.
        let transactions = E::generate_transactions(&config, None).await;
        for tx in transactions {
            let transaction = RemoraTransaction::<E>::new_for_tests(tx);
            let message = PrimaryToProxyMessage::Txn(crate::executor::api::PrimaryToProxyTxn {
                executor_idx: 0,
                executor_cnt: 1,
                txn: transaction,
            });
            tx_proxy.send(message).await.unwrap();
        }

        // Spawn the proxy.
        proxy.spawn();

        // Receive the results.
        let results = rx_results.recv().await.unwrap();
        assert!(results.success());
    }

    #[tokio::test]
    async fn test_single_threaded_proxy() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        pre_execute::<SuiExecutor>(ProxyMode::SingleThreaded, executor, config).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy() {
        let config = BenchmarkParameters::new_for_tests();
        let executor = SuiExecutor::new(&config).await;
        pre_execute::<SuiExecutor>(ProxyMode::MultiThreaded, executor, config).await;
    }

    #[tokio::test]
    async fn test_single_threaded_proxy_fake_transactions() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(ProxyMode::SingleThreaded, executor, config).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy_fake_transactions() {
        let config = BenchmarkParameters::new_for_fake_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(ProxyMode::MultiThreaded, executor, config).await;
    }

    #[tokio::test]
    async fn test_single_threaded_proxy_fake_transactions_contention() {
        let config = BenchmarkParameters::new_for_fake_contention_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(ProxyMode::SingleThreaded, executor, config).await;
    }

    #[tokio::test]
    async fn test_multi_threaded_proxy_fake_transactions_contention() {
        let config = BenchmarkParameters::new_for_fake_contention_tests();
        let executor = FakeExecutor::new(&config).await;
        pre_execute::<FakeExecutor>(ProxyMode::MultiThreaded, executor, config).await;
    }
}
