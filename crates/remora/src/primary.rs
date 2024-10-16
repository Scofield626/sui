// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, ops::Deref, sync::Arc};

use dashmap::DashMap;
use sui_types::{
    base_types::{ObjectID, ObjectRef},
    digests::TransactionDigest,
    storage::ObjectStore,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    dependency_controller::DependencyController,
    executor::{
        ExecutableTransaction, ExecutionEffects, Executor, StateStore, TransactionWithTimestamp,
    },
    mock_consensus::ConsensusCommit,
};

/// The primary executor is responsible for executing transactions and merging the results
/// from the proxies.
pub struct PrimaryExecutor<E: Executor> {
    /// The executor for the transactions.
    executor: E,
    /// The object store.
    store: Arc<E::Store>,
    /// The receiver for consensus commits.
    rx_commits: Receiver<ConsensusCommit<TransactionWithTimestamp<E::Transaction>>>,
    /// The receiver for proxy results.
    rx_proxies: Receiver<ExecutionEffects<E::StateChanges>>,
    /// Output channel for the final results.
    tx_output: Sender<(
        TransactionWithTimestamp<E::Transaction>,
        ExecutionEffects<E::StateChanges>,
    )>,
    /// The dependency controller for multi-core tx execution.
    dependency_controller: DependencyController,
}

impl<E: Executor> PrimaryExecutor<E> {
    /// Create a new primary executor.
    pub fn new(
        executor: E,
        store: E::Store,
        rx_commits: Receiver<ConsensusCommit<TransactionWithTimestamp<E::Transaction>>>,
        rx_proxies: Receiver<ExecutionEffects<E::StateChanges>>,
        tx_output: Sender<(
            TransactionWithTimestamp<E::Transaction>,
            ExecutionEffects<E::StateChanges>,
        )>,
    ) -> Self {
        Self {
            executor,
            store: Arc::new(store),
            rx_commits,
            rx_proxies,
            tx_output,
            dependency_controller: DependencyController::new(),
        }
    }

    /// Get the input objects for a transaction.
    // TODO: This function should return an error when the input object is not found
    // or the input objects are malformed instead of panicking.
    fn get_input_objects(&self, transaction: &E::Transaction) -> HashMap<ObjectID, ObjectRef> {
        transaction
            .input_objects()
            .iter()
            .map(|kind| {
                self.store
                    .get_object(&kind.object_id())
                    .expect("Failed to read objects from store")
                    .map(|object| (object.id(), object.compute_object_reference()))
                    .expect("Input object not found") // TODO: Return error instead of panic
            })
            .collect()
    }

    /// Merge the results from the proxies and re-execute the transaction if necessary.
    // TODO: Naive merging strategy for now.
    pub async fn merge_results(
        &mut self,
        proxy_results: &DashMap<TransactionDigest, ExecutionEffects<E::StateChanges>>,
        transaction: &TransactionWithTimestamp<E::Transaction>,
    ) -> ExecutionEffects<E::StateChanges> {
        let mut skip = true;

        if let Some((_, proxy_result)) = proxy_results.remove(transaction.deref().digest()) {
            let initial_state = self.get_input_objects(transaction);
            for (id, vid) in &proxy_result.modified_at_versions() {
                let (_, v, _) = initial_state
                    .get(id)
                    .expect("Transaction's inputs already checked");
                if v != vid {
                    skip = false;
                }
            }
            if skip {
                let effects = proxy_result.clone();
                self.store
                    .commit_objects(effects.changes, effects.new_state);
                return proxy_result;
            }
        }

        tracing::trace!("Re-executing transaction");
        self.executor.execute(&self.store, transaction).await
    }

    /// Run the primary executor.
    pub async fn run(&mut self)
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send,
    {
        let proxy_results = DashMap::new();

        loop {
            tokio::select! {
            // Receive a commit from the consensus.
            Some(commit) = self.rx_commits.recv() => {
                tracing::debug!("Received commit");
                let mut task_id = 0;
                for tx in commit {
                    //let results = self.merge_results(&proxy_results, &tx).await;
                    task_id += 1;
                    let (prior_handles, current_handles) = self
                        .dependency_controller
                        .get_dependencies(task_id, tx.input_object_ids());

                    let store = self.store.clone();
                    let tx_results = self.tx_output.clone();
                    let ctx = self.executor.get_context();
                    let ctx = ctx.clone();
                    tokio::spawn(async move {
                        for prior_notify in prior_handles {
                            prior_notify.notified().await;
                        }

                        let execution_result = E::exec_on_ctx(ctx, store, tx.clone()).await;

                        for notify in current_handles {
                            notify.notify_one();
                        }

                        if tx_results.send((tx, execution_result)).await.is_err() {
                            tracing::warn!("Failed to send execution result, stopping primary");
                        }
                    });
                }
            }

            // Receive a execution result from a proxy.
            Some(proxy_result) = self.rx_proxies.recv() => {
                proxy_results.insert(
                    *proxy_result.transaction_digest(),
                    proxy_result
                );
                tracing::debug!("Received proxy result");
                }
            }
        }
    }

    /// Spawn the primary executor in a new task.
    pub fn spawn(mut self) -> JoinHandle<()>
    where
        E: Send + 'static,
        <E as Executor>::Store: Send + Sync,
        <E as Executor>::Transaction: Send + Sync,
        <E as Executor>::StateChanges: Send + Sync,
    {
        tokio::spawn(async move {
            self.run().await;
        })
    }
}

#[cfg(test)]
mod tests {

    use tokio::sync::mpsc;

    use crate::{
        config::BenchmarkConfig,
        executor::{generate_transactions, Executor, SuiExecutor, SuiTransactionWithTimestamp},
        primary::PrimaryExecutor,
    };

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn merge_results() {
        let (tx_commit, rx_commit) = mpsc::channel(100);
        let (tx_results, rx_results) = mpsc::channel(100);
        let (tx_output, mut rx_output) = mpsc::channel(100);

        // Generate transactions.
        let config = BenchmarkConfig::new_for_tests();
        let mut executor = SuiExecutor::new(&config).await;
        let transactions: Vec<_> = generate_transactions(&config)
            .await
            .into_iter()
            .map(|tx| SuiTransactionWithTimestamp::new_for_tests(tx))
            .collect();
        let total_transactions = transactions.len();

        // Pre-execute the transactions.
        let mut proxy_results = Vec::new();
        let proxy_store = executor.create_in_memory_store();
        for tx in transactions.clone() {
            proxy_results.push(executor.execute(&proxy_store, &tx).await);
        }

        // Boot the primary executor.
        let store = executor.create_in_memory_store();
        PrimaryExecutor::new(executor, store, rx_commit, rx_results, tx_output).spawn();

        // Merge the proxy results into the primary.
        for r in proxy_results {
            tx_results.send(r).await.unwrap();
        }
        tokio::task::yield_now().await;

        // Send the transactions to the primary executor.
        tx_commit.send(transactions).await.unwrap();

        // Check the results.
        for _ in 0..total_transactions {
            let (_, result) = rx_output.recv().await.unwrap();
            assert!(result.success());
        }
    }
}
