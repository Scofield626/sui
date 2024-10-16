// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    executor::{Executor, TransactionWithTimestamp},
    metrics::Metrics,
};

/// A load balancer is responsible for distributing transactions to the consensus and proxies.
pub struct LoadBalancer<E: Executor> {
    /// The receiver for transactions.
    rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
    /// The sender to forward transactions to the consensus.
    tx_consensus: Sender<TransactionWithTimestamp<E::Transaction>>,
}

impl<E: Executor> LoadBalancer<E> {
    /// Create a new load balancer.
    pub fn new(
        rx_transactions: Receiver<TransactionWithTimestamp<E::Transaction>>,
        tx_consensus: Sender<TransactionWithTimestamp<E::Transaction>>,
    ) -> Self {
        Self {
            rx_transactions,
            tx_consensus,
        }
    }

    /// Run the load balancer.
    pub async fn run(&mut self, metrics: Arc<Metrics>) {
        tracing::info!("Load balancer started");

        let mut i = 0;
        while let Some(transaction) = self.rx_transactions.recv().await {
            if i == 0 {
                metrics.register_start_time();
            }

            if self.tx_consensus.send(transaction.clone()).await.is_err() {
                tracing::warn!("Failed to send transaction to primary, stopping load balancer");
                break;
            }

            i += 1;
        }
    }

    /// Spawn the load balancer in a new task.
    pub fn spawn(mut self, metrics: Arc<Metrics>) -> JoinHandle<()>
    where
        E: 'static,
        <E as Executor>::Transaction: Send,
    {
        tokio::spawn(async move {
            self.run(metrics).await;
        })
    }
}
