// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use sui_test_transaction_builder::PublishData;
use sui_types::base_types::SuiAddress;

use crate::{
    benchmark_context::BenchmarkContext,
    command::WorkloadKind,
    tx_generator::{
        counter_tx_generator::CounterTxGenerator,
        variable_counter_tx_generator::VariableCounterTxGenerator,
        MoveTxGenerator,
        NonMoveTxGenerator,
        PackagePublishTxGenerator,
        TxGenerator,
    },
};

#[derive(Clone)]
pub struct Workload {
    tx_count: u64,
    workload_kind: WorkloadKind,
    stats: Option<(usize, HashMap<usize, Vec<usize>>)>,
}

impl Workload {
    pub fn new(tx_count: u64, workload_kind: WorkloadKind) -> Self {
        let stats = workload_kind.build_stats(tx_count as usize);
        Self {
            tx_count,
            workload_kind,
            stats,
        }
    }

    pub fn num_accounts(&self) -> u64 {
        match self.workload_kind {
            WorkloadKind::NoMove | WorkloadKind::PTB { .. } | WorkloadKind::Publish { .. } => {
                self.tx_count
            }
            WorkloadKind::Counter { txs_per_counter } => self.tx_count / txs_per_counter,
            WorkloadKind::SolanaTransactions => self
                .stats
                .as_ref()
                .map(|(distinct_objects, stats)| stats.keys().len().max(*distinct_objects) as u64)
                .unwrap(),
            WorkloadKind::EthereumTransfers => self
                .stats
                .as_ref()
                .map(|(distinct_objects, stats)| stats.keys().len().max(*distinct_objects) as u64)
                .unwrap(),
        }
    }

    pub(crate) fn gas_object_num_per_account(&self) -> u64 {
        self.workload_kind.gas_object_num_per_account()
    }

    pub async fn create_tx_generator(&self, ctx: &mut BenchmarkContext) -> Arc<dyn TxGenerator> {
        match &self.workload_kind {
            WorkloadKind::NoMove => Arc::new(NonMoveTxGenerator::new()),
            WorkloadKind::PTB {
                num_transfers,
                use_native_transfer,
                num_dynamic_fields,
                computation,
                num_shared_objects,
                num_mints,
                nft_size,
                use_batch_mint,
            } => {
                let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.extend(["move_package"]);
                let move_package = ctx.publish_package(PublishData::Source(path, false)).await;
                let root_objects = ctx
                    .preparing_dynamic_fields(move_package.0, *num_dynamic_fields)
                    .await;
                let shared_objects = ctx
                    .prepare_shared_objects(move_package.0, *num_shared_objects)
                    .await;
                Arc::new(MoveTxGenerator::new(
                    move_package.0,
                    *num_transfers,
                    *use_native_transfer,
                    *computation,
                    root_objects,
                    shared_objects,
                    *num_mints,
                    *nft_size,
                    *use_batch_mint,
                ))
            }
            WorkloadKind::Publish {
                manifest_file: manifest_path,
            } => Arc::new(PackagePublishTxGenerator::new(ctx, manifest_path.clone()).await),
            WorkloadKind::Counter { txs_per_counter } => {
                let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.extend(["move_package"]);
                let move_package = ctx.publish_package(PublishData::Source(path, false)).await;

                // generate counter objects
                let counter_objects = ctx
                    .prepare_shared_objects(move_package.0, self.num_accounts() as usize)
                    .await;

                let mut account_orders: HashMap<SuiAddress, usize> = HashMap::new();

                // Iterate over the values and assign a unique index to each
                for (idx, value) in ctx.get_accounts().keys().enumerate() {
                    account_orders.insert(*value, idx);
                }

                Arc::new(CounterTxGenerator::new(
                    move_package.0,
                    counter_objects,
                    account_orders,
                    *txs_per_counter,
                ))
            }
            WorkloadKind::SolanaTransactions | WorkloadKind::EthereumTransfers => {
                let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
                path.extend(["move_package"]);
                let move_package = ctx.publish_package(PublishData::Source(path, false)).await;

                let stats = &self.stats.as_ref().expect("Stats should be already built");

                // Generate counter objects. For internal implementation reasons, there must be at
                // least one account per shared object.
                let num_of_counters = stats.0;
                let counter_objects = ctx
                    .prepare_shared_objects(move_package.0, num_of_counters)
                    .await;

                // Iterate over the values and assign a unique index to each
                let mut account_orders: HashMap<SuiAddress, usize> = HashMap::new();
                for (idx, value) in ctx.get_accounts().keys().enumerate() {
                    account_orders.insert(*value, idx);
                }

                Arc::new(VariableCounterTxGenerator::new(
                    move_package.0,
                    counter_objects,
                    account_orders,
                    stats.1.clone(),
                ))
            }
        }
    }
}
