// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, path::PathBuf};

use dashmap::DashMap;
use rand_chacha::ChaCha8Rng;
use rayon::prelude::*;

use clap::{Parser, Subcommand, ValueEnum};
use rand::SeedableRng;
use strum_macros::EnumIter;

#[derive(Parser)]
#[clap(
    name = "sui-single-node-benchmark",
    about = "Benchmark a single validator node",
    rename_all = "kebab-case",
    author,
    version
)]
pub struct Command {
    #[arg(
        long,
        default_value_t = 500000,
        help = "Number of transactions to submit"
    )]
    pub tx_count: u64,
    #[arg(
        long,
        default_value_t = 100,
        help = "Number of transactions in a consensus commit/checkpoint"
    )]
    pub checkpoint_size: usize,
    #[arg(
        long,
        help = "Whether to print out a sample transaction and effects that is going to be benchmarked on"
    )]
    pub print_sample_tx: bool,
    #[arg(
        long,
        default_value_t = false,
        help = "If true, skip signing on the validators, instead, creating certificates directly using validator secrets"
    )]
    pub skip_signing: bool,
    #[arg(
        long,
        default_value = "baseline",
        ignore_case = true,
        help = "Which component to benchmark"
    )]
    pub component: Component,
    #[clap(subcommand)]
    pub workload: WorkloadKind,
}

#[derive(Copy, Clone, EnumIter, ValueEnum)]
pub enum Component {
    ExecutionOnly,
    /// Baseline includes the execution and storage layer only.
    Baseline,
    /// On top of Baseline, this schedules transactions through the transaction manager.
    WithTxManager,
    /// This goes through the `handle_certificate` entry point on authority_server, which includes
    /// certificate verification, transaction manager, as well as a noop consensus layer. The noop
    /// consensus layer does absolutely nothing when receiving a transaction in consensus.
    ValidatorWithoutConsensus,
    /// Similar to ValidatorWithNoopConsensus, but the consensus layer contains a fake consensus
    /// protocol that basically sequences transactions in order. It then verify the transaction
    /// and store the sequenced transactions into the store. It covers the consensus-independent
    /// portion of the code in consensus handler.
    ValidatorWithFakeConsensus,
    /// Benchmark only validator signing component: `handle_transaction`.
    TxnSigning,
    /// Benchmark the checkpoint executor by constructing a full epoch of checkpoints, execute
    /// all transactions in them and measure time.
    CheckpointExecutor,
    /// Send transactions to a channel instead of executing them.
    PipeTxsToChannel,
}

#[derive(Subcommand, Clone)]
pub enum WorkloadKind {
    NoMove,
    PTB {
        #[arg(
            long,
            default_value_t = 0,
            help = "Number of address owned input objects per transaction.\
                This represents the amount of DB reads per transaction prior to execution."
        )]
        num_transfers: u64,
        #[arg(
            long,
            default_value_t = false,
            help = "When transferring an object, whether to use native TransferObjecet command, or to use Move code for the transfer"
        )]
        use_native_transfer: bool,
        #[arg(
            long,
            default_value_t = 0,
            help = "Number of dynamic fields read per transaction.\
            This represents the amount of runtime DB reads per transaction during execution."
        )]
        num_dynamic_fields: u64,
        #[arg(
            long,
            default_value_t = 0,
            help = "Computation intensity per transaction.\
            The transaction computes the n-th Fibonacci number \
            specified by this parameter * 100."
        )]
        computation: u8,
        #[arg(
            long,
            default_value_t = 0,
            help = "Whether to use shared objects in the transaction.\
            If 0, no shared objects will be used.\
            Otherwise `v` shared objects will be created and each transaction will use these `v` shared objects."
        )]
        num_shared_objects: usize,
        #[arg(
            long,
            default_value_t = 0,
            help = "How many NFTs to mint/transfer during the transaction.\
            If 0, no NFTs will be minted.\
            Otherwise `v` NFTs with the specified size will be created and transferred to the sender"
        )]
        num_mints: u16,
        #[arg(
            long,
            default_value_t = 32,
            help = "Size of the Move contents of the NFT to be minted, in bytes.\
            Defaults to 32 bytes (i.e., NFT with ID only)."
        )]
        nft_size: u16,
        #[arg(
            long,
            help = "If true, call a single batch_mint Move function.\
            Otherwise, batch via a PTB with multiple commands"
        )]
        use_batch_mint: bool,
    },
    Publish {
        #[arg(
            long,
            help = "Path to the manifest file that describe the package dependencies.\
            Follow examples in the tests directory to see how to set up the manifest file.\
            The manifest file is a json file that contains a list of dependent packages that need to\
            be published first, as well as the root package that will be benchmarked on. Each package\
            can be either in source code or bytecode form. If it is in source code form, the benchmark\
            will compile the package first before publishing it."
        )]
        manifest_file: PathBuf,
    },
    Counter {
        #[arg(
            long,
            default_value_t = 1,
            help = "Number of times each counter is incremented (a measure of contention)."
        )]
        txs_per_counter: u64,
    },
    SolanaTransactions,
    EthereumTransfers,
    EthereumNftMint,
    UniswapNormal,
    UniswapPeak,
}

/// Parallelized transaction processing with deterministic RNG
fn build_stats_common<F>(
    tx_count: usize,
    tx_generator: F,
) -> Option<(usize, HashMap<usize, Vec<usize>>)>
where
    F: Fn(&mut ChaCha8Rng) -> Vec<usize> + Send + Sync,
{
    let num_threads = num_cpus::get();
    let tx_batch_size = (tx_count / num_threads).max(10_000);
    let tx_batches = (tx_count + tx_batch_size - 1) / tx_batch_size;

    tracing::debug!("Total Transactions: {}", tx_count);
    tracing::debug!("Using {} Threads", num_threads);
    tracing::debug!("Batch Size: {}", tx_batch_size);
    tracing::debug!("Total Batches: {}", tx_batches);

    let object_ids_map = DashMap::new();
    let next_object_id = AtomicUsize::new(0);

    // Generate transactions in parallel
    let stats: HashMap<usize, Vec<usize>> = (0..tx_batches)
        .into_par_iter()
        .flat_map(|batch_id| {
            let mut rng = ChaCha8Rng::seed_from_u64(0);
            rng.set_stream(batch_id as u64);

            let mut batch_stats = Vec::new();
            let start_tx_id = batch_id * tx_batch_size;
            let end_tx_id = ((batch_id + 1) * tx_batch_size).min(tx_count);

            for tx_id in start_tx_id..end_tx_id {
                let objects = tx_generator(&mut rng);

                let object_ids: Vec<usize> = objects
                    .into_iter()
                    .map(|obj| {
                        *object_ids_map
                            .entry(obj)
                            .or_insert_with(|| next_object_id.fetch_add(1, Ordering::SeqCst))
                    })
                    .collect();

                batch_stats.push((tx_id, object_ids));
            }

            batch_stats
        })
        .collect();

    let num_of_distinct_objects = object_ids_map.len();
    Some((num_of_distinct_objects, stats))
}

impl WorkloadKind {
    pub(crate) fn gas_object_num_per_account(&self) -> u64 {
        match self {
            // Each transaction will always have 1 gas object, plus the number of owned objects that will be transferred.
            Self::NoMove => 1,
            Self::PTB { num_transfers, .. } => *num_transfers + 1,
            Self::Publish { .. } => 1,
            Self::Counter { txs_per_counter } => *txs_per_counter,
            Self::SolanaTransactions => 1,
            Self::EthereumTransfers => 1,
            Self::EthereumNftMint => 1,
            Self::UniswapNormal => 1,
            Self::UniswapPeak => 1,
        }
    }

    /// Returns the number of accounts that will be used in the workload and workload-specific stats.
    pub(crate) fn build_stats(
        &self,
        tx_count: usize,
    ) -> Option<(usize, HashMap<usize, Vec<usize>>)> {
        match self {
            Self::SolanaTransactions => build_stats_common(tx_count, |mut rng| {
                let (inputs, _) = crate::load_statistics::solana_concurrency(&mut rng);
                inputs
            }),
            Self::EthereumTransfers => build_stats_common(tx_count, |rng| {
                let (sender, recipient) = crate::load_statistics::ethereum_transfers(rng);
                vec![sender, recipient]
            }),
            Self::EthereumNftMint => build_stats_common(tx_count, |mut rng| {
                let (nft, minter) = crate::load_statistics::ethereum_nft_mint(&mut rng);
                vec![nft, minter]
            }),
            Self::UniswapNormal => build_stats_common(tx_count, |mut rng| {
                let coin_pair = crate::load_statistics::ethereum_uniswap_normal(&mut rng);
                vec![coin_pair]
            }),
            Self::UniswapPeak => build_stats_common(tx_count, |mut rng| {
                let coin_pair = crate::load_statistics::ethereum_uniswap_peak(&mut rng);
                vec![coin_pair]
            }),
            WorkloadKind::NoMove
            | WorkloadKind::PTB { .. }
            | WorkloadKind::Publish { .. }
            | WorkloadKind::Counter { .. } => None,
        }
    }
}
