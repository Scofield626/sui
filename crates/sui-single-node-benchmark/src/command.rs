// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, path::PathBuf};

use clap::{Parser, Subcommand, ValueEnum};
use rand::{rngs::StdRng, SeedableRng};
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
}

impl WorkloadKind {
    pub(crate) fn gas_object_num_per_account(&self) -> u64 {
        match self {
            // Each transaction will always have 1 gas object, plus the number of owned objects that will be transferred.
            WorkloadKind::NoMove => 1,
            WorkloadKind::PTB { num_transfers, .. } => *num_transfers + 1,
            WorkloadKind::Publish { .. } => 1,
            WorkloadKind::Counter { txs_per_counter } => *txs_per_counter,
            WorkloadKind::SolanaTransactions => 1,
        }
    }

    /// Returns the number of accounts that will be used in the workload and workload-specific stats.
    pub(crate) fn build_stats(
        &self,
        tx_count: usize,
    ) -> Option<(usize, HashMap<usize, Vec<usize>>)> {
        let mut rng = StdRng::seed_from_u64(0);

        match self {
            Self::SolanaTransactions => {
                // Maps transaction ids to the object digests they access.
                let mut stats = HashMap::new();

                // Maps raw object digests to consecutive object ids.
                let mut object_ids_map = HashMap::new();
                let mut next_object_id = 0;

                for tx_id in 0..tx_count {
                    let (inputs, _) = crate::load_statistics::solana_concurrency(&mut rng);
                    for input in &inputs {
                        object_ids_map.entry(*input).or_insert_with(|| {
                            let id = next_object_id;
                            next_object_id += 1;
                            id
                        });
                    }
                    stats.insert(tx_id, inputs);
                }

                // Convert raw object digests to object ids.
                let stats: HashMap<usize, _> = stats
                    .into_iter()
                    .map(|(tx_id, inputs)| {
                        let inputs = inputs
                            .into_iter()
                            .map(|input| *object_ids_map.get(&input).unwrap())
                            .collect();
                        (tx_id, inputs)
                    })
                    .collect();

                let num_of_distinct_objects = object_ids_map.len();
                Some((num_of_distinct_objects, stats))
            }
            _ => None,
        }
    }
}
