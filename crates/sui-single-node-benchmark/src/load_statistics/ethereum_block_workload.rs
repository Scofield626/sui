// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::Rng;
use rand_distr::{Distribution, WeightedIndex};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct EthereumBlockData {
    pub description: String,
    pub zipfian_threshold: f64,
    pub blocks: Vec<BlockData>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BlockData {
    pub block_number: u64,
    pub block_hex: String,
    pub contention_level: String,
    pub top_10_hotspots: Vec<Hotspot>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Hotspot {
    pub rank: u32,
    pub object_id: String,
    pub percentage: f64,
}

/// Load the Ethereum block data from the JSON file
pub fn load_ethereum_block_data() -> EthereumBlockData {
    let json_str = include_str!("ethereum_block_data.json");
    serde_json::from_str(json_str).expect("Failed to parse ethereum_block_data.json")
}

/// Generate object selection based on a specific block's hotspot data
fn generate_objects_from_block<R: Rng>(rng: &mut R, block: &BlockData) -> Vec<usize> {
    // Create weighted distribution for this block's hot objects
    let hot_weights: Vec<f64> = block.top_10_hotspots.iter()
        .map(|h| h.percentage)
        .collect();
    
    let total_hot_percentage: f64 = hot_weights.iter().sum();
    let hot_dist = WeightedIndex::new(&hot_weights)
        .expect("Weights should be non-negative and not all zero");
    
    // Transfer only
    let num_objects = 2;
    let mut selected_objects = Vec::new();
    
    for _ in 0..num_objects {
        let random_value = rng.gen_range(0.0..100.0);
        
        if random_value < total_hot_percentage {
            // Select from hot objects based on their weights
            let hot_idx = hot_dist.sample(rng);
            selected_objects.push(hot_idx);
        } else {
            // Select from the rest of the keyspace (random)
            // We'll use a range that doesn't overlap with hot object indices
            let random_idx = rng.gen_range(10..10000000); // Assuming keyspace goes beyond hot objects
            selected_objects.push(random_idx);
        }
    }
    
    tracing::debug!(
        "Sampled Ethereum block {} objects: {:?} (hot percentage: {:.2}%)",
        block.block_number,
        selected_objects,
        total_hot_percentage
    );
    
    selected_objects
}

/// Generate object selection for Ethereum block workload based on the first block
pub fn ethereum_block_workload<R: Rng>(rng: &mut R) -> Vec<usize> {
    let block_data = load_ethereum_block_data();
    let first_block = &block_data.blocks[0]; // Use the first block as requested
    generate_objects_from_block(rng, first_block)
}

/// Generate object selection for a specific block by block number
pub fn ethereum_block_workload_by_block<R: Rng>(rng: &mut R, block_number: u64) -> Vec<usize> {
    let block_data = load_ethereum_block_data();
    
    // Find the block with the specified block number
    let block = block_data.blocks.iter()
        .find(|b| b.block_number == block_number)
        .expect(&format!("Block {} not found in data", block_number));
    
    generate_objects_from_block(rng, block)
}
