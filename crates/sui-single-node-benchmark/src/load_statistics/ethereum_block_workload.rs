// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rand::Rng;
use rand_distr::{Distribution, WeightedIndex};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EthereumBlockData {
    pub description: String,
    pub zipfian_threshold: f64,
    pub blocks: Vec<BlockData>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BlockData {
    pub block_number: u64,
    pub block_hex: String,
    pub contention_level: String,
    pub top_10_hotspots: Vec<Hotspot>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Hotspot {
    pub rank: u32,
    pub object_id: String,
    pub percentage: f64,
}

/// Dynamic workload state for object ID mapping across blocks
#[derive(Debug, Clone)]
pub struct DynamicEthereumWorkload {
    block_data: EthereumBlockData,
    object_id_to_index: HashMap<String, usize>,
}

impl DynamicEthereumWorkload {
    /// Create a new dynamic workload with object ID mapping
    pub fn new() -> Self {
        let block_data = load_ethereum_block_data();
        let mut object_id_to_index = HashMap::new();
        
        // Create a global mapping from object_id to index across all blocks
        let mut global_index = 0;
        for block in &block_data.blocks {
            for hotspot in &block.top_10_hotspots {
                if !object_id_to_index.contains_key(&hotspot.object_id) {
                    object_id_to_index.insert(hotspot.object_id.clone(), global_index);
                    global_index += 1;
                }
            }
        }
        
        Self {
            block_data,
            object_id_to_index,
        }
    }
    
    /// Get all available blocks
    pub fn get_blocks(&self) -> &[BlockData] {
        &self.block_data.blocks
    }
    
    /// Generate objects from a specific block using the global object ID mapping
    pub fn generate_objects_from_block<R: Rng>(&self, rng: &mut R, block: &BlockData) -> Vec<usize> {
        // Create weighted distribution for this block's hot objects
        let hot_weights: Vec<f64> = block.top_10_hotspots.iter()
            .map(|h| h.percentage)
            .collect();
        
        let total_hot_percentage: f64 = hot_weights.iter().sum();
        let hot_dist = WeightedIndex::new(&hot_weights)
            .expect("Weights should be non-negative and not all zero");
        
        // Transfer only - 2 objects
        let num_objects = 2;
        let mut selected_objects = Vec::new();
        
        for _ in 0..num_objects {
            let random_value = rng.gen_range(0.0..100.0);
            
            if random_value < total_hot_percentage {
                // Select from hot objects based on their weights
                let hot_idx = hot_dist.sample(rng);
                let hotspot = &block.top_10_hotspots[hot_idx];
                
                // Get the global index for this object ID
                if let Some(&global_idx) = self.object_id_to_index.get(&hotspot.object_id) {
                    selected_objects.push(global_idx);
                } else {
                    // Fallback to random if object ID not found
                    let random_idx = rng.gen_range(10..10000000);
                    selected_objects.push(random_idx);
                }
            } else {
                // Select from the rest of the keyspace (random)
                let random_idx = rng.gen_range(10..10000000);
                selected_objects.push(random_idx);
            }
        }
        
        tracing::debug!(
            "Dynamic workload - Block {} objects: {:?} (hot percentage: {:.2}%)",
            block.block_number,
            selected_objects,
            total_hot_percentage
        );
        
        selected_objects
    }
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
