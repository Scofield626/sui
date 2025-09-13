use mixed_workload::{GAS_COST_DISTRIBUTION, MIXED_DISTRIBUTION, WRITE_LENGTH_DISTRIBUTION};
use nft_workload::{NFT_CONTRACT_DISTRIBUTION, NFT_USER_DISTRIBUTION};
use p2p_workload::{RECEIVER_DISTRIBUTION, SENDER_DISTRIBUTION};
use rand::{seq::SliceRandom, Rng};
use rand_distr::{Distribution, WeightedIndex, Zipf};
use uniswap_workload::{AVERAGE_VALUE_DISTRIBUTION, BURSTY_VALUE_DISTRIBUTION};

mod ethereum_block_workload;
mod mixed_workload;
mod nft_workload;
mod p2p_workload;
mod uniswap_workload;

pub use ethereum_block_workload::{DynamicEthereumWorkload, load_ethereum_block_data, ethereum_block_workload_by_block};

/// Returns the percentile of a sorted vector.
#[allow(dead_code)]
fn percentile(sorted_vec: &[f64], percentile: f64) -> Option<f64> {
    let len = sorted_vec.len();
    if len == 0 {
        return None;
    }

    let index = ((percentile / 100.0) * len as f64).ceil() as usize - 1;
    Some(sorted_vec[index.min(len - 1)])
}

/// Solana workload.
/// Generate a transaction with a random number of inputs and gas utilization.
pub fn solana_concurrency<R: Rng>(rng: &mut R) -> (Vec<usize>, usize) {
    let number_of_inputs = WRITE_LENGTH_DISTRIBUTION
        .choose(rng)
        .expect("Empty distribution")
        .round() as usize;

    let dist = WeightedIndex::new(&MIXED_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero");
    let inputs = (0..number_of_inputs)
        .map(|_| dist.sample(rng))
        .collect::<Vec<_>>();

    let execution_time = WeightedIndex::new(&GAS_COST_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);

    // let median_number_of_inputs =
    //     percentile(&WRITE_LENGTH_DISTRIBUTION, 50.0).expect("Empty distribution");
    // let p70_number_of_inputs =
    //     percentile(&WRITE_LENGTH_DISTRIBUTION, 70.0).expect("Empty distribution");

    tracing::debug!(
        "Sampled a Solana transaction with complexity {execution_time} (units of gas) and input objects: {inputs:?}"
    );

    (inputs, execution_time)
}

/// Ethereum transfers workload.
/// Generate a transfer transaction.
pub fn ethereum_transfers<R: Rng>(rng: &mut R) -> (usize, usize) {
    // Generate a transfer transaction.
    let sender = WeightedIndex::new(&SENDER_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);
    let recipient = WeightedIndex::new(&RECEIVER_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);

    tracing::debug!("Sampled Ethereum transfer: from {sender} to {recipient}\n");

    (sender, recipient)
}

/// Ethereum NFT mint.
/// Generate a typical NFT mint transaction.
pub fn ethereum_nft_mint<R: Rng>(rng: &mut R) -> (usize, usize) {
    // Generate a typical NFT mint transaction.
    let object_id = WeightedIndex::new(&NFT_CONTRACT_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);
    let minter = WeightedIndex::new(&NFT_USER_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);

    tracing::debug!("Sampled Ethereum NFT mint: user {minter} minted NFT {object_id}\n");

    (object_id, minter)
}

/// Ethereum Uniswap workload.
/// Generate a Uniswap transaction during normal operations.
pub fn ethereum_uniswap_normal<R: Rng>(rng: &mut R) -> usize {
    let coin_pair = WeightedIndex::new(AVERAGE_VALUE_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);

    tracing::debug!("Uniswap transaction (normal operations) swapped coin pair {coin_pair}\n");

    coin_pair
}

/// Ethereum Uniswap workload.
/// Generate a Uniswap transaction during peak time.
pub fn ethereum_uniswap_peak<R: Rng>(rng: &mut R) -> usize {
    let coin_pair = WeightedIndex::new(BURSTY_VALUE_DISTRIBUTION)
        .expect("Weights should be non-negative and not all zero")
        .sample(rng);

    tracing::debug!("Uniswap transaction (peak times) swapped coin pair {coin_pair}\n");

    coin_pair
}

/// Generate a zipfian tunable workload.
pub fn zipfian_workload<R: Rng>(rng: &mut R, theta: f64, number_of_inputs: usize) -> Vec<usize> {
    const MAX_INPUTS: u64 = 10_000_000;
    let zipf = Zipf::new(MAX_INPUTS, theta).expect("Invalid zipf parameters");
    (0..number_of_inputs)
        .map(|_| zipf.sample(rng).round() as usize)
        .collect()
}

/// Ethereum block workload.
/// Generate a transaction based on real Ethereum block hotspot data.
pub fn ethereum_block_workload<R: Rng>(rng: &mut R) -> Vec<usize> {
    ethereum_block_workload::ethereum_block_workload(rng)
}
