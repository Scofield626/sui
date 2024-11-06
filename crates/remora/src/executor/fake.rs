
use std::{collections::BTreeMap, time::{Duration, Instant}};
use rand::thread_rng;
use rand_distr::{Distribution, Normal};

use sui_types::base_types::VersionNumber;
use super::api::{ExecutableTransaction, ExecutionResults, Executor, StateStore, Transaction};
use sui_single_node_benchmark::benchmark_context::BenchmarkContext;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SequenceNumber}, committee::EpochId, digests::TransactionDigest, effects::TransactionEffects, error::SuiResult, object::{Object, Owner}, storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, PackageObject, ParentSync}, transaction::InputObjectKind
};


#[derive(Clone)]
pub struct FakeTransaction {
    delay: u64, // delay in ms for that this transaction simulates
}
pub type FakeTransactionType = Transaction<FakeExecutor>;

pub type FakeExecutionResults = ExecutionResults<FakeExecutor>;
pub struct FakeStore;

impl FakeExecutionResults {
    // todo: is there a way to make this a constant instead of a fn?
    pub fn dummy() -> Self {
        FakeExecutionResults {
            updates: TransactionEffects::default(),
            new_state: BTreeMap::new(),
        }
    }
}

impl ExecutableTransaction for FakeTransaction {
    fn digest(&self) -> &TransactionDigest {
        // Return a reference to a static TransactionDigest
        &TransactionDigest::ZERO
    }

    fn input_objects(&self) -> Vec<InputObjectKind> {
        // Return an empty vector since the method won't be used
        Vec::new()
    }
}

impl ObjectStore for FakeStore {
    fn get_object(
        &self,
        _object_id: &ObjectID,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        unreachable!()
    }

    fn get_object_by_key(
        &self,
        _object_id: &ObjectID,
        _version: VersionNumber,
    ) -> Result<Option<Object>, sui_types::storage::error::Error> {
        unreachable!()
    }
}

impl ParentSync for FakeStore {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        _object_id: ObjectID,
    ) -> SuiResult<Option<ObjectRef>> {
        unreachable!()
    }
}

impl ChildObjectResolver for FakeStore {
    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: SequenceNumber,
    ) -> SuiResult<Option<Object>> {
        Ok(self.get_object(child).unwrap().and_then(|o| {
            if o.version() <= child_version_upper_bound
                && o.owner == Owner::ObjectOwner((*parent).into())
            {
                Some(o.clone())
            } else {
                None
            }
        }))
    }

    fn get_object_received_at_version(
        &self,
        _owner: &ObjectID,
        _receiving_object_id: &ObjectID,
        _receive_object_at_version: SequenceNumber,
        _epoch_id: EpochId,
    ) -> SuiResult<Option<Object>> {
        unimplemented!()
    }
}

impl BackingPackageStore for FakeStore {
    fn get_package_object(&self, _package_id: &ObjectID) -> SuiResult<Option<PackageObject>> {
        unreachable!()
    }
}

impl StateStore<TransactionEffects> for FakeStore {
    fn commit_objects(&self, _updates: TransactionEffects, _new_state: BTreeMap<ObjectID, Object>) {}
}

#[derive(Clone)]
pub struct FakeExecutor;

impl Executor for FakeExecutor {
    type Transaction = FakeTransaction;
    type ExecutionResults = TransactionEffects;
    type Store = FakeStore;

    fn context(&self) -> Option<std::sync::Arc<BenchmarkContext>> {
        None
    }

    async fn execute(
        _ctx: Option<std::sync::Arc<BenchmarkContext>>,
        _store: std::sync::Arc<Self::Store>,
        transaction: &FakeTransactionType,
    ) -> FakeExecutionResults {

        let start = Instant::now();
        let duration = Duration::from_millis(transaction.delay);
    
        // Busy-wait until the specified duration has elapsed.
        while Instant::now() - start < duration {}
        FakeExecutionResults::dummy()
    }
        
    fn pre_execute_check(
        _ctx: Option<std::sync::Arc<BenchmarkContext>>,
        _store: std::sync::Arc<Self::Store>,
        _transaction: &super::api::TransactionWithTimestamp<Self::Transaction>,
    ) -> bool {
        true
    }
    
    fn create_in_memory_store(&self) -> Self::Store {
        FakeStore
    }
    
    async fn load_state_for_shared_objects(&self) {}
}

pub fn generate_fake_transaction(delay: u64) -> FakeTransaction {
    FakeTransaction { delay }
}

pub fn generate_fake_transactions_normal_distribution(mean: u64, std_dev: u64, num_transactions: u64) -> Vec<FakeTransaction> {
    let normal = Normal::new(mean as f64, std_dev as f64).unwrap();
    let mut rng = thread_rng();
    
    let transactions = (0..num_transactions).map(|_| {
        let delay = normal.sample(&mut rng) as u64;
        generate_fake_transaction(delay)
    }).collect();

    transactions
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::executor::{
            api::Executor, fake::{generate_fake_transaction, generate_fake_transactions_normal_distribution, FakeExecutor, FakeTransactionType}};

    #[tokio::test]
    async fn test_fake_executor_one_long_transaction() {
        let executor = FakeExecutor;
        let store = Arc::new(executor.create_in_memory_store());
        let ctx = executor.context();
        let transaction = FakeTransactionType::new_for_tests(generate_fake_transaction(30_000));
        let _result = FakeExecutor::execute(ctx, store, &transaction).await; 
    }

    #[tokio::test]
    async fn test_fake_executor_many_transactions() {
        let executor = FakeExecutor;
        let store = Arc::new(executor.create_in_memory_store());
        let ctx = executor.context();
        let transactions = generate_fake_transactions_normal_distribution(5,1,10);
        for tx in transactions {
            let transaction = FakeTransactionType::new_for_tests(tx);
            let _result = FakeExecutor::execute(ctx.clone(), store.clone(), &transaction).await; 
        }
    }

}