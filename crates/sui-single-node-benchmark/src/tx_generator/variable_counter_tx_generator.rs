// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use sui_test_transaction_builder::TestTransactionBuilder;
use sui_types::{
    base_types::{ObjectID, SequenceNumber, SuiAddress},
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{CallArg, ObjectArg, Transaction, DEFAULT_VALIDATOR_GAS_PRICE},
    Identifier,
};

use super::TxGenerator;
use crate::mock_account::Account;

pub struct VariableCounterTxGenerator {
    move_package: ObjectID,
    counter_objects: Vec<(ObjectID, SequenceNumber)>,
    account_orders: HashMap<SuiAddress, usize>,
    /// Maps tx id to a list of counter indices to increment.
    stats: HashMap<usize, Vec<usize>>,
}

impl VariableCounterTxGenerator {
    pub fn new(
        move_package: ObjectID,
        counter_objects: Vec<(ObjectID, SequenceNumber)>,
        account_orders: HashMap<SuiAddress, usize>,
        stats: HashMap<usize, Vec<usize>>,
    ) -> Self {
        Self {
            move_package,
            counter_objects,
            account_orders,
            stats,
        }
    }
}

impl TxGenerator for VariableCounterTxGenerator {
    fn generate_txs(&self, account: Account) -> Vec<Transaction> {
        let index = self.account_orders.get(&account.sender).unwrap();
        let counters = match self.stats.get(index) {
            Some(counters) => counters,
            None => {
                // No more transactions
                return vec![];
            }
        };

        let pt = {
            let mut builder = ProgrammableTransactionBuilder::new();
            for i in counters {
                builder
                    .move_call(
                        self.move_package,
                        Identifier::new("benchmark").unwrap(),
                        Identifier::new("increment_shared_counter").unwrap(),
                        vec![],
                        vec![CallArg::Object(ObjectArg::SharedObject {
                            id: self.counter_objects[*i].0,
                            initial_shared_version: self.counter_objects[*i].1,
                            mutable: true,
                        })],
                    )
                    .unwrap();
            }
            builder.finish()
        };

        vec![TestTransactionBuilder::new(
            account.sender,
            account.gas_objects[0],
            DEFAULT_VALIDATOR_GAS_PRICE,
        )
        .programmable(pt)
        .build_and_sign(account.keypair.as_ref())]
    }

    fn name(&self) -> &'static str {
        "Variable Counter Increment Transaction Generator"
    }
}
