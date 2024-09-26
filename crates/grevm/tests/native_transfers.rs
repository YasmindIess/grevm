pub mod common;

use common::storage::InMemoryDB;
use reth_revm::db::PlainAccount;
use revm_primitives::{alloy_primitives::U160, Address, TransactTo, TxEnv, U256};
use std::collections::HashMap;

#[test]
fn native_transfers_independent() {
    let block_size = 10_000; // number of transactions
    let accounts: HashMap<Address, PlainAccount> =
        (0..=block_size).map(common::mock_eoa_account).collect();
    let db = InMemoryDB::new(accounts, Default::default(), Default::default());
    // `Address::ZERO` is the minner address in compare_evm_execute
    // Skipping `Address::ZERO` as the beneficiary account.
    let txs: Vec<TxEnv> = (1..=block_size)
        .map(|i| {
            let address = Address::from(U160::from(i));
            TxEnv {
                caller: address,
                transact_to: TransactTo::Call(address),
                value: U256::from(1),
                gas_limit: common::TRANSFER_GAS_LIMIT,
                gas_price: U256::from(1),
                nonce: Some(1),
                ..TxEnv::default()
            }
        })
        .collect();
    common::compare_evm_execute(db, txs, true);
}

#[test]
fn native_with_same_sender() {
    let block_size = 100;
    let accounts: HashMap<Address, PlainAccount> =
        (0..=block_size + 1).map(common::mock_eoa_account).collect();
    let db = InMemoryDB::new(accounts, Default::default(), Default::default());

    let sender_address = Address::from(U160::from(1));
    let receiver_address = Address::from(U160::from(block_size + 1));
    let mut sender_nonce = 0;
    // `Address::ZERO` is the minner address in compare_evm_execute
    // Skipping `Address::ZERO` as the beneficiary account.
    let txs: Vec<TxEnv> = (1..=block_size)
        .map(|i| {
            let (address, to, nonce) = if i % 4 != 1 {
                (Address::from(U160::from(i)), Address::from(U160::from(i)), 1)
            } else {
                sender_nonce += 1;
                (sender_address, receiver_address, sender_nonce)
            };

            TxEnv {
                caller: address,
                transact_to: TransactTo::Call(to),
                value: U256::from(i),
                gas_limit: common::TRANSFER_GAS_LIMIT,
                gas_price: U256::from(1),
                // If setting nonce, then nonce validation against the account's nonce,
                // the parallel execution will fail for the nonce validation.
                // However, the failed evm.transact() doesn't generate write set,
                // then there's no dependency can be detected even two txs are related.
                // TODO(gaoxin): lazily update nonce
                nonce: None,
                ..TxEnv::default()
            }
        })
        .collect();
    common::compare_evm_execute(db, txs, false);
}

#[test]
fn native_with_all_related() {
    let block_size = 100;
    let accounts: HashMap<Address, PlainAccount> =
        (0..=block_size + 1).map(common::mock_eoa_account).collect();
    let db = InMemoryDB::new(accounts, Default::default(), Default::default());
    let txs: Vec<TxEnv> = (1..=block_size)
        .map(|i| {
            // tx(i) => tx(i+1), all transactions should execute sequentially.
            let from = Address::from(U160::from(i));
            let to = Address::from(U160::from(i + 1));

            TxEnv {
                caller: from,
                transact_to: TransactTo::Call(to),
                value: U256::from(1000),
                gas_limit: common::TRANSFER_GAS_LIMIT,
                gas_price: U256::from(1),
                nonce: None,
                ..TxEnv::default()
            }
        })
        .collect();
    common::compare_evm_execute(db, txs, false);
}
