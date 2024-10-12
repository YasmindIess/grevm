use std::collections::HashMap;

use crate::common::storage::InMemoryDB;
use crate::common::{mock_block_accounts, START_ADDRESS};
use crate::erc20::erc20_contract::ERC20Token;
use reth_revm::db::DbAccount;
use reth_revm::{
    db::PlainAccount,
    primitives::{uint, Address, TransactTo, TxEnv, U256},
};
use revm_primitives::alloy_primitives::U160;
use revm_primitives::{AccountInfo, Bytecode, B256};

pub mod erc20_contract;

pub const GAS_LIMIT: u64 = 50_000;

/// Mapping from address to [EvmAccount]
pub type ChainState = HashMap<Address, DbAccount>;

/// Mapping from code hashes to [EvmCode]s
pub type Bytecodes = HashMap<B256, Bytecode>;

/// Mapping from block numbers to block hashes
pub type BlockHashes = HashMap<u64, B256>;

// TODO: Better randomness control. Sometimes we want duplicates to test
// dependent transactions, sometimes we want to guarantee non-duplicates
// for independent benchmarks.
fn generate_addresses(length: usize) -> Vec<Address> {
    (0..length).map(|_| Address::new(rand::random())).collect()
}

#[derive(Clone, Debug, Copy)]
pub enum TransactionCallDataType {
    Empty,
    Mix,
    Allowance,
    Approve,
    BalanceOf,
    Decimals,
    DecreaseAllowance,
    IncreaseAllowance,
    Name,
    Symbol,
    TotalSupply,
    Transfer,
    TransferFrom,
}

#[derive(Clone, Debug, Copy)]
pub enum TransactionModeType {
    Random,
    SameCaller,
    Empty,
}

#[derive(Clone, Debug)]
pub struct TxnBatchConfig {
    num_eoa: usize,
    num_sca: usize,
    num_txns_per_address: usize,
    transaction_call_data_type: TransactionCallDataType,
    transaction_mode_type: TransactionModeType,
}

impl TxnBatchConfig {
    pub fn new(
        num_eoa: usize,
        num_sca: usize,
        num_txns_per_address: usize,
        transaction_call_data_type: TransactionCallDataType,
        transaction_mode_type: TransactionModeType,
    ) -> Self {
        TxnBatchConfig {
            num_eoa,
            num_sca,
            num_txns_per_address,
            transaction_call_data_type,
            transaction_mode_type,
        }
    }
}

pub fn generate_erc20_batch(
    eoa_addresses: Vec<Address>,
    sca_addresses: Vec<Address>,
    num_transfers_per_eoa: usize,
    transaction_call_data_type: TransactionCallDataType,
    transaction_mode_type: TransactionModeType,
) -> Vec<TxEnv> {
    let mut txns = Vec::new();

    for nonce in 0..num_transfers_per_eoa {
        for caller_index in 0..eoa_addresses.len() {
            let sender: Address = eoa_addresses[caller_index];

            let recipient: Address = match transaction_mode_type {
                TransactionModeType::SameCaller => sender,
                TransactionModeType::Random => {
                    eoa_addresses[rand::random::<usize>() % eoa_addresses.len()]
                }
                TransactionModeType::Empty => Address::new([0; 20]),
            };
            let to_address: Address = sca_addresses[rand::random::<usize>() % sca_addresses.len()];

            let mut tx_env = TxEnv {
                caller: sender,
                gas_limit: GAS_LIMIT,
                gas_price: U256::from(0xb2d05e07u64),
                transact_to: TransactTo::Call(to_address),
                value: U256::from(0),
                nonce: Some(nonce as u64),
                ..TxEnv::default()
            };

            match transaction_call_data_type {
                TransactionCallDataType::Transfer => {
                    tx_env.data = ERC20Token::transfer(recipient, U256::from(rand::random::<u8>()));
                }
                TransactionCallDataType::TransferFrom => {
                    tx_env.data = ERC20Token::transfer_from(
                        sender,
                        recipient,
                        U256::from(rand::random::<u8>()),
                    );
                }
                _ => {}
            }
            txns.push(tx_env);
        }
    }
    txns
}

pub fn generate_cluster(
    txn_batch_config: &TxnBatchConfig,
) -> (HashMap<Address, PlainAccount>, HashMap<B256, Bytecode>, Vec<TxEnv>) {
    let mut state = HashMap::new();
    let eoa_addresses: Vec<Address> = generate_addresses(txn_batch_config.num_eoa);

    for person in eoa_addresses.iter() {
        state.insert(
            *person,
            PlainAccount {
                info: AccountInfo {
                    balance: uint!(1_000_000_000_000_000_000_U256),
                    ..AccountInfo::default()
                },
                ..PlainAccount::default()
            },
        );
    }

    let mut bytecodes = HashMap::new();
    let mut erc20_sca: Vec<PlainAccount> = Vec::new();
    let mut erc20_sca_address: Vec<Address> = Vec::new();
    for i in 0..txn_batch_config.num_eoa {
        let gld_address = Address::new(rand::random());
        erc20_sca_address.push(gld_address);

        let gld_account =
            ERC20Token::new("Gold Token", "GLD", 18, 222_222_000_000_000_000_000_000u128)
                .add_balances(&eoa_addresses, uint!(1_000_000_000_000_000_000_U256))
                .build();
        erc20_sca.push(gld_account.clone());
        bytecodes.insert(gld_account.info.code_hash, gld_account.info.code.clone().unwrap());
        state.insert(gld_address, gld_account);
    }

    let mut txs = generate_erc20_batch(
        eoa_addresses,
        erc20_sca_address,
        txn_batch_config.num_txns_per_address,
        txn_batch_config.transaction_call_data_type,
        txn_batch_config.transaction_mode_type,
    );

    (state, bytecodes, txs)
}

pub fn generate_independent_data(block_size: usize) -> (InMemoryDB, Vec<TxEnv>) {
    let db_latency_us = std::env::var_os("DB_LATENCY_US")
        .map(|s| s.to_string_lossy().parse().unwrap())
        .unwrap_or(0);

    let pevm_gas_limit: u64 = 26_938;
    let mut accounts = mock_block_accounts(START_ADDRESS + 1, block_size);
    let eoa_accounts: Vec<Address> = accounts.keys().cloned().collect();
    let mut bytecodes = HashMap::new();
    // START_ADDRESS as contract address
    let contract_address = Address::from(U160::from(START_ADDRESS));
    let galxe_account =
        ERC20Token::new("Galxe Token", "G", 18, 222_222_000_000_000_000_000_000u128)
            .add_balances(&eoa_accounts, uint!(1_000_000_000_000_000_000_U256))
            .build();
    bytecodes.insert(galxe_account.info.code_hash, galxe_account.info.code.clone().unwrap());
    accounts.insert(contract_address, galxe_account);
    let mut db = InMemoryDB::new(accounts, bytecodes, Default::default());
    db.latency_us = db_latency_us;

    let txs: Vec<TxEnv> = (1..=block_size)
        .map(|i| {
            let address = Address::from(U160::from(START_ADDRESS + i));
            let call_data = ERC20Token::transfer(address, U256::from(900));
            TxEnv {
                caller: address,
                transact_to: TransactTo::Call(contract_address),
                value: U256::from(0),
                gas_limit: GAS_LIMIT,
                gas_price: U256::from(1),
                nonce: Some(1),
                data: call_data,
                ..TxEnv::default()
            }
        })
        .collect();
    (db, txs)
}
