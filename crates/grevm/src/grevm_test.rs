use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::{Arc, RwLock};

use auto_impl::auto_impl;
use lazy_static::lazy_static;
use rand::Rng;
use tokio::runtime::{Builder, Runtime};
use tokio::task;

lazy_static! {
    static ref TK_TEST_RUNTIME: Runtime = Builder::new_multi_thread()
        .worker_threads(4)
        .thread_name("tokio-test-runtime")
        .enable_all()
        .build()
        .unwrap();
}

#[auto_impl(& mut, Box)]
trait Database {
    type Error;

    fn get(&mut self, key: i32) -> Result<i32, Self::Error>;
}

#[auto_impl(&, & mut, Box, Rc, Arc)]
trait DatabaseRef {
    type Error;

    fn get_ref(&self, key: i32) -> Result<i32, Self::Error>;
}

#[derive(Debug)]
struct DBError {
    msg: String,
}

impl DBError {
    pub fn new(msg: String) -> Self {
        DBError { msg }
    }
}

impl Display for DBError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error: {}", self.msg)
    }
}

impl std::error::Error for DBError {}

#[derive(Debug)]
struct MockDB {
    cache: HashMap<i32, i32>,
}

impl Database for MockDB {
    type Error = DBError;

    fn get(&mut self, key: i32) -> Result<i32, Self::Error> {
        // Simulate database latency
        std::thread::sleep(std::time::Duration::from_millis(200));
        self.cache.get(&key).copied().ok_or(DBError::new(String::from("not found")))
    }
}

impl DatabaseRef for MockDB {
    type Error = <Self as Database>::Error;

    fn get_ref(&self, key: i32) -> Result<i32, Self::Error> {
        // Simulate database latency
        std::thread::sleep(std::time::Duration::from_millis(200));
        self.cache.get(&key).copied().ok_or(DBError::new(String::from("not found")))
    }
}

struct CacheDB<DB> {
    pub database: Arc<DB>,
    pub cache: HashMap<i32, i32>,
    pub should_yield: bool,
}

impl<DB> CacheDB<DB> {
    fn new_yield(database: DB) -> Self {
        Self {
            database: Arc::new(database),
            cache: Default::default(),
            should_yield: true,
        }
    }

    fn new(database: DB) -> Self {
        Self {
            database: Arc::new(database),
            cache: Default::default(),
            should_yield: false,
        }
    }
}

impl<DB> Database for CacheDB<DB>
where
    DB: DatabaseRef + Send + Sync,
    DB::Error: Send + Sync,
{
    type Error = DB::Error;

    fn get(&mut self, key: i32) -> Result<i32, Self::Error> {
        if let Some(&value) = self.cache.get(&key) {
            return Ok(value);
        }
        let value = self.database.get_ref(key)?;

        self.cache.insert(key, value);
        Ok(value)
    }
}

impl<DB> DatabaseRef for CacheDB<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    type Error = <Self as Database>::Error;

    fn get_ref(&self, key: i32) -> Result<i32, Self::Error> {
        if let Some(&value) = self.cache.get(&key) {
            return Ok(value);
        }
        if self.should_yield {
            let db = self.database.clone();
            // Block the execution of asynchronous operations on the current thread,
            // and yield the IO operation
            task::block_in_place(move || {
                TK_TEST_RUNTIME.block_on(async move {
                    task::spawn_blocking(move || {
                        db.get_ref(key)
                    }).await.unwrap()
                })
            })
        } else {
            self.database.get_ref(key)
        }
    }
}

struct PartitionExecutor<DB> {
    partition_id: usize,
    db: CacheDB<DB>,
}

impl<DB> PartitionExecutor<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(partition_id: usize, db: DB) -> Self {
        Self {
            partition_id,
            db: CacheDB::new(db),
        }
    }

    pub fn execute(&mut self) {
        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let key = rng.gen_range(0..100);
            let r = self.db.get(key);
            // Simulate CPU intensive computing operations, which also takes approximately 200ms
            let sum: u64 = (0..30_000_000).sum();
            print!("Partition {} sum {}", self.partition_id, sum);
            if r.is_ok() {
                println!("Partition {} get {}", self.partition_id, key);
            }
        }
        println!("Update {} keys in db", self.db.cache.len());
    }
}

struct Scheduler<DB> {
    state: Arc<CacheDB<DB>>,
    executors: Vec<Arc<RwLock<PartitionExecutor<Arc<CacheDB<DB>>>>>>,
}

impl<DB> Scheduler<DB>
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    pub fn new(db: DB) -> Self {
        Self {
            state: Arc::new(CacheDB::new(db)),
            executors: Default::default(),
        }
    }

    pub fn new_yield(db: DB) -> Self {
        Self {
            state: Arc::new(CacheDB::new_yield(db)),
            executors: Default::default(),
        }
    }

    async fn executor_execute(executor: Arc<RwLock<PartitionExecutor<Arc<CacheDB<DB>>>>>) {
        executor.write().unwrap().execute();
    }

    pub fn parallel_execute(&mut self) {
        for partition_id in 0..10 {
            self.executors.push(Arc::new(RwLock::new(PartitionExecutor::new(partition_id, self.state.clone()))));
        }

        TK_TEST_RUNTIME.block_on(async {
            let mut tasks = vec![];
            for executor in &self.executors {
                tasks.push(TK_TEST_RUNTIME.spawn(Self::executor_execute(executor.clone())));
            }
            futures::future::join_all(tasks).await;
        });
        println!("complete all tasks");
    }
}


fn execute(should_yield: bool) {
    let mut data: HashMap<i32, i32> = HashMap::new();
    for i in 0..100 {
        data.insert(i, i * i);
    }
    let mock_db = MockDB { cache: data };
    execute_impl(mock_db, should_yield);
}

fn execute_impl<DB>(db: DB, should_yield: bool)
where
    DB: DatabaseRef + Send + Sync + 'static,
    DB::Error: Send + Sync,
{
    let mut scheduler = if should_yield {
        Scheduler::new_yield(db)
    } else {
        Scheduler::new(db)
    };
    scheduler.parallel_execute();
}

#[cfg(test)]
mod tests {
    use super::execute;

    #[test]
    fn test_parallel_without_yield() {
        execute(false);
    }

    #[test]
    fn test_parallel_with_yield() {
        execute(true);
    }
}
