use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::memory_manager::MemoryManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use lazy_static::lazy_static;
use log::LevelFilter;
use simplelog::{ColorChoice, ConfigBuilder, TermLogger, TerminalMode, ThreadLogMode};
use tokio::runtime::Runtime;

mod exec;
mod metrics;

#[cfg(feature = "mm")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "sn")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

pub struct BlazeIter {
    pub stream: SendableRecordBatchStream,
    pub execution_plan: Arc<dyn ExecutionPlan>,
    pub runtime: Arc<Runtime>,
}

lazy_static! {
    pub static ref LOGGING_INIT: Mutex<Option<()>> = Mutex::default();
    pub static ref SESSIONCTX: Mutex<Option<Arc<SessionContext>>> = Mutex::default();
}

pub fn init_session_ctx(
    max_memory: usize,
    memory_fraction: f64,
    batch_size: usize,
    tmp_dirs: String,
) -> Arc<SessionContext> {
    let mut session_ctx = SESSIONCTX.lock().unwrap();
    if session_ctx.is_none() {
        let dirs = tmp_dirs.split(',').map(PathBuf::from).collect::<Vec<_>>();
        let runtime_config = RuntimeConfig::new()
            .with_memory_manager(MemoryManagerConfig::New {
                max_memory,
                memory_fraction,
            })
            .with_disk_manager(DiskManagerConfig::NewSpecified(dirs));
        let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
        let config = SessionConfig::new().with_batch_size(batch_size);
        *session_ctx = Some(Arc::new(SessionContext::with_config_rt(config, runtime)));
    }
    return session_ctx.as_ref().unwrap().clone();
}

pub fn init_logging() {
    let mut logging_init = LOGGING_INIT.lock().unwrap();
    if logging_init.is_none() {
        TermLogger::init(
            LevelFilter::Info,
            ConfigBuilder::new()
                .set_thread_mode(ThreadLogMode::Both)
                .build(),
            TerminalMode::Stderr,
            ColorChoice::Never,
        )
        .unwrap();
        *logging_init = Some(());
    }
}
