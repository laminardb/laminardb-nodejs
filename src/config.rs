//! Open configuration: JS options → validated `LaminarConfig`.
//!
//! Every field maps 1:1 onto a real core config field (the structural
//! equivalent of the Java binding's native config handle — the mapping cannot
//! invent fields the core does not have). Unknown JS keys are ignored by napi
//! object conversion; the TypeScript layer owns the strict typed interface.

use std::collections::HashMap;
use std::path::PathBuf;

use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarConfig;
use napi_derive::napi;

const MEMORY: &str = ":memory:";

/// Checkpointing options; an empty object enables manual checkpoints only.
#[napi(object)]
pub struct CheckpointConfig {
    /// Interval in milliseconds; absent = manual `checkpoint()` calls only.
    pub interval_ms: Option<u32>,
    /// One end-to-end attempt deadline in milliseconds; absent = core default
    /// (120 s).
    pub timeout_ms: Option<u32>,
    /// Checkpoint directory; absent = the database storage directory, then
    /// `./data` (the core never silently selects volatile storage).
    pub data_dir: Option<String>,
    /// Maximum bytes for one participant's checkpoint node-data object;
    /// absent = core default.
    pub max_node_data_bytes: Option<f64>,
}

/// Connection options for `open`. `:memory:` (or no storage directory)
/// opens in-memory.
#[napi(object)]
pub struct OpenConfig {
    /// Local durability directory. Wins over the positional path argument.
    pub storage_dir: Option<String>,
    pub checkpoint: Option<CheckpointConfig>,
    /// Default source buffer size in rows.
    pub buffer_size: Option<u32>,
    /// Emit windowed aggregates incrementally before window close.
    pub incremental_emit: Option<bool>,
    /// Object-store URL for cloud checkpoints (e.g. `s3://bucket/prefix`).
    pub object_store_url: Option<String>,
    /// Object-store options (credentials, region, endpoint).
    pub object_store_options: Option<HashMap<String, String>>,
}

impl OpenConfig {
    /// Build the core config. `positional_path` is the sugar argument from
    /// `open(path, config)`; `config.storageDir` wins when both are given.
    pub fn into_core(self, positional_path: Option<String>) -> napi::Result<LaminarConfig> {
        let mut config = LaminarConfig::default();
        let storage = self.storage_dir.or(positional_path);
        match storage.as_deref() {
            None | Some(MEMORY) => {}
            Some(dir) => config.storage_dir = Some(PathBuf::from(dir)),
        }
        if let Some(checkpoint) = self.checkpoint {
            config.checkpoint = Some(StreamCheckpointConfig {
                interval_ms: checkpoint.interval_ms.map(u64::from),
                timeout_ms: checkpoint.timeout_ms.map(u64::from),
                data_dir: checkpoint.data_dir.map(PathBuf::from),
                max_node_data_bytes: checkpoint
                    .max_node_data_bytes
                    .map(|bytes| bytes.max(0.0) as u64),
            });
        }
        if let Some(buffer_size) = self.buffer_size {
            config.default_buffer_size = buffer_size as usize;
        }
        if let Some(incremental_emit) = self.incremental_emit {
            config.incremental_emit = incremental_emit;
        }
        if let Some(url) = self.object_store_url {
            config.object_store_url = Some(url);
        }
        if let Some(options) = self.object_store_options {
            config.object_store_options = options;
        }
        Ok(config)
    }
}
