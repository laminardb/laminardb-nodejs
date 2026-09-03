//! Engine telemetry mapped onto plain JS objects (plan 03 Task 2.4).
//!
//! Field-for-field projections of the core's metrics structs; the core types
//! stay internal, the objects are the contract.

use laminar_db::{LaminarDB, PipelineMetrics, SourceMetrics, StreamMetrics};
use napi::Result;
use napi_derive::napi;

use crate::error::map_db_error;

/// Aggregate pipeline counters.
#[napi(object)]
pub struct PipelineMetricsObject {
    pub total_events_ingested: i64,
    pub total_events_emitted: i64,
    pub total_events_dropped: i64,
    pub total_cycles: i64,
    pub total_batches: i64,
    pub uptime_ms: f64,
    /// Engine lifecycle state name (e.g. `Running`).
    pub state: String,
    pub source_count: i64,
    pub stream_count: i64,
    pub sink_count: i64,
    /// Minimum watermark across all sources (epoch milliseconds).
    pub pipeline_watermark: i64,
    pub mv_updates: i64,
    pub mv_bytes_stored: i64,
}

/// Counters for one registered source.
#[napi(object)]
pub struct SourceMetricsObject {
    pub name: String,
    pub total_events: i64,
    pub pending: i64,
    pub capacity: i64,
    pub is_backpressured: bool,
    /// Event-time watermark (epoch milliseconds).
    pub watermark: i64,
    /// Buffer utilization, 0.0..1.0.
    pub utilization: f64,
}

/// Counters for one stream.
#[napi(object)]
pub struct StreamMetricsObject {
    pub name: String,
    pub total_events: i64,
    /// Defining SQL, when known.
    pub sql: Option<String>,
}

fn saturating(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn saturating_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn pipeline_metrics(metrics: PipelineMetrics) -> PipelineMetricsObject {
    PipelineMetricsObject {
        total_events_ingested: saturating(metrics.total_events_ingested),
        total_events_emitted: saturating(metrics.total_events_emitted),
        total_events_dropped: saturating(metrics.total_events_dropped),
        total_cycles: saturating(metrics.total_cycles),
        total_batches: saturating(metrics.total_batches),
        uptime_ms: metrics.uptime.as_secs_f64() * 1000.0,
        state: metrics.state.to_string(),
        source_count: saturating_usize(metrics.source_count),
        stream_count: saturating_usize(metrics.stream_count),
        sink_count: saturating_usize(metrics.sink_count),
        pipeline_watermark: metrics.pipeline_watermark,
        mv_updates: saturating(metrics.mv_updates),
        mv_bytes_stored: saturating(metrics.mv_bytes_stored),
    }
}

pub(crate) fn source_metrics(metrics: SourceMetrics) -> SourceMetricsObject {
    SourceMetricsObject {
        name: metrics.name,
        total_events: saturating(metrics.total_events),
        pending: saturating_usize(metrics.pending),
        capacity: saturating_usize(metrics.capacity),
        is_backpressured: metrics.is_backpressured,
        watermark: metrics.watermark,
        utilization: metrics.utilization,
    }
}

pub(crate) fn stream_metrics(metrics: StreamMetrics) -> StreamMetricsObject {
    StreamMetricsObject {
        name: metrics.name,
        total_events: saturating(metrics.total_events),
        sql: metrics.sql,
    }
}

/// Telemetry read from the engine. All methods take registry locks; they are
/// async so a long DDL cannot stall the JS thread through them.
pub struct Telemetry;

impl Telemetry {
    pub async fn metrics(db: &LaminarDB) -> Result<PipelineMetricsObject> {
        Ok(pipeline_metrics(db.metrics()))
    }

    pub async fn source_metrics(db: &LaminarDB, name: &str) -> Result<SourceMetricsObject> {
        let metrics = db.source_metrics(name).ok_or_else(|| {
            crate::error::coded_error(200, &format!("source not found: '{name}'"))
        })?;
        Ok(source_metrics(metrics))
    }

    pub async fn all_source_metrics(db: &LaminarDB) -> Result<Vec<SourceMetricsObject>> {
        Ok(db
            .all_source_metrics()
            .into_iter()
            .map(source_metrics)
            .collect())
    }

    pub async fn stream_metrics(db: &LaminarDB, name: &str) -> Result<StreamMetricsObject> {
        let metrics = db.stream_metrics(name).ok_or_else(|| {
            crate::error::coded_error(200, &format!("stream not found: '{name}'"))
        })?;
        Ok(stream_metrics(metrics))
    }

    pub async fn all_stream_metrics(db: &LaminarDB) -> Result<Vec<StreamMetricsObject>> {
        Ok(db
            .all_stream_metrics()
            .into_iter()
            .map(stream_metrics)
            .collect())
    }

    pub async fn pipeline_state(db: &LaminarDB) -> Result<String> {
        Ok(db.pipeline_state().to_owned())
    }

    pub async fn pipeline_watermark(db: &LaminarDB) -> Result<i64> {
        Ok(db.pipeline_watermark())
    }

    pub async fn total_events_processed(db: &LaminarDB) -> Result<i64> {
        Ok(saturating(db.total_events_processed()))
    }

    pub async fn cancel_query(db: &LaminarDB, query_id: u64) -> Result<()> {
        db.cancel_query(query_id).map_err(map_db_error)
    }
}
