use atlas_metrics::{MetricLevel, MetricRegistry};
use atlas_metrics::metrics::MetricKind;

/// State transfer will take the
/// 6XX metric ID range
pub const STATE_TRANSFER_STATE_INSTALL_CLONE_TIME : &str = "STATE_CLONE_TIME";
pub const STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID : usize = 600;

pub const STATE_TRANSFER_TIME : &str = "STATE_TRANSFER_TIME";
pub const STATE_TRANSFER_TIME_ID : usize = 601;

pub const CHECKPOINT_UPDATE_TIME : &str = "CHECKPOINT_UPDATE_TIME";
pub const CHECKPOINT_UPDATE_TIME_ID : usize = 602;

pub const PROCESS_REQ_STATE_TIME : &str = "PROCESS_REQ_STATE_TIME";
pub const PROCESS_REQ_STATE_TIME_ID : usize = 603;

pub const TOTAL_STATE_TRANSFERED : &str = "TOTAL_STATE_TRANSFERED";
pub const TOTAL_STATE_TRANSFERED_ID : usize = 604;

pub const TOTAL_STATE_INSTALLED : &str = "TOTAL_STATE_INSTALLED";
pub const TOTAL_STATE_INSTALLED_ID : usize = 605;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![
        (STATE_TRANSFER_STATE_INSTALL_CLONE_TIME_ID, STATE_TRANSFER_STATE_INSTALL_CLONE_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (STATE_TRANSFER_TIME_ID, STATE_TRANSFER_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (CHECKPOINT_UPDATE_TIME_ID, CHECKPOINT_UPDATE_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (PROCESS_REQ_STATE_TIME_ID, PROCESS_REQ_STATE_TIME.to_string(), MetricKind::Duration, MetricLevel::Info).into(),
        (TOTAL_STATE_TRANSFERED_ID, TOTAL_STATE_TRANSFERED.to_string(), MetricKind::Counter, MetricLevel::Info).into(),
        (TOTAL_STATE_INSTALLED_ID, TOTAL_STATE_INSTALLED.to_string(), MetricKind::Counter, MetricLevel::Info).into(),
    ]
}