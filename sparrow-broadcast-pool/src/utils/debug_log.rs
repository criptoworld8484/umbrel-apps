use serde_json::{json, Value};
use std::io::Write;
use std::sync::Mutex;

const DEBUG_LOG_PATH: &str =
    "/home/criptoworld/Documents/OpenCode/Mywalletcompromise/.cursor/debug-774864.log";

static DEBUG_LOG_MUTEX: Mutex<()> = Mutex::new(());

// #region agent log
pub fn agent_log(hypothesis_id: &str, location: &str, message: &str, data: Value) {
    let entry = json!({
        "sessionId": "774864",
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": chrono::Utc::now().timestamp_millis()
    });
    let Ok(_guard) = DEBUG_LOG_MUTEX.lock() else {
        return;
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(DEBUG_LOG_PATH)
    {
        let _ = writeln!(file, "{}", entry);
    }
}
// #endregion
