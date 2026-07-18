//! Structured logging: one event per line on stderr, JSON or human text.
//! Zero-dependency (serde_json only, which we already carry).
//!
//! JSON mode emits `{"ts":"2026-07-18T09:00:00.123Z","level":"info",
//! "event":"request","method":"POST",...}` — one object per line, ready for
//! Loki/Cloud Logging/jq. Text mode emits the same fields as `key=value`
//! pairs for humans.

use std::io::Write as _;
use std::sync::atomic::{AtomicU8, Ordering};

#[derive(Clone, Copy, PartialEq)]
pub enum Format {
    Json,
    Text,
}

static FORMAT: AtomicU8 = AtomicU8::new(1); // default Text

pub fn set_format(f: Format) {
    FORMAT.store(if f == Format::Json { 0 } else { 1 }, Ordering::Relaxed);
}
fn format() -> Format {
    if FORMAT.load(Ordering::Relaxed) == 0 { Format::Json } else { Format::Text }
}

/// Unix millis -> RFC3339 UTC ("2026-07-18T09:00:00.123Z"). Civil-date
/// conversion per Hinnant's algorithm; no chrono dependency.
pub fn iso8601(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (h, m, s) = (sod / 3600, (sod % 3600) / 60, sod % 60);
    // days since 1970-01-01 -> civil y/m/d
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{millis:03}Z")
}

/// Emit one structured event. `fields` must be a JSON object (use
/// `serde_json::json!({...})`); non-objects are wrapped under `"msg"`.
pub fn event(level: &str, event: &str, fields: serde_json::Value) {
    let ts = iso8601(crate::engine::now_ms());
    let line = match format() {
        Format::Json => {
            let mut obj = serde_json::Map::new();
            obj.insert("ts".into(), serde_json::Value::String(ts));
            obj.insert("level".into(), serde_json::Value::String(level.into()));
            obj.insert("event".into(), serde_json::Value::String(event.into()));
            match fields {
                serde_json::Value::Object(m) => obj.extend(m),
                serde_json::Value::Null => {}
                other => {
                    obj.insert("msg".into(), other);
                }
            }
            serde_json::Value::Object(obj).to_string()
        }
        Format::Text => {
            let mut line = format!("{ts} {} {event}", level.to_uppercase());
            if let serde_json::Value::Object(m) = &fields {
                for (k, v) in m {
                    match v {
                        serde_json::Value::String(s) => line.push_str(&format!(" {k}={s}")),
                        other => line.push_str(&format!(" {k}={other}")),
                    }
                }
            } else if !fields.is_null() {
                line.push_str(&format!(" msg={fields}"));
            }
            line
        }
    };
    let mut err = std::io::stderr().lock();
    let _ = writeln!(err, "{line}");
}

pub fn info(ev: &str, fields: serde_json::Value) {
    event("info", ev, fields)
}
pub fn warn(ev: &str, fields: serde_json::Value) {
    event("warn", ev, fields)
}
pub fn error(ev: &str, fields: serde_json::Value) {
    event("error", ev, fields)
}

#[cfg(test)]
mod tests {
    use super::iso8601;

    #[test]
    fn iso8601_known_answers() {
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
        // 2000-03-01 leap-adjacent boundary
        assert_eq!(iso8601(951_868_800_000), "2000-03-01T00:00:00.000Z");
        // arbitrary modern timestamp with millis
        assert_eq!(iso8601(1_784_540_130_456), "2026-07-20T09:35:30.456Z");
    }
}
