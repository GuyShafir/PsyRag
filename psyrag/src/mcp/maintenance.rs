use crate::engine::Engine;
use std::path::Path;

const DAY_MS: i64 = 24 * 60 * 60 * 1000;

pub fn read_last_sleep(path: &Path) -> i64 {
    std::fs::read_to_string(path).ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

pub fn write_last_sleep(path: &Path, ts: i64) {
    let _ = std::fs::write(path, ts.to_string());
}

/// If more than a day since the last sleep, run the heavy offline pass now
/// (we hold the flock a cron would otherwise need). Returns whether it slept.
pub fn sleep_if_stale(engine: &mut Engine, last_sleep_path: &Path, now: i64) -> bool {
    let last = read_last_sleep(last_sleep_path);
    if last == 0 {
        // First run: establish the marker; do NOT downscale the freshly
        // cold-started seed. Sleep begins mattering on subsequent days.
        write_last_sleep(last_sleep_path, now);
        return false;
    }
    if now - last <= DAY_MS {
        return false;
    }
    let _ = engine.layer.sleep(engine.pg.graph(), now);
    let _ = engine.save_sidecar();
    write_last_sleep(last_sleep_path, now);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn last_sleep_roundtrips_and_defaults_zero() {
        let f = std::env::temp_dir().join(format!("psyrag-ls-{}", std::process::id()));
        std::fs::remove_file(&f).ok();
        assert_eq!(read_last_sleep(&f), 0);
        write_last_sleep(&f, 1_700_000_000_000);
        assert_eq!(read_last_sleep(&f), 1_700_000_000_000);
        std::fs::remove_file(&f).ok();
    }
}
