//! Zero-dependency Prometheus text-format metrics.
//!
//! Request counters/latency histograms are recorded lock-free into fixed
//! atomic arrays keyed by route class × status class — route classes are a
//! closed set, so cardinality is bounded no matter what paths clients send.
//! Per-database state (edges, weights, homeostat, wedged, WAL size) is
//! sampled at scrape time, labeled by db name.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Closed set of route classes (the `/db/{name}` prefix is stripped before
/// classification, so per-DB request cardinality never leaks into labels).
pub const ROUTES: &[&str] = &[
    "retrieve",
    "ingest",
    "feedback",
    "touch",
    "match",
    "consolidate",
    "sleep",
    "checkpoint",
    "purge",
    "quarantine",
    "stats",
    "graph",
    "traces",
    "trace",
    "dbs",
    "db_admin",
    "health",
    "ui",
    "other",
];

pub fn classify(db_route: &str) -> usize {
    let name = match db_route
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("")
    {
        "retrieve" => "retrieve",
        "ingest" => "ingest",
        "feedback" => "feedback",
        "touch" => "touch",
        "match" => "match",
        "consolidate" => "consolidate",
        "sleep" => "sleep",
        "checkpoint" => "checkpoint",
        "purge" => "purge",
        "quarantine" => "quarantine",
        "stats" | "metrics" => "stats",
        "graph" => "graph",
        "traces" => "traces",
        "trace" => "trace",
        "dbs" => "dbs",
        "" => "db_admin", // POST/DELETE /db/{name}
        "health" | "live" | "ready" => "health",
        "ui" => "ui",
        _ => "other",
    };
    ROUTES
        .iter()
        .position(|r| *r == name)
        .unwrap_or(ROUTES.len() - 1)
}

const STATUS_CLASSES: &[&str] = &["2xx", "4xx", "5xx"];
fn status_class(code: u16) -> usize {
    match code {
        200..=399 => 0,
        400..=499 => 1,
        _ => 2,
    }
}

/// Histogram bucket upper bounds, seconds.
const BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

struct Hist {
    buckets: Vec<AtomicU64>, // one per bound + one +Inf
    sum_micros: AtomicU64,
    count: AtomicU64,
}

impl Hist {
    fn new() -> Self {
        Hist {
            buckets: (0..=BUCKETS.len()).map(|_| AtomicU64::new(0)).collect(),
            sum_micros: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
    fn observe(&self, d: Duration) {
        let s = d.as_secs_f64();
        let idx = BUCKETS
            .iter()
            .position(|&b| s <= b)
            .unwrap_or(BUCKETS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_micros
            .fetch_add(d.as_micros() as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct RequestMetrics {
    counters: Vec<[AtomicU64; 3]>, // [route][status_class]
    hists: Vec<Hist>,              // [route]
}

impl RequestMetrics {
    pub fn new() -> Self {
        RequestMetrics {
            counters: (0..ROUTES.len()).map(|_| Default::default()).collect(),
            hists: (0..ROUTES.len()).map(|_| Hist::new()).collect(),
        }
    }

    pub fn record(&self, route: usize, status: u16, dur: Duration) {
        let r = route.min(ROUTES.len() - 1);
        self.counters[r][status_class(status)].fetch_add(1, Ordering::Relaxed);
        self.hists[r].observe(dur);
    }

    /// Render the request-side metrics in Prometheus exposition format.
    pub fn render(&self, out: &mut String) {
        out.push_str("# TYPE psyrag_requests_total counter\n");
        out.push_str(
            "# HELP psyrag_requests_total HTTP requests by route class and status class.\n",
        );
        for (r, name) in ROUTES.iter().enumerate() {
            for (sc, scname) in STATUS_CLASSES.iter().enumerate() {
                let v = self.counters[r][sc].load(Ordering::Relaxed);
                if v > 0 {
                    out.push_str(&format!(
                        "psyrag_requests_total{{route=\"{name}\",status=\"{scname}\"}} {v}\n"
                    ));
                }
            }
        }
        out.push_str("# TYPE psyrag_request_duration_seconds histogram\n");
        out.push_str("# HELP psyrag_request_duration_seconds Request latency by route class.\n");
        for (r, name) in ROUTES.iter().enumerate() {
            let h = &self.hists[r];
            if h.count.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let mut cum = 0u64;
            for (i, bound) in BUCKETS.iter().enumerate() {
                cum += h.buckets[i].load(Ordering::Relaxed);
                out.push_str(&format!(
                    "psyrag_request_duration_seconds_bucket{{route=\"{name}\",le=\"{bound}\"}} {cum}\n"
                ));
            }
            cum += h.buckets[BUCKETS.len()].load(Ordering::Relaxed);
            out.push_str(&format!(
                "psyrag_request_duration_seconds_bucket{{route=\"{name}\",le=\"+Inf\"}} {cum}\n"
            ));
            let sum = h.sum_micros.load(Ordering::Relaxed) as f64 / 1e6;
            let count = h.count.load(Ordering::Relaxed);
            out.push_str(&format!(
                "psyrag_request_duration_seconds_sum{{route=\"{name}\"}} {sum}\n"
            ));
            out.push_str(&format!(
                "psyrag_request_duration_seconds_count{{route=\"{name}\"}} {count}\n"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_covers_routes_and_strips_nothing_unknown() {
        assert_eq!(ROUTES[classify("/retrieve")], "retrieve");
        assert_eq!(ROUTES[classify("/trace/17")], "trace");
        assert_eq!(ROUTES[classify("")], "db_admin");
        assert_eq!(ROUTES[classify("/definitely-not-a-route")], "other");
        assert_eq!(ROUTES[classify("/live")], "health");
    }

    #[test]
    fn histogram_renders_cumulative_buckets() {
        let m = RequestMetrics::new();
        m.record(classify("/retrieve"), 200, Duration::from_millis(3));
        m.record(classify("/retrieve"), 200, Duration::from_millis(30));
        m.record(classify("/retrieve"), 404, Duration::from_millis(300));
        let mut s = String::new();
        m.render(&mut s);
        assert!(s.contains("psyrag_requests_total{route=\"retrieve\",status=\"2xx\"} 2"));
        assert!(s.contains("psyrag_requests_total{route=\"retrieve\",status=\"4xx\"} 1"));
        assert!(s.contains("le=\"+Inf\"} 3"));
        assert!(s.contains("psyrag_request_duration_seconds_count{route=\"retrieve\"} 3"));
        // cumulative: the 0.05 bucket holds the 3ms and 30ms observations
        assert!(s.contains("le=\"0.05\"} 2"));
    }
}
