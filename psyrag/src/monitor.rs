//! CLI monitoring dashboard. Polls a running `psyrag serve` /stats endpoint and
//! redraws a live terminal view using ANSI escapes only (no TUI dependency).

use crate::metrics::{fetch, Metrics};
use std::io::Write;
use std::time::Duration;

fn bar(v: f32, max: f32, width: usize) -> String {
    let filled = if max > 0.0 {
        ((v / max) * width as f32).round().clamp(0.0, width as f32) as usize
    } else {
        0
    };
    format!("{}{}", "#".repeat(filled), "-".repeat(width.saturating_sub(filled)))
}

fn sparkline(vals: &[f32], width: usize) -> String {
    if vals.is_empty() {
        return String::new();
    }
    let chars = ['_', '.', ':', '-', '=', '+', '*', '#'];
    let recent: Vec<f32> = vals.iter().rev().take(width).rev().copied().collect();
    let (mut lo, mut hi) = (f32::MAX, f32::MIN);
    for &v in &recent {
        lo = lo.min(v);
        hi = hi.max(v);
    }
    let span = (hi - lo).max(1e-6);
    recent
        .iter()
        .map(|&v| {
            let idx = (((v - lo) / span) * (chars.len() - 1) as f32).round() as usize;
            chars[idx.min(chars.len() - 1)]
        })
        .collect()
}

/// Render one frame to a string. Pure — unit-tested and used by the live loop.
pub fn render_frame(m: &Metrics, tick: u64, history: &[f32]) -> String {
    let err = m.ewma_mass - m.setpoint;
    let live_frac = if m.edges_total > 0 {
        m.edges_live as f32 / m.edges_total as f32
    } else {
        0.0
    };
    let mut s = String::new();
    s.push_str("+-- psyrag-graph-plasticity monitor -----------------------------+\n");
    s.push_str(&format!("| tick {:<54}|\n", tick));
    s.push_str("+--------------------------------------------------------------+\n");
    s.push_str(&format!("| mass {:>8.4}  setpoint {:>6.3}  err {:>+8.4}         |\n", m.ewma_mass, m.setpoint, err));
    s.push_str(&format!("|   mass    [{}] {:>6.3}   |\n", bar(m.ewma_mass, (m.setpoint * 2.0).max(0.1), 20), m.ewma_mass));
    s.push_str(&format!("| lambda_scale {:>6.3}  integral {:>+10.2}            |\n", m.lambda_scale, m.integral));
    s.push_str(&format!("|   l-scale [{}] {:>6.3}   |\n", bar(m.lambda_scale, 8.0, 20), m.lambda_scale));
    s.push_str("+--------------------------------------------------------------+\n");
    s.push_str(&format!("| edges total {:>8} live {:>8} dead {:>8}      |\n", m.edges_total, m.edges_live, m.edges_dead));
    s.push_str(&format!("|   live    [{}] {:>5.1}%   |\n", bar(live_frac, 1.0, 20), live_frac * 100.0));
    s.push_str(&format!("| w min {:>6.3} mean {:>6.3} max {:>6.3} nodes {:>7}  |\n", m.weight_min, m.weight_mean, m.weight_max, m.nodes));
    s.push_str("+--------------------------------------------------------------+\n");
    s.push_str(&format!("| mass trend {:<50}|\n", sparkline(history, 50)));
    s.push_str("+--------------------------------------------------------------+\n");
    s
}

pub fn run(url: &str, interval_ms: u64) -> Result<(), String> {
    let metrics_url = format!("{}/stats", url.trim_end_matches('/'));
    let mut history: Vec<f32> = Vec::new();
    let mut tick = 0u64;
    let mut out = std::io::stdout();
    loop {
        // clear screen + home cursor
        print!("\x1b[2J\x1b[H");
        match fetch(&metrics_url) {
            Ok(m) => {
                history.push(m.ewma_mass);
                if history.len() > 512 {
                    history.remove(0);
                }
                print!("{}", render_frame(&m, tick, &history));
            }
            Err(e) => {
                println!("waiting for {metrics_url} ... ({e})");
            }
        }
        out.flush().ok();
        tick += 1;
        std::thread::sleep(Duration::from_millis(interval_ms));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_renders() {
        let m = Metrics {
            edges_total: 100, edges_live: 90, edges_dead: 10, nodes: 40,
            lambda_scale: 1.2, setpoint: 0.5, ewma_mass: 0.48, integral: 12.3,
            weight_min: 0.01, weight_max: 0.9, weight_mean: 0.3,
        };
        let f = render_frame(&m, 7, &[0.1, 0.2, 0.3, 0.48]);
        assert!(f.contains("monitor"));
        assert!(f.contains("lambda_scale"));
        assert!(f.contains("mass trend"));
    }
}
