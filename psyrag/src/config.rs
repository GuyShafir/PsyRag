//! Config plumbing: load `psyrag_core::Config` from a JSON file, fall
//! back to defaults, and emit a fully-commented example. (JSON, not TOML, to keep
//! the dependency surface to serde_json only — swap in TOML later if wanted.)

use psyrag_core::Config;

pub fn load(path: Option<&str>) -> Result<Config, String> {
    match path {
        None => Ok(Config::default()),
        Some(p) => {
            let s = std::fs::read_to_string(p).map_err(|e| format!("read {p}: {e}"))?;
            serde_json::from_str(&s).map_err(|e| format!("parse {p}: {e}"))
        }
    }
}

/// Example config: every knob with its default. `psyrag config --write path` emits
/// this; the `_comment_*` keys are ignored on load (deny_unknown_fields is off in
/// the JSON path because this example carries doc keys).
pub fn example_json() -> String {
    let cfg = Config::default();
    let mut v = serde_json::to_value(&cfg).unwrap();
    if let serde_json::Value::Object(ref mut m) = v {
        m.insert("_comment".into(), serde_json::json!(
            "psyrag-graph-plasticity config. Update rule w(t)=w(prev)*e^{-lambda*dt}+alpha*R. \
             alpha=reinforcement gain; lambda_base=decay/sec; beta=authority sensitivity; \
             w0=initial weight; r_clip=touch clip; setpoint=target activated mass; \
             k_i=integral gain; ewma_beta=mass smoothing; scale_min/max=lambda_scale bounds; \
             integral_min/max=anti-windup; depth/fan=retrieval spread; theta=prune floor; \
             norm_target=per-source L1 budget; authority_by_kind={KIND:authority} raises \
             decay resistance; authority_default for unlisted kinds; functional_kinds=[KIND] declares single-valued predicates (only these are conflict-checked in consolidation); feedback_gamma=path-credit decay per hop; feedback_hit=credit for a used node; feedback_miss_penalty=negative credit for surfaced-but-unused (0=off)."
        ));
    }
    serde_json::to_string_pretty(&v).unwrap()
}
