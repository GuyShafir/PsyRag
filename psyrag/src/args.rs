//! Tiny dependency-free arg parser. `--key value` (repeatable) and `--flag`.
//! A single `-` prefix is NOT a flag, so negative numbers pass through as values.

use std::collections::HashMap;

pub struct Args {
    pub positionals: Vec<String>,
    flags: HashMap<String, Vec<String>>,
    bools: Vec<String>,
}

impl Args {
    pub fn parse(argv: impl IntoIterator<Item = String>) -> Self {
        let toks: Vec<String> = argv.into_iter().collect();
        let mut positionals = Vec::new();
        let mut flags: HashMap<String, Vec<String>> = HashMap::new();
        let mut bools = Vec::new();
        let mut i = 0;
        while i < toks.len() {
            let t = &toks[i];
            if let Some(key) = t.strip_prefix("--") {
                let (key, inline) = match key.split_once('=') {
                    Some((k, v)) => (k.to_string(), Some(v.to_string())),
                    None => (key.to_string(), None),
                };
                if let Some(v) = inline {
                    flags.entry(key).or_default().push(v);
                } else if i + 1 < toks.len() && !toks[i + 1].starts_with("--") {
                    flags.entry(key).or_default().push(toks[i + 1].clone());
                    i += 1;
                } else {
                    bools.push(key);
                }
            } else {
                positionals.push(t.clone());
            }
            i += 1;
        }
        Args { positionals, flags, bools }
    }

    pub fn subcommand(&self) -> Option<&str> {
        self.positionals.first().map(|s| s.as_str())
    }
    pub fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).and_then(|v| v.last()).map(|s| s.as_str())
    }
    pub fn get_all(&self, key: &str) -> Vec<String> {
        self.flags.get(key).cloned().unwrap_or_default()
    }
    pub fn has(&self, key: &str) -> bool {
        self.bools.iter().any(|b| b == key) || self.flags.contains_key(key)
    }
    pub fn get_or<'a>(&'a self, key: &str, default: &'a str) -> &'a str {
        self.get(key).unwrap_or(default)
    }
    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|s| s.parse().ok())
    }
    pub fn get_u32(&self, key: &str) -> Option<u32> {
        self.get(key).and_then(|s| s.parse().ok())
    }
    pub fn get_f32(&self, key: &str) -> Option<f32> {
        self.get(key).and_then(|s| s.parse().ok())
    }
    pub fn get_usize(&self, key: &str) -> Option<usize> {
        self.get(key).and_then(|s| s.parse().ok())
    }
}
