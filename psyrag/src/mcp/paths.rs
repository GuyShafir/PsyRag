use std::path::{Path, PathBuf};

pub struct Paths {
    pub dir: PathBuf,
    pub wal: PathBuf,
    pub sidecar: PathBuf,
    pub sock: PathBuf,
    pub last_sleep: PathBuf,
}

/// Walk up from `start` to the first ancestor containing `.psyrag/` or
/// `.git/`, then anchor `.psyrag/` there (created if absent). This makes the
/// server and the hook shim resolve the same directory from any cwd inside
/// the repo, with no per-project config.
pub fn resolve(start: &Path) -> Result<Paths, String> {
    let start = start.canonicalize().map_err(|e| format!("cwd: {e}"))?;
    let mut cur: Option<&Path> = Some(&start);
    let mut root: Option<PathBuf> = None;
    while let Some(d) = cur {
        if d.join(".psyrag").is_dir() || d.join(".git").exists() {
            root = Some(d.to_path_buf());
            break;
        }
        cur = d.parent();
    }
    let root = root.ok_or_else(|| {
        "no .psyrag/ or .git/ found from cwd upward; run inside a project".to_string()
    })?;
    let dir = root.join(".psyrag");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    Ok(Paths {
        wal: dir.join("mem.wal"),
        sidecar: dir.join("mem.wal.psyrag.json"),
        sock: dir.join("mcp.sock"),
        last_sleep: dir.join("last_sleep"),
        dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_psyrag_beside_git_from_nested_cwd() {
        let tmp = std::env::temp_dir().join(format!("psyrag-paths-{}", std::process::id()));
        let nested = tmp.join("a/b/c");
        std::fs::create_dir_all(tmp.join(".git")).unwrap();
        std::fs::create_dir_all(&nested).unwrap();
        let p = resolve(&nested).unwrap();
        let canonical_tmp = tmp.canonicalize().unwrap();
        assert_eq!(p.dir, canonical_tmp.join(".psyrag"));
        assert_eq!(p.wal, canonical_tmp.join(".psyrag/mem.wal"));
        assert_eq!(p.sock, canonical_tmp.join(".psyrag/mcp.sock"));
        assert!(p.dir.is_dir(), "resolve creates the .psyrag dir");
        std::fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn errors_when_no_repo_root_found() {
        let tmp = std::env::temp_dir().join(format!("psyrag-noroot-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        assert!(resolve(&tmp).is_err());
        std::fs::remove_dir_all(&tmp).ok();
    }
}
