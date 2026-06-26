use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn python_executable(root: &Path) -> PathBuf {
    let venv = root.join(".venv/bin/python3");
    if venv.is_file() {
        return venv;
    }
    PathBuf::from("python3")
}

#[test]
fn lb_ms_equivalence() {
    let root = repo_root();
    let script = root.join("compare_lb_ms.py");
    assert!(script.is_file(), "compare_lb_ms.py not found at {}", script.display());

    let lb_binary = env::var("CARGO_BIN_EXE_lb").expect("CARGO_BIN_EXE_lb must be set");
    let ms_binary = env::var("CARGO_BIN_EXE_ms").expect("CARGO_BIN_EXE_ms must be set");

    let status = Command::new(python_executable(&root))
        .arg(&script)
        .arg("--scenario")
        .arg("all")
        .arg("--n")
        .arg("200000")
        .arg("--no-build")
        .arg("--lb-binary")
        .arg(&lb_binary)
        .arg("--ms-binary")
        .arg(&ms_binary)
        .current_dir(&root)
        .status()
        .expect("failed to spawn compare_lb_ms.py");

    assert!(
        status.success(),
        "compare_lb_ms.py failed with status {status:?}"
    );
}
