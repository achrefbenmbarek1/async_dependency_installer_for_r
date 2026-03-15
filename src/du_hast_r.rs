use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Generator};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_MANIFEST: &str = "fer.json";
const DEFAULT_LOCKFILE: &str = "nein.lock";
const DEFAULT_FETCHER: &str = "./target/debug/async_dependency_installer_for_R";
const RUNNER_SCRIPT: &str = "scripts/du_hast_r_runner.R";
const EVENT_PREFIX: &str = "DHR_EVENT ";
const MAX_LOG_LINES: usize = 160;

#[derive(Debug, Parser)]
#[command(name = "du_hast_r")]
#[command(about = "High-energy async R package manager (Rust + R planner)", long_about = None)]
struct Cli {
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Init {
        #[arg(default_value = DEFAULT_MANIFEST)]
        manifest: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Lock {
        #[arg(default_value = DEFAULT_MANIFEST)]
        manifest: PathBuf,
        #[arg(long, default_value = DEFAULT_LOCKFILE)]
        lockfile: PathBuf,
        #[arg(long, default_value = DEFAULT_FETCHER)]
        fetcher: PathBuf,
    },
    Gefragt {
        #[arg(default_value = DEFAULT_MANIFEST)]
        manifest: PathBuf,
        #[arg(long, default_value = DEFAULT_LOCKFILE)]
        lockfile: PathBuf,
        #[arg(long, default_value = DEFAULT_FETCHER)]
        fetcher: PathBuf,
        #[arg(long)]
        no_lock_write: bool,
    },
    Nein {
        package: String,
        #[arg(default_value = DEFAULT_MANIFEST)]
        manifest: PathBuf,
        #[arg(long)]
        lock: bool,
        #[arg(long, default_value = DEFAULT_LOCKFILE)]
        lockfile: PathBuf,
        #[arg(long, default_value = DEFAULT_FETCHER)]
        fetcher: PathBuf,
    },
    Import {
        #[arg(long)]
        from: PathBuf,
        #[arg(default_value = DEFAULT_MANIFEST)]
        manifest: PathBuf,
        #[arg(long)]
        force: bool,
    },
    Completions { shell: Shell },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
    Elvish,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestSettings {
    #[serde(default = "default_download_threads")]
    download_threads: usize,
    #[serde(default = "default_install_ncpus")]
    install_ncpus: usize,
    #[serde(default = "default_make_jobs")]
    make_jobs: usize,
    #[serde(default)]
    repos: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    #[serde(default = "default_name")]
    name: String,
    #[serde(default = "default_version")]
    version: String,
    #[serde(default)]
    settings: ManifestSettings,
    dependencies: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RunnerEvent {
    phase: String,
    status: String,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    total_roots: Option<u64>,
    #[serde(default)]
    packages: Option<u64>,
    #[serde(default)]
    threads: Option<u64>,
    #[serde(default)]
    layers: Option<u64>,
    #[serde(default)]
    layer: Option<u64>,
    #[serde(default)]
    completed_packages: Option<u64>,
    #[serde(default)]
    total_packages: Option<u64>,
    #[serde(default)]
    seconds: Option<f64>,
    #[serde(default)]
    downloaded_bytes: Option<u64>,
    #[serde(default)]
    reused_bytes: Option<u64>,
    #[serde(default)]
    cache_hit_rate: Option<f64>,
    #[serde(default)]
    total_seconds: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct PhaseMetrics {
    fetch_seconds: Option<f64>,
    install_seconds: Option<f64>,
    total_seconds: Option<f64>,
    downloaded_bytes: Option<u64>,
    reused_bytes: Option<u64>,
    cache_hit_rate: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct UiPhaseState {
    resolve_done: bool,
    fetch_done: bool,
    install_done: bool,
}

impl Default for ManifestSettings {
    fn default() -> Self {
        Self {
            download_threads: default_download_threads(),
            install_ncpus: default_install_ncpus(),
            make_jobs: default_make_jobs(),
            repos: BTreeMap::new(),
        }
    }
}

impl Default for Manifest {
    fn default() -> Self {
        let mut dependencies = BTreeMap::new();
        dependencies.insert("BiocGenerics".to_string(), "0.56.0".to_string());
        Self {
            name: default_name(),
            version: default_version(),
            settings: ManifestSettings::default(),
            dependencies,
        }
    }
}

fn default_name() -> String {
    "du_hast_r_project".to_string()
}
fn default_version() -> String {
    "0.1.0".to_string()
}
fn default_download_threads() -> usize {
    16
}
fn default_install_ncpus() -> usize {
    2
}
fn default_make_jobs() -> usize {
    4
}

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let cli = Cli::parse();
    let verbose = cli.verbose;

    match cli.command {
        Commands::Init { manifest, force } => cmd_init(&manifest, force),
        Commands::Lock {
            manifest,
            lockfile,
            fetcher,
        } => cmd_lock(&manifest, &lockfile, &fetcher, verbose),
        Commands::Gefragt {
            manifest,
            lockfile,
            fetcher,
            no_lock_write,
        } => cmd_gefragt(&manifest, &lockfile, &fetcher, no_lock_write, verbose),
        Commands::Nein {
            package,
            manifest,
            lock,
            lockfile,
            fetcher,
        } => cmd_nein(&package, &manifest, lock, &lockfile, &fetcher, verbose),
        Commands::Import {
            from,
            manifest,
            force,
        } => cmd_import(&from, &manifest, force),
        Commands::Completions { shell } => cmd_completions(shell),
    }
}

fn cmd_init(path: &Path, force: bool) -> Result<(), String> {
    if path.exists() && !force {
        return Err(format!(
            "manifest already exists at {} (use --force to overwrite)",
            path.display()
        ));
    }
    let payload =
        serde_json::to_string_pretty(&Manifest::default()).map_err(|e| format!("serialize manifest: {e}"))?;
    fs::write(path, payload).map_err(|e| format!("write manifest {}: {e}", path.display()))?;
    println!("WROTE {}", path.display());
    println!("NEXT: du_hast_r lock && du_hast_r gefragt");
    Ok(())
}

fn cmd_lock(
    manifest_path: &Path,
    lockfile_path: &Path,
    fetcher_path: &Path,
    verbose: bool,
) -> Result<(), String> {
    let manifest = read_manifest(manifest_path)?;
    validate_manifest(&manifest)?;
    let metrics = run_runner("lock", manifest_path, lockfile_path, fetcher_path, verbose)?;
    attach_manifest_hash(lockfile_path, manifest_path)?;
    println!("LOCKED {}", lockfile_path.display());
    print_metrics(metrics);
    Ok(())
}

fn cmd_gefragt(
    manifest_path: &Path,
    lockfile_path: &Path,
    fetcher_path: &Path,
    no_lock_write: bool,
    verbose: bool,
) -> Result<(), String> {
    let manifest = read_manifest(manifest_path)?;
    validate_manifest(&manifest)?;

    if !lockfile_path.exists() {
        if no_lock_write {
            return Err(format!(
                "{} is missing and --no-lock-write was set",
                lockfile_path.display()
            ));
        }
        cmd_lock(manifest_path, lockfile_path, fetcher_path, verbose)?;
    }

    validate_lock_manifest_hash(lockfile_path, manifest_path)?;
    let metrics = run_runner("install", manifest_path, lockfile_path, fetcher_path, verbose)?;
    println!("DONE gefragt using {}", lockfile_path.display());
    print_metrics(metrics);
    Ok(())
}

fn cmd_nein(
    package: &str,
    manifest_path: &Path,
    lock: bool,
    lockfile_path: &Path,
    fetcher_path: &Path,
    verbose: bool,
) -> Result<(), String> {
    let mut manifest = read_manifest(manifest_path)?;
    if manifest.dependencies.remove(package).is_none() {
        return Err(format!(
            "package '{}' not found in {}",
            package,
            manifest_path.display()
        ));
    }

    let payload =
        serde_json::to_string_pretty(&manifest).map_err(|e| format!("encode manifest JSON: {e}"))?;
    fs::write(manifest_path, payload)
        .map_err(|e| format!("write manifest {}: {e}", manifest_path.display()))?;

    println!("REMOVED {} from {}", package, manifest_path.display());
    if lock {
        cmd_lock(manifest_path, lockfile_path, fetcher_path, verbose)?;
    }
    Ok(())
}

fn cmd_import(from: &Path, manifest_path: &Path, force: bool) -> Result<(), String> {
    if manifest_path.exists() && !force {
        return Err(format!(
            "manifest already exists at {} (use --force to overwrite)",
            manifest_path.display()
        ));
    }

    let dependencies = if is_renv_lock(from) {
        import_renv_lock(from)?
    } else if is_description(from) {
        import_description(from)?
    } else {
        return Err(format!(
            "unsupported import source {} (expected renv.lock or DESCRIPTION)",
            from.display()
        ));
    };

    if dependencies.is_empty() {
        return Err("import produced no dependencies".to_string());
    }

    let manifest = Manifest {
        dependencies,
        ..Manifest::default()
    };
    let payload = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("serialize imported manifest: {e}"))?;
    fs::write(manifest_path, payload)
        .map_err(|e| format!("write manifest {}: {e}", manifest_path.display()))?;

    println!("IMPORTED {} -> {}", from.display(), manifest_path.display());
    Ok(())
}

fn cmd_completions(shell: Shell) -> Result<(), String> {
    let mut cmd = Cli::command();
    match shell {
        Shell::Bash => emit_completions(clap_complete::shells::Bash, &mut cmd),
        Shell::Zsh => emit_completions(clap_complete::shells::Zsh, &mut cmd),
        Shell::Fish => emit_completions(clap_complete::shells::Fish, &mut cmd),
        Shell::PowerShell => emit_completions(clap_complete::shells::PowerShell, &mut cmd),
        Shell::Elvish => emit_completions(clap_complete::shells::Elvish, &mut cmd),
    }
    Ok(())
}

fn emit_completions<G: Generator>(generator: G, cmd: &mut clap::Command) {
    generate(generator, cmd, "du_hast_r", &mut std::io::stdout());
}

fn read_manifest(path: &Path) -> Result<Manifest, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("read manifest {}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("invalid manifest JSON: {e}"))
}

fn validate_manifest(manifest: &Manifest) -> Result<(), String> {
    if manifest.dependencies.is_empty() {
        return Err("manifest.dependencies is empty".to_string());
    }
    for (pkg, ver) in &manifest.dependencies {
        if pkg.trim().is_empty() {
            return Err("manifest has an empty dependency name".to_string());
        }
        if ver.trim().is_empty() {
            return Err(format!("dependency {pkg} has empty version"));
        }
    }
    Ok(())
}

fn build_runner_command(mode: &str, manifest: &Path, lockfile: &Path, fetcher: &Path) -> Command {
    let mut cmd = Command::new("Rscript");
    cmd.arg(RUNNER_SCRIPT)
        .arg(mode)
        .arg(manifest)
        .arg(lockfile)
        .arg(fetcher)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

fn run_runner(
    mode: &str,
    manifest_path: &Path,
    lockfile_path: &Path,
    fetcher_path: &Path,
    verbose: bool,
) -> Result<PhaseMetrics, String> {
    let mut cmd = build_runner_command(mode, manifest_path, lockfile_path, fetcher_path);
    run_with_multibar(&mut cmd, verbose)
}

fn run_with_multibar(command: &mut Command, verbose: bool) -> Result<PhaseMetrics, String> {
    let mut child = command
        .spawn()
        .map_err(|e| format!("failed to spawn command: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture stderr".to_string())?;

    let multi = MultiProgress::new();
    let resolve_pb = multi.add(phase_spinner("RESOLVE", "mapping dependency graph"));
    let fetch_pb = multi.add(phase_spinner("FETCH", "syncing source artifacts"));
    let install_pb = multi.add(progress_bar_install("assembling layered install"));

    let (tx, rx) = mpsc::channel::<(bool, String)>();
    let tx_out = tx.clone();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            let _ = tx_out.send((false, line));
        }
    });
    thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            let _ = tx.send((true, line));
        }
    });

    let started = Instant::now();
    let mut metrics = PhaseMetrics::default();
    let mut ui_state = UiPhaseState::default();
    let mut ring = VecDeque::<String>::new();

    loop {
        if let Ok((is_stderr, line)) = rx.recv_timeout(Duration::from_millis(120)) {
            if let Some(event) = parse_event(&line) {
                apply_event(
                    &event,
                    &resolve_pb,
                    &fetch_pb,
                    &install_pb,
                    &mut metrics,
                    &mut ui_state,
                );
            } else if !line.trim().is_empty() {
                if verbose {
                    if is_stderr {
                        println!("[stderr] {line}");
                    } else {
                        println!("{line}");
                    }
                } else {
                    let entry = if is_stderr {
                        format!("[stderr] {line}")
                    } else {
                        line
                    };
                    ring.push_back(entry);
                    if ring.len() > MAX_LOG_LINES {
                        let _ = ring.pop_front();
                    }
                }
            }
        }

        match child.try_wait() {
            Ok(Some(status)) => {
                if !ui_state.resolve_done {
                    resolve_pb.tick();
                }
                if !ui_state.fetch_done {
                    fetch_pb.tick();
                }
                if !ui_state.install_done {
                    install_pb.set_position(100);
                }
                resolve_pb.finish_with_message("resolve complete".to_string());
                fetch_pb.finish_with_message("fetch complete".to_string());
                install_pb.finish_with_message("install complete".to_string());

                while let Ok((is_stderr, line)) = rx.try_recv() {
                    if parse_event(&line).is_none() && !line.trim().is_empty() {
                        if verbose {
                            if is_stderr {
                                println!("[stderr] {line}");
                            } else {
                                println!("{line}");
                            }
                        } else {
                            let entry = if is_stderr {
                                format!("[stderr] {line}")
                            } else {
                                line
                            };
                            ring.push_back(entry);
                            if ring.len() > MAX_LOG_LINES {
                                let _ = ring.pop_front();
                            }
                        }
                    }
                }

                if status.success() {
                    if metrics.total_seconds.is_none() {
                        metrics.total_seconds = Some(started.elapsed().as_secs_f64());
                    }
                    return Ok(metrics);
                }

                let details = if ring.is_empty() {
                    "<no command output captured>".to_string()
                } else {
                    ring.into_iter().collect::<Vec<_>>().join("\n")
                };
                return Err(format!("runner failed with status {status}\n{details}"));
            }
            Ok(None) => {
                pulse_phase(&resolve_pb, ui_state.resolve_done);
                pulse_phase(&fetch_pb, ui_state.fetch_done);
                pulse_phase(&install_pb, ui_state.install_done);
            }
            Err(e) => return Err(format!("failed while waiting for runner process: {e}")),
        }
    }
}

fn phase_spinner(prefix: &str, msg: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    let style = ProgressStyle::with_template(
        "{spinner:.cyan.bold} {prefix:>8.bold} {msg:.bright_white} {elapsed_precise}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_spinner())
    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]);
    pb.set_style(style);
    pb.set_prefix(prefix.to_string());
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_message(msg.to_string());
    pb
}

fn progress_bar_install(msg: &str) -> ProgressBar {
    let pb = ProgressBar::new(100);
    let style = ProgressStyle::with_template(
        "{prefix:>8.bold.magenta} [{bar:32.magenta/black}] {pos:>3}% {msg:.bright_white}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("=>-");
    pb.set_style(style);
    pb.set_prefix("INSTALL".to_string());
    pb.set_position(2);
    pb.enable_steady_tick(Duration::from_millis(120));
    pb.set_message(msg.to_string());
    pb
}

fn pulse_phase(pb: &ProgressBar, done: bool) {
    if done {
        return;
    }
    let pos = pb.position();
    if pos < 92 {
        pb.set_position(pos + 1);
    }
    pb.tick();
}

fn parse_event(line: &str) -> Option<RunnerEvent> {
    serde_json::from_str(line.strip_prefix(EVENT_PREFIX)?).ok()
}

fn apply_event(
    event: &RunnerEvent,
    resolve_pb: &ProgressBar,
    fetch_pb: &ProgressBar,
    install_pb: &ProgressBar,
    metrics: &mut PhaseMetrics,
    ui_state: &mut UiPhaseState,
) {
    match (event.phase.as_str(), event.status.as_str()) {
        ("resolve", "start") => {
            resolve_pb.set_message(format!("resolving {} roots", event.total_roots.unwrap_or(0)));
        }
        ("resolve", "done") => {
            resolve_pb.set_message(format!(
                "resolved {} packages in {:.2}s",
                event.packages.unwrap_or(0),
                event.seconds.unwrap_or(0.0)
            ));
            ui_state.resolve_done = true;
        }
        ("fetch", "start") => {
            fetch_pb.set_message(format!("{} downloader threads", event.threads.unwrap_or(0)));
        }
        ("fetch", "done") => {
            let secs = event.seconds.unwrap_or(0.0);
            let dl = event.downloaded_bytes.unwrap_or(0);
            let reused = event.reused_bytes.unwrap_or(0);
            let hit = event.cache_hit_rate.unwrap_or(0.0) * 100.0;
            let speed = if secs > 0.0 { dl as f64 / secs } else { 0.0 };
            fetch_pb.set_message(format!(
                "{:.2}s | dl {} | reused {} | {:.1}% cache | {}/s",
                secs,
                human_bytes(dl),
                human_bytes(reused),
                hit,
                human_bytes(speed as u64)
            ));
            metrics.fetch_seconds = Some(secs);
            metrics.downloaded_bytes = Some(dl);
            metrics.reused_bytes = Some(reused);
            metrics.cache_hit_rate = Some(event.cache_hit_rate.unwrap_or(0.0));
            ui_state.fetch_done = true;
        }
        ("install", "start") => {
            install_pb.set_position(8);
            install_pb.set_message(format!("{} layers", event.layers.unwrap_or(0)));
        }
        ("install", "progress") => {
            let done = event.completed_packages.unwrap_or(0);
            let total = event.total_packages.unwrap_or(1).max(1);
            let pct = ((done as f64 / total as f64) * 100.0).round() as u64;
            install_pb.set_position(pct.min(99));
            install_pb.set_message(format!(
                "layer {}/{} | pkg {}/{}",
                event.layer.unwrap_or(0),
                event.layers.unwrap_or(0),
                done,
                total
            ));
        }
        ("install", "done") => {
            install_pb.set_position(100);
            install_pb.set_message(format!("installed in {:.2}s", event.seconds.unwrap_or(0.0)));
            metrics.install_seconds = event.seconds;
            ui_state.install_done = true;
        }
        ("done", "done") => metrics.total_seconds = event.total_seconds,
        _ => {
            if let Some(msg) = &event.message {
                resolve_pb.set_message(msg.clone());
            }
        }
    }
}

fn print_metrics(metrics: PhaseMetrics) {
    if let Some(total) = metrics.total_seconds {
        println!(
            "SUMMARY total={:.2}s fetch={:.2}s install={:.2}s downloaded={} reused={} cache_hit={:.1}%",
            total,
            metrics.fetch_seconds.unwrap_or(0.0),
            metrics.install_seconds.unwrap_or(0.0),
            human_bytes(metrics.downloaded_bytes.unwrap_or(0)),
            human_bytes(metrics.reused_bytes.unwrap_or(0)),
            metrics.cache_hit_rate.unwrap_or(0.0) * 100.0
        );
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut val = bytes as f64;
    let mut idx = 0usize;
    while val >= 1024.0 && idx + 1 < UNITS.len() {
        val /= 1024.0;
        idx += 1;
    }
    format!("{val:.1}{}", UNITS[idx])
}

fn attach_manifest_hash(lockfile_path: &Path, manifest_path: &Path) -> Result<(), String> {
    let lock_text = fs::read_to_string(lockfile_path)
        .map_err(|e| format!("read lockfile {}: {e}", lockfile_path.display()))?;
    let mut lock_json: Value =
        serde_json::from_str(&lock_text).map_err(|e| format!("invalid lockfile JSON: {e}"))?;

    let manifest_bytes = fs::read(manifest_path)
        .map_err(|e| format!("read manifest for hash {}: {e}", manifest_path.display()))?;
    let manifest_sha256 = hex::encode(Sha256::digest(manifest_bytes));

    if !lock_json.is_object() {
        return Err("lockfile root must be a JSON object".to_string());
    }

    lock_json["manifest_sha256"] = Value::String(manifest_sha256);
    lock_json["manifest_path"] = Value::String(manifest_path.display().to_string());

    let payload =
        serde_json::to_string_pretty(&lock_json).map_err(|e| format!("encode lockfile JSON: {e}"))?;
    fs::write(lockfile_path, payload)
        .map_err(|e| format!("write lockfile {}: {e}", lockfile_path.display()))?;
    Ok(())
}

fn validate_lock_manifest_hash(lockfile_path: &Path, manifest_path: &Path) -> Result<(), String> {
    let lock_text = fs::read_to_string(lockfile_path)
        .map_err(|e| format!("read lockfile {}: {e}", lockfile_path.display()))?;
    let lock_json: Value =
        serde_json::from_str(&lock_text).map_err(|e| format!("invalid lockfile JSON: {e}"))?;

    let Some(lock_hash) = lock_json.get("manifest_sha256").and_then(Value::as_str) else {
        return Ok(());
    };

    let manifest_bytes = fs::read(manifest_path)
        .map_err(|e| format!("read manifest for hash {}: {e}", manifest_path.display()))?;
    let manifest_sha256 = hex::encode(Sha256::digest(manifest_bytes));
    if lock_hash != manifest_sha256 {
        return Err(format!(
            "lockfile {} is stale for manifest {} (run `du_hast_r lock` first)",
            lockfile_path.display(),
            manifest_path.display()
        ));
    }
    Ok(())
}

fn is_renv_lock(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("renv.lock"))
        .unwrap_or(false)
}

fn is_description(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("DESCRIPTION"))
        .unwrap_or(false)
}

fn import_renv_lock(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("read renv.lock {}: {e}", path.display()))?;
    let root: Value = serde_json::from_str(&text).map_err(|e| format!("invalid renv.lock JSON: {e}"))?;

    let packages = root
        .get("Packages")
        .and_then(Value::as_object)
        .ok_or_else(|| "renv.lock missing Packages object".to_string())?;

    let mut deps = BTreeMap::new();
    for (name, entry) in packages {
        let version = entry
            .get("Version")
            .and_then(Value::as_str)
            .unwrap_or("*")
            .to_string();
        deps.insert(name.clone(), version);
    }
    Ok(deps)
}

fn import_description(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let text = fs::read_to_string(path)
        .map_err(|e| format!("read DESCRIPTION {}: {e}", path.display()))?;
    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    let mut current_key: Option<String> = None;

    for raw_line in text.lines() {
        if raw_line.trim().is_empty() {
            continue;
        }
        if raw_line.starts_with(' ') || raw_line.starts_with('\t') {
            if let Some(key) = &current_key {
                let value = fields.entry(key.clone()).or_default();
                value.push(' ');
                value.push_str(raw_line.trim());
            }
            continue;
        }

        if let Some((key, value)) = raw_line.split_once(':') {
            let key = key.trim().to_string();
            fields.insert(key.clone(), value.trim().to_string());
            current_key = Some(key);
        }
    }

    let mut deps = BTreeMap::new();
    for field_name in ["Depends", "Imports", "LinkingTo"] {
        if let Some(value) = fields.get(field_name) {
            for token in value.split(',') {
                let clean = token.split('(').next().unwrap_or("").trim();
                if clean.is_empty() || clean == "R" {
                    continue;
                }
                deps.entry(clean.to_string()).or_insert_with(|| "*".to_string());
            }
        }
    }
    Ok(deps)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_description_dependencies() {
        let dcf = "Package: demo\nVersion: 0.1.0\nImports: foo (>= 1.0), bar\nDepends: R (>= 4.3), baz\nLinkingTo: qux\n";
        let path = std::env::temp_dir().join("du_hast_r_DESCRIPTION_test");
        fs::write(&path, dcf).expect("write temp DESCRIPTION");

        let deps = import_description(&path).expect("parse description");
        assert!(deps.contains_key("foo"));
        assert!(deps.contains_key("bar"));
        assert!(deps.contains_key("baz"));
        assert!(deps.contains_key("qux"));
        assert!(!deps.contains_key("R"));

        let _ = fs::remove_file(path);
    }
}
