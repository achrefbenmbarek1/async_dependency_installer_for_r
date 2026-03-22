use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Generator};
use crossterm::{
    event::{self, Event as CEvent, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph},
};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use async_dependency_installer_for_r::DynamicMode;
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, IsTerminal, Stdout, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_MANIFEST: &str = "fer.json";
const DEFAULT_LOCKFILE: &str = "nein.lock";
const DEFAULT_FETCHER: &str = "./target/debug/async_dependency_installer_for_R";
const RUNNER_SCRIPT: &str = "scripts/du_hast_r_runner.R";
const EVENT_PREFIX: &str = "DHR_EVENT ";
const MAX_LOG_LINES: usize = 160;
const TUI_TICK: Duration = Duration::from_millis(100);
const RESOURCE_TICK: Duration = Duration::from_millis(800);

#[derive(Debug, Parser)]
#[command(name = "du_hast_r")]
#[command(about = "High-energy async R package manager (Rust + R planner)", long_about = None)]
struct Cli {
    #[arg(long, global = true)]
    verbose: bool,
    #[arg(long, global = true, help = "Render install progress in a full-screen terminal UI")]
    tui: bool,
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
    #[serde(default)]
    dynamics: bool,
    #[serde(default = "default_dynamic_mode")]
    dynamic_mode: DynamicMode,
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
    make_jobs: Option<u64>,
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
    #[serde(default)]
    lib: Option<String>,
    #[serde(default)]
    package: Option<String>,
    #[serde(default)]
    mem_available_ratio: Option<f64>,
    #[serde(default)]
    swap_used_ratio: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct PhaseMetrics {
    fetch_seconds: Option<f64>,
    install_seconds: Option<f64>,
    total_seconds: Option<f64>,
    downloaded_bytes: Option<u64>,
    reused_bytes: Option<u64>,
    cache_hit_rate: Option<f64>,
    install_lib: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct UiPhaseState {
    resolve_done: bool,
    fetch_done: bool,
    install_done: bool,
}

#[derive(Debug, Clone)]
struct TuiState {
    mode: String,
    phase_message: String,
    resolve_pct: u16,
    fetch_pct: u16,
    install_pct: u16,
    metrics: PhaseMetrics,
    logs: VecDeque<String>,
    packages: BTreeMap<String, PackageActivity>,
    current_package: Option<String>,
    download_threads: u64,
    install_threads: u64,
    active_phase: String,
    core_count: usize,
    f_prefix_pending: bool,
    input_mode: InputMode,
    package_search: String,
    log_search: String,
    package_search_error: Option<String>,
    log_search_error: Option<String>,
    g_prefix_pending: bool,
    log_cursor: usize,
    log_anchor: Option<usize>,
    log_view_offset: usize,
    log_selection_active: bool,
    mem_total_bytes: Option<u64>,
    mem_used_bytes: Option<u64>,
    proc_rss_bytes: Option<u64>,
    last_resource_refresh: Instant,
    status_message: Option<String>,
    started: Instant,
    completed: bool,
}

#[derive(Debug, Clone, Default)]
struct PackageActivity {
    downloaded: bool,
    installing: bool,
    compiling: bool,
    done: bool,
    last_note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum InputMode {
    #[default]
    Normal,
    PackageSearch,
    LogSearch,
    LogVisual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyAction {
    None,
    Quit,
}

impl TuiState {
    fn new(mode: &str) -> Self {
        Self {
            mode: mode.to_string(),
            phase_message: "waiting for planner events".to_string(),
            resolve_pct: 3,
            fetch_pct: 3,
            install_pct: 3,
            metrics: PhaseMetrics::default(),
            logs: VecDeque::new(),
            packages: BTreeMap::new(),
            current_package: None,
            download_threads: 0,
            install_threads: 0,
            active_phase: "resolve".to_string(),
            core_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            f_prefix_pending: false,
            input_mode: InputMode::Normal,
            package_search: String::new(),
            log_search: String::new(),
            package_search_error: None,
            log_search_error: None,
            g_prefix_pending: false,
            log_cursor: 0,
            log_anchor: None,
            log_view_offset: 0,
            log_selection_active: false,
            mem_total_bytes: None,
            mem_used_bytes: None,
            proc_rss_bytes: None,
            last_resource_refresh: Instant::now() - RESOURCE_TICK,
            status_message: None,
            started: Instant::now(),
            completed: false,
        }
    }

    fn push_log(&mut self, entry: String) {
        self.track_package_from_line(&entry);
        self.logs.push_back(entry);
    }

    fn track_package_from_line(&mut self, line: &str) {
        let lower = line.to_lowercase();
        if let Some(name) = extract_pkg_from_tarball(line) {
            let entry = self.packages.entry(name).or_default();
            entry.downloaded = true;
            entry.last_note = "artifact staged".to_string();
        }
        if let Some(name) = extract_install_pkg(line) {
            self.current_package = Some(name.clone());
            let entry = self.packages.entry(name).or_default();
            entry.installing = true;
            entry.last_note = "installing".to_string();
        } else if lower.contains("** libs") || lower.contains("compil") {
            if let Some(pkg) = &self.current_package {
                let entry = self.packages.entry(pkg.clone()).or_default();
                entry.compiling = true;
                entry.last_note = "compiling native code".to_string();
            }
        } else if lower.contains("* done")
            || lower.contains("installation of package")
            || lower.contains("byte-compile")
        {
            if let Some(pkg) = &self.current_package {
                let entry = self.packages.entry(pkg.clone()).or_default();
                entry.done = true;
                entry.installing = false;
                entry.last_note = "installed".to_string();
            }
        }
    }

    fn active_threads(&self) -> u64 {
        match self.active_phase.as_str() {
            "fetch" => self.download_threads.max(1),
            "install" => self.install_threads.max(1),
            _ => 1,
        }
    }

    fn maybe_refresh_resources(&mut self) {
        if self.last_resource_refresh.elapsed() < RESOURCE_TICK {
            return;
        }
        self.last_resource_refresh = Instant::now();
        let (used, total) = read_system_memory();
        self.mem_used_bytes = used;
        self.mem_total_bytes = total;
        self.proc_rss_bytes = read_process_rss();
    }

    fn filtered_log_indices(&self) -> Vec<usize> {
        let log_filter = compile_regex(&self.log_search);
        self.logs
            .iter()
            .enumerate()
            .filter(|(_, line)| log_filter.as_ref().is_none_or(|re| re.is_match(line)))
            .map(|(idx, _)| idx)
            .collect()
    }

    fn enter_visual_mode(&mut self, panel_height: usize) {
        let filtered = self.filtered_log_indices();
        if filtered.is_empty() {
            self.status_message = Some("no logs available for visual mode".to_string());
            return;
        }
        self.input_mode = InputMode::LogVisual;
        self.g_prefix_pending = false;
        let last = filtered.len().saturating_sub(1);
        self.log_cursor = last;
        self.log_anchor = None;
        self.log_selection_active = false;
        self.align_log_view_for_cursor(panel_height, filtered.len());
        self.status_message = Some("log mode: move with j/k/h/l, select with V".to_string());
    }

    fn align_log_view_for_cursor(&mut self, panel_height: usize, total: usize) {
        let page = panel_height.max(1);
        if self.log_cursor < self.log_view_offset {
            self.log_view_offset = self.log_cursor;
        } else if self.log_cursor >= self.log_view_offset + page {
            self.log_view_offset = self.log_cursor + 1 - page;
        }
        if total <= page {
            self.log_view_offset = 0;
        } else {
            self.log_view_offset = self.log_view_offset.min(total - page);
        }
    }
}

struct TuiSession {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TuiSession {
    fn enter() -> Result<Self, String> {
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen)
            .map_err(|e| format!("failed to enter alternate screen: {e}"))?;
        enable_raw_mode().map_err(|e| format!("failed to enable raw mode: {e}"))?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend).map_err(|e| format!("failed to open terminal UI: {e}"))?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, state: &TuiState) -> Result<(), String> {
        self.terminal
            .draw(|frame| draw_tui(frame, state))
            .map_err(|e| format!("failed to draw terminal UI: {e}"))?;
        Ok(())
    }

    fn log_panel_rows(&self) -> usize {
        let Ok(area) = self.terminal.size() else {
            return 8;
        };
        let top = 3u16 + 11u16 + 8u16 + 3u16;
        let body = area.height.saturating_sub(top);
        body.saturating_sub(2) as usize
    }
}

impl Drop for TuiSession {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

impl Default for ManifestSettings {
    fn default() -> Self {
        Self {
            dynamics: false,
            dynamic_mode: default_dynamic_mode(),
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
fn default_dynamic_mode() -> DynamicMode {
    DynamicMode::SharedServer
}
fn default_download_threads() -> usize {
    // I'm optimizing for cran/bioconductor since it appears that most of their mirrors can only
    // handle 4-8 concurrent connections
    let logical_cpus = num_cpus::get();
    (logical_cpus as f32 * 0.75).round().max(2.0).min(12.0) as usize
}
fn default_install_ncpus() -> usize {
    num_cpus::get_physical().min(8)
}
fn default_make_jobs() -> usize {
    num_cpus::get_physical().min(8)
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
    let tui = cli.tui;

    match cli.command {
        Commands::Init { manifest, force } => cmd_init(&manifest, force),
        Commands::Lock {
            manifest,
            lockfile,
            fetcher,
        } => cmd_lock(&manifest, &lockfile, &fetcher, verbose, tui),
        Commands::Gefragt {
            manifest,
            lockfile,
            fetcher,
            no_lock_write,
        } => cmd_gefragt(&manifest, &lockfile, &fetcher, no_lock_write, verbose, tui),
        Commands::Nein {
            package,
            manifest,
            lock,
            lockfile,
            fetcher,
        } => cmd_nein(&package, &manifest, lock, &lockfile, &fetcher, verbose, tui),
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
    tui: bool,
) -> Result<(), String> {
    let manifest = read_manifest(manifest_path)?;
    validate_manifest(&manifest)?;
    let metrics = run_runner("lock", manifest_path, lockfile_path, fetcher_path, verbose, tui)?;
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
    tui: bool,
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
        cmd_lock(manifest_path, lockfile_path, fetcher_path, verbose, tui)?;
    }

    validate_lock_manifest_hash(lockfile_path, manifest_path)?;
    let metrics = run_runner("install", manifest_path, lockfile_path, fetcher_path, verbose, tui)?;
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
    tui: bool,
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
        cmd_lock(manifest_path, lockfile_path, fetcher_path, verbose, tui)?;
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
    tui: bool,
) -> Result<PhaseMetrics, String> {
    let mut cmd = build_runner_command(mode, manifest_path, lockfile_path, fetcher_path);
    if tui && std::io::stdout().is_terminal() {
        run_with_tui(&mut cmd, mode, verbose)
    } else {
        run_with_multibar(&mut cmd, verbose)
    }
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

fn run_with_tui(command: &mut Command, mode: &str, verbose: bool) -> Result<PhaseMetrics, String> {
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

    let mut state = TuiState::new(mode);
    let mut session = TuiSession::enter()?;
    session.draw(&state)?;

    loop {
        while let Ok((is_stderr, line)) = rx.try_recv() {
            if let Some(event) = parse_event(&line) {
                apply_event_tui(&event, &mut state);
            } else if !line.trim().is_empty() {
                let entry = if is_stderr {
                    format!("[stderr] {line}")
                } else {
                    line
                };
                state.push_log(entry);
            }
        }

        state.metrics.total_seconds = Some(state.started.elapsed().as_secs_f64());
        state.maybe_refresh_resources();
        session.draw(&state)?;

        if event::poll(TUI_TICK).map_err(|e| format!("failed to poll terminal input: {e}"))? {
            if let CEvent::Key(key) = event::read().map_err(|e| format!("failed to read terminal input: {e}"))?
            {
                if key.kind == KeyEventKind::Press
                    && handle_tui_key(&mut state, key.code, session.log_panel_rows()) == KeyAction::Quit
                {
                    let _ = child.kill();
                    let details = if state.logs.is_empty() {
                        "<no command output captured>".to_string()
                    } else {
                        state.logs.iter().cloned().collect::<Vec<_>>().join("\n")
                    };
                    return Err(format!("run cancelled by user\n{details}"));
                }
            }
        }

        match child.try_wait() {
            Ok(Some(status)) => {
                while let Ok((is_stderr, line)) = rx.try_recv() {
                    if parse_event(&line).is_none() && !line.trim().is_empty() {
                        let entry = if is_stderr {
                            format!("[stderr] {line}")
                        } else {
                            line
                        };
                        state.push_log(entry);
                    }
                }

                if status.success() {
                    state.resolve_pct = 100;
                    state.fetch_pct = 100;
                    state.install_pct = 100;
                    state.phase_message = "all phases complete".to_string();
                    state.completed = true;
                    state.metrics.total_seconds = Some(state.started.elapsed().as_secs_f64());
                    session.draw(&state)?;
                    wait_for_q_to_exit(&mut session, &mut state)?;
                    drop(session);
                    if verbose {
                        for line in state.logs {
                            println!("{line}");
                        }
                    }
                    return Ok(state.metrics);
                }

                drop(session);
                let details = if state.logs.is_empty() {
                    "<no command output captured>".to_string()
                } else {
                    state.logs.into_iter().collect::<Vec<_>>().join("\n")
                };
                return Err(format!("runner failed with status {status}\n{details}"));
            }
            Ok(None) => {}
            Err(e) => {
                drop(session);
                return Err(format!("failed while waiting for runner process: {e}"));
            }
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
        ("install", "target") => {
            if let Some(lib) = &event.lib {
                metrics.install_lib = Some(lib.clone());
            }
            if let Some(msg) = &event.message {
                install_pb.set_message(msg.clone());
            }
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
        ("install", "package_start") => {
            let pkg = event.package.as_deref().unwrap_or("package");
            let workers = event.threads.unwrap_or(1);
            let make_jobs = event.make_jobs.unwrap_or(1);
            install_pb.set_message(format!("starting {pkg} | workers {workers} | make -j{make_jobs}"));
        }
        ("install", "package_done") => {
            let pkg = event.package.as_deref().unwrap_or("package");
            install_pb.set_message(format!(
                "finished {pkg} in {:.2}s",
                event.seconds.unwrap_or(0.0)
            ));
        }
        ("install", "package_fail") => {
            let pkg = event.package.as_deref().unwrap_or("package");
            install_pb.set_message(format!("failed {pkg}"));
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

fn apply_event_tui(event: &RunnerEvent, state: &mut TuiState) {
    match (event.phase.as_str(), event.status.as_str()) {
        ("resolve", "start") => {
            state.active_phase = "resolve".to_string();
            state.resolve_pct = 15;
            state.phase_message = format!("resolving {} roots", event.total_roots.unwrap_or(0));
        }
        ("resolve", "done") => {
            state.resolve_pct = 100;
            state.phase_message = format!(
                "resolved {} packages in {:.2}s",
                event.packages.unwrap_or(0),
                event.seconds.unwrap_or(0.0)
            );
        }
        ("fetch", "start") => {
            state.active_phase = "fetch".to_string();
            state.fetch_pct = 14;
            state.download_threads = event.threads.unwrap_or(1);
            state.phase_message = format!("fetch started ({} threads)", event.threads.unwrap_or(0));
        }
        ("fetch", "done") => {
            state.fetch_pct = 100;
            state.active_phase = "install".to_string();
            let secs = event.seconds.unwrap_or(0.0);
            let dl = event.downloaded_bytes.unwrap_or(0);
            let reused = event.reused_bytes.unwrap_or(0);
            let hit = event.cache_hit_rate.unwrap_or(0.0) * 100.0;
            state.phase_message = format!(
                "fetched in {:.2}s | dl {} | reused {} | {:.1}% cache",
                secs,
                human_bytes(dl),
                human_bytes(reused),
                hit
            );
            state.metrics.fetch_seconds = Some(secs);
            state.metrics.downloaded_bytes = Some(dl);
            state.metrics.reused_bytes = Some(reused);
            state.metrics.cache_hit_rate = Some(event.cache_hit_rate.unwrap_or(0.0));
        }
        ("install", "start") => {
            state.active_phase = "install".to_string();
            state.install_threads = event.threads.unwrap_or(1);
            state.install_pct = 8;
            state.phase_message = format!(
                "install started ({} layers, {} threads)",
                event.layers.unwrap_or(0),
                state.install_threads
            );
        }
        ("install", "target") => {
            if let Some(lib) = &event.lib {
                state.metrics.install_lib = Some(lib.clone());
            }
            if let Some(msg) = &event.message {
                state.phase_message = msg.clone();
            }
        }
        ("install", "progress") => {
            let done = event.completed_packages.unwrap_or(0);
            let total = event.total_packages.unwrap_or(1).max(1);
            let pct = ((done as f64 / total as f64) * 100.0).round() as u16;
            state.install_pct = pct.min(99);
            state.phase_message = format!(
                "layer {}/{} | pkg {}/{}",
                event.layer.unwrap_or(0),
                event.layers.unwrap_or(0),
                done,
                total
            );
        }
        ("install", "package_start") => {
            if let Some(pkg) = &event.package {
                state.current_package = Some(pkg.clone());
                let entry = state.packages.entry(pkg.clone()).or_default();
                entry.installing = true;
                entry.compiling = false;
                entry.last_note = format!(
                    "workers={} make -j{}",
                    event.threads.unwrap_or(1),
                    event.make_jobs.unwrap_or(1)
                );
                state.phase_message = format!("starting {}", pkg);
            }
        }
        ("install", "package_done") => {
            if let Some(pkg) = &event.package {
                let entry = state.packages.entry(pkg.clone()).or_default();
                entry.done = true;
                entry.installing = false;
                entry.compiling = false;
                entry.last_note = format!("installed in {:.2}s", event.seconds.unwrap_or(0.0));
                state.phase_message = format!("finished {}", pkg);
            }
        }
        ("install", "package_fail") => {
            if let Some(pkg) = &event.package {
                let entry = state.packages.entry(pkg.clone()).or_default();
                entry.installing = false;
                entry.compiling = false;
                entry.last_note = "failed".to_string();
                state.phase_message = format!("failed {}", pkg);
            }
        }
        ("install", "done") => {
            state.install_pct = 100;
            state.metrics.install_seconds = event.seconds;
            for entry in state.packages.values_mut() {
                if entry.installing || entry.compiling {
                    entry.done = true;
                    entry.installing = false;
                    entry.last_note = "installed".to_string();
                }
            }
            state.phase_message = format!("install complete in {:.2}s", event.seconds.unwrap_or(0.0));
        }
        ("done", "done") => {
            state.active_phase = "done".to_string();
            state.metrics.total_seconds = event.total_seconds;
            state.phase_message = "all phases complete".to_string();
        }
        _ => {
            if let Some(msg) = &event.message {
                state.phase_message = msg.clone();
            }
        }
    }
}

fn handle_tui_key(state: &mut TuiState, code: KeyCode, log_panel_rows: usize) -> KeyAction {
    state.status_message = None;
    match state.input_mode {
        InputMode::PackageSearch => match code {
            KeyCode::Esc | KeyCode::Enter => {
                state.input_mode = InputMode::Normal;
                state.package_search_error = None;
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
            }
            KeyCode::Backspace => {
                state.package_search.pop();
                state.package_search_error = compile_regex_error(&state.package_search);
            }
            KeyCode::Char(c) => {
                state.package_search.push(c);
                state.package_search_error = compile_regex_error(&state.package_search);
            }
            _ => {}
        },
        InputMode::LogSearch => match code {
            KeyCode::Esc | KeyCode::Enter => {
                state.input_mode = InputMode::Normal;
                state.log_search_error = None;
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
            }
            KeyCode::Backspace => {
                state.log_search.pop();
                state.log_search_error = compile_regex_error(&state.log_search);
            }
            KeyCode::Char(c) => {
                state.log_search.push(c);
                state.log_search_error = compile_regex_error(&state.log_search);
            }
            _ => {}
        },
        InputMode::LogVisual => {
            let filtered_len = state.filtered_log_indices().len();
            match code {
                KeyCode::Esc | KeyCode::Char('v') => {
                    state.input_mode = InputMode::Normal;
                    state.log_anchor = None;
                    state.log_selection_active = false;
                    state.g_prefix_pending = false;
                    state.status_message = Some("left log mode".to_string());
                }
                KeyCode::Char('q') => return KeyAction::Quit,
                KeyCode::Char('V') => {
                    state.g_prefix_pending = false;
                    if state.log_selection_active {
                        state.log_selection_active = false;
                        state.log_anchor = None;
                        state.status_message = Some("selection off".to_string());
                    } else if filtered_len > 0 {
                        state.log_selection_active = true;
                        state.log_anchor = Some(state.log_cursor.min(filtered_len - 1));
                        state.status_message = Some("selection on".to_string());
                    }
                }
                KeyCode::Char('j') => {
                    state.g_prefix_pending = false;
                    if filtered_len > 0 {
                        state.log_cursor = (state.log_cursor + 1).min(filtered_len - 1);
                        state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                    }
                }
                KeyCode::Char('k') => {
                    state.g_prefix_pending = false;
                    state.log_cursor = state.log_cursor.saturating_sub(1);
                    state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                }
                KeyCode::Char('h') => {
                    state.g_prefix_pending = false;
                    state.log_cursor = state.log_cursor.saturating_sub(log_panel_rows.max(1));
                    state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                }
                KeyCode::Char('l') => {
                    state.g_prefix_pending = false;
                    if filtered_len > 0 {
                        state.log_cursor =
                            (state.log_cursor + log_panel_rows.max(1)).min(filtered_len - 1);
                        state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                    }
                }
                KeyCode::Char('G') => {
                    state.g_prefix_pending = false;
                    if filtered_len > 0 {
                        state.log_cursor = filtered_len - 1;
                        state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                    }
                }
                KeyCode::Char('g') => {
                    if state.g_prefix_pending {
                        state.g_prefix_pending = false;
                        state.log_cursor = 0;
                        state.align_log_view_for_cursor(log_panel_rows, filtered_len);
                    } else {
                        state.g_prefix_pending = true;
                    }
                }
                KeyCode::Char('y') => {
                    state.g_prefix_pending = false;
                    let copied = selected_log_text(state);
                    match copy_to_clipboard(&copied) {
                        Ok(_) => state.status_message = Some("copied logs to clipboard".to_string()),
                        Err(e) => state.status_message = Some(format!("clipboard copy failed: {e}")),
                    }
                }
                KeyCode::Char('E') => {
                    state.g_prefix_pending = false;
                    match export_logs_to_file(&state.logs) {
                        Ok(path) => state.status_message = Some(format!("logs exported to {}", path.display())),
                        Err(e) => state.status_message = Some(format!("failed to export logs: {e}")),
                    }
                }
                _ => {
                    state.g_prefix_pending = false;
                }
            }
        }
        InputMode::Normal => match code {
            KeyCode::Char('q') => return KeyAction::Quit,
            KeyCode::Esc => {
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
            }
            KeyCode::Char('f') => {
                state.f_prefix_pending = true;
                state.g_prefix_pending = false;
            }
            KeyCode::Char('p') if state.f_prefix_pending => {
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
                state.input_mode = InputMode::PackageSearch;
                state.package_search_error = compile_regex_error(&state.package_search);
            }
            KeyCode::Char('l') if state.f_prefix_pending => {
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
                state.input_mode = InputMode::LogSearch;
                state.log_search_error = compile_regex_error(&state.log_search);
            }
            KeyCode::Char('v') => {
                state.f_prefix_pending = false;
                state.enter_visual_mode(log_panel_rows);
            }
            KeyCode::Char('E') => {
                state.f_prefix_pending = false;
                match export_logs_to_file(&state.logs) {
                    Ok(path) => state.status_message = Some(format!("logs exported to {}", path.display())),
                    Err(e) => state.status_message = Some(format!("failed to export logs: {e}")),
                }
            }
            _ => {
                state.f_prefix_pending = false;
                state.g_prefix_pending = false;
            }
        },
    }
    KeyAction::None
}

fn compile_regex(query: &str) -> Option<Regex> {
    if query.trim().is_empty() {
        return None;
    }
    RegexBuilder::new(query).case_insensitive(true).build().ok()
}

fn compile_regex_error(query: &str) -> Option<String> {
    if query.trim().is_empty() {
        return None;
    }
    RegexBuilder::new(query)
        .case_insensitive(true)
        .build()
        .err()
        .map(|e| e.to_string())
}

fn extract_install_pkg(line: &str) -> Option<String> {
    let re = Regex::new(r#"(?i)installing .* package ['"`]?([A-Za-z][A-Za-z0-9._]+)"#).ok()?;
    re.captures(line)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn extract_pkg_from_tarball(line: &str) -> Option<String> {
    let re = Regex::new(r"([A-Za-z][A-Za-z0-9._]+)_[0-9][A-Za-z0-9._-]*\.tar\.gz").ok()?;
    re.captures(line)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

fn selected_log_text(state: &TuiState) -> String {
    let filtered = state.filtered_log_indices();
    if filtered.is_empty() {
        return String::new();
    }
    let cursor = state.log_cursor.min(filtered.len() - 1);
    let anchor = if state.log_selection_active {
        state.log_anchor.unwrap_or(cursor).min(filtered.len() - 1)
    } else {
        cursor
    };
    let (start, end) = if anchor <= cursor {
        (anchor, cursor)
    } else {
        (cursor, anchor)
    };
    let mut out = Vec::new();
    for idx in start..=end {
        let original = filtered[idx];
        if let Some(line) = state.logs.get(original) {
            out.push(line.clone());
        }
    }
    out.join("\n")
}

fn export_logs_to_file(logs: &VecDeque<String>) -> Result<PathBuf, String> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("time error: {e}"))?
        .as_secs();
    let path = PathBuf::from(format!("du_hast_r_logs_{stamp}.log"));
    let body = logs.iter().cloned().collect::<Vec<_>>().join("\n");
    fs::write(&path, body).map_err(|e| format!("write {}: {e}", path.display()))?;
    Ok(path)
}

fn copy_to_clipboard(text: &str) -> Result<(), String> {
    if text.is_empty() {
        return Err("no log selection to copy".to_string());
    }
    let mut attempts: Vec<Vec<&str>> = Vec::new();
    if std::env::var("WAYLAND_DISPLAY").is_ok() {
        attempts.push(vec!["wl-copy"]);
    }
    if std::env::var("DISPLAY").is_ok() {
        attempts.push(vec!["xclip", "-selection", "clipboard"]);
        attempts.push(vec!["xsel", "--clipboard", "--input"]);
    }
    attempts.push(vec!["pbcopy"]);

    let mut last_err = "no clipboard command available".to_string();
    for cmd in attempts {
        let mut child = match Command::new(cmd[0]).args(&cmd[1..]).stdin(Stdio::piped()).spawn() {
            Ok(c) => c,
            Err(e) => {
                last_err = format!("spawn {} failed: {e}", cmd[0]);
                continue;
            }
        };
        if let Some(stdin) = child.stdin.as_mut() {
            if let Err(e) = stdin.write_all(text.as_bytes()) {
                last_err = format!("write {} stdin failed: {e}", cmd[0]);
                let _ = child.kill();
                continue;
            }
        }
        match child.wait() {
            Ok(status) if status.success() => return Ok(()),
            Ok(status) => last_err = format!("{} exited with {}", cmd[0], status),
            Err(e) => last_err = format!("wait {} failed: {e}", cmd[0]),
        }
    }
    Err(last_err)
}

fn read_system_memory() -> (Option<u64>, Option<u64>) {
    let text = match fs::read_to_string("/proc/meminfo") {
        Ok(t) => t,
        Err(_) => return (None, None),
    };
    let total_kb = parse_meminfo_kb(&text, "MemTotal:");
    let avail_kb = parse_meminfo_kb(&text, "MemAvailable:");
    match (total_kb, avail_kb) {
        (Some(total), Some(avail)) => {
            let used = total.saturating_sub(avail);
            (Some(used * 1024), Some(total * 1024))
        }
        _ => (None, None),
    }
}

fn parse_meminfo_kb(content: &str, key: &str) -> Option<u64> {
    for line in content.lines() {
        if !line.starts_with(key) {
            continue;
        }
        let val = line
            .split_whitespace()
            .nth(1)
            .and_then(|n| n.parse::<u64>().ok());
        if val.is_some() {
            return val;
        }
    }
    None
}

fn read_process_rss() -> Option<u64> {
    let text = fs::read_to_string("/proc/self/status").ok()?;
    let kb = parse_meminfo_kb(&text, "VmRSS:")?;
    Some(kb * 1024)
}

fn draw_tui(frame: &mut ratatui::Frame<'_>, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = Paragraph::new(Line::from(vec![
        Span::styled("du_hast_r ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
        Span::raw("| "),
        Span::styled(
            format!("mode: {}", state.mode),
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ),
        Span::raw(" | "),
        Span::raw(state.phase_message.as_str()),
    ]))
    .block(Block::default().title("Status").borders(Borders::ALL));
    frame.render_widget(header, chunks[0]);

    let gauges = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Length(3)])
        .split(chunks[1]);
    frame.render_widget(
        gauge_widget("Resolve", state.resolve_pct, Color::Blue),
        gauges[0],
    );
    frame.render_widget(gauge_widget("Fetch", state.fetch_pct, Color::Cyan), gauges[1]);
    frame.render_widget(
        gauge_widget("Install", state.install_pct, Color::Magenta),
        gauges[2],
    );

    let middle = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(64), Constraint::Percentage(36)])
        .split(chunks[2]);

    let stats = vec![
        Line::from(format!(
            "total: {:>7}  fetch: {:>7}  install: {:>7}",
            format_secs(state.metrics.total_seconds),
            format_secs(state.metrics.fetch_seconds),
            format_secs(state.metrics.install_seconds)
        )),
        Line::from(format!(
            "downloaded: {:>10}  reused: {:>10}  cache hit: {:>6}",
            human_bytes(state.metrics.downloaded_bytes.unwrap_or(0)),
            human_bytes(state.metrics.reused_bytes.unwrap_or(0)),
            format_percent(state.metrics.cache_hit_rate)
        )),
        Line::from(format!(
            "library: {}",
            state
                .metrics
                .install_lib
                .clone()
                .unwrap_or_else(|| "--".to_string())
        )),
    ];
    let stats_panel =
        Paragraph::new(stats).block(Block::default().title("Metrics").borders(Borders::ALL));
    frame.render_widget(stats_panel, middle[0]);

    let active_threads = state.active_threads();
    let overall_util = ((active_threads as f64 / state.core_count.max(1) as f64) * 100.0)
        .clamp(0.0, 100.0);
    let core_lines = estimated_core_usage(state.core_count, active_threads)
        .into_iter()
        .enumerate()
        .take(6)
        .map(|(idx, pct)| Line::from(format!("core {:>2}: {:>3}%", idx + 1, pct)))
        .collect::<Vec<_>>();
    let mut thread_lines = vec![Line::from(format!(
        "active threads: {}  | cores: {}",
        active_threads, state.core_count
    ))];
    thread_lines.push(Line::from(format!("overall est utilization: {:>5.1}%", overall_util)));
    thread_lines.push(Line::from(format!(
        "ram used/total: {} / {}",
        state
            .mem_used_bytes
            .map(human_bytes)
            .unwrap_or_else(|| "--".to_string()),
        state
            .mem_total_bytes
            .map(human_bytes)
            .unwrap_or_else(|| "--".to_string())
    )));
    thread_lines.push(Line::from(format!(
        "du_hast_r rss: {}",
        state
            .proc_rss_bytes
            .map(human_bytes)
            .unwrap_or_else(|| "--".to_string())
    )));
    thread_lines.extend(core_lines);
    let thread_panel = Paragraph::new(thread_lines).block(
        Block::default()
            .title("Threads / Core Util (Estimated)")
            .borders(Borders::ALL),
    );
    frame.render_widget(thread_panel, middle[1]);

    let bottom = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[3]);

    let package_filter = compile_regex(&state.package_search);
    let package_items: Vec<ListItem<'_>> = state
        .packages
        .iter()
        .filter(|(name, _)| package_filter.as_ref().is_none_or(|re| re.is_match(name)))
        .take(bottom[0].height.saturating_sub(2) as usize)
        .map(|(name, pkg)| {
            ListItem::new(format!(
                "{:<24} dl:{} in:{} cc:{} ok:{} | {}",
                truncate_for_panel(name, 24),
                yes_no(pkg.downloaded),
                yes_no(pkg.installing),
                yes_no(pkg.compiling),
                yes_no(pkg.done),
                pkg.last_note
            ))
        })
        .collect();
    let package_title = if state.package_search.trim().is_empty() {
        "Packages".to_string()
    } else {
        format!("Packages (regex: {})", state.package_search)
    };
    let packages = List::new(package_items)
        .block(Block::default().title(package_title).borders(Borders::ALL));
    frame.render_widget(packages, bottom[0]);

    let log_filter = compile_regex(&state.log_search);
    let filtered_indices = state
        .logs
        .iter()
        .enumerate()
        .filter(|(_, line)| log_filter.as_ref().is_none_or(|re| re.is_match(line)))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let visible_logs = bottom[1].height.saturating_sub(2) as usize;
    let total_filtered = filtered_indices.len();
    let (start_idx, cursor_opt, anchor_opt) = if state.input_mode == InputMode::LogVisual && total_filtered > 0 {
        let cursor = state.log_cursor.min(total_filtered - 1);
        let page = visible_logs.max(1);
        let max_start = total_filtered.saturating_sub(page);
        let start = state.log_view_offset.min(max_start);
        let anchor = if state.log_selection_active {
            state.log_anchor
                .map(|a| a.min(total_filtered - 1))
                .or(Some(cursor))
        } else {
            None
        };
        (start, Some(cursor), anchor)
    } else {
        (total_filtered.saturating_sub(visible_logs), None, None)
    };
    let end_idx = (start_idx + visible_logs).min(total_filtered);
    let mut log_items: Vec<ListItem<'_>> = Vec::new();
    for i in start_idx..end_idx {
        let original = filtered_indices[i];
        let Some(line) = state.logs.get(original) else {
            continue;
        };
        let mut style = Style::default();
        if let (Some(cursor), Some(anchor)) = (cursor_opt, anchor_opt) {
            let (sel_start, sel_end) = if anchor <= cursor { (anchor, cursor) } else { (cursor, anchor) };
            if i >= sel_start && i <= sel_end {
                style = style.bg(Color::DarkGray).fg(Color::White);
            }
        }
        if let Some(cursor) = cursor_opt {
            if i == cursor {
                style = style
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD);
            }
        }
        log_items.push(ListItem::new(line.clone()).style(style));
    }
    let log_title = if state.log_search.trim().is_empty() {
        "Logs".to_string()
    } else {
        format!("Logs (regex: {})", state.log_search)
    };
    let logs = List::new(log_items).block(Block::default().title(log_title).borders(Borders::ALL));
    if let Some(cursor) = cursor_opt {
        let mut list_state = ListState::default();
        if cursor >= start_idx && cursor < end_idx {
            list_state.select(Some(cursor - start_idx));
        }
        frame.render_stateful_widget(logs, bottom[1], &mut list_state);
    } else {
        frame.render_widget(logs, bottom[1]);
    }

    let footer_text = match state.input_mode {
        InputMode::PackageSearch => format!(
            "Search Packages (regex): {}{}",
            state.package_search,
            state
                .package_search_error
                .as_ref()
                .map(|e| format!("  [invalid: {}]", truncate_for_panel(e, 36)))
                .unwrap_or_default()
        ),
        InputMode::LogSearch => format!(
            "Search Logs (regex): {}{}",
            state.log_search,
            state
                .log_search_error
                .as_ref()
                .map(|e| format!("  [invalid: {}]", truncate_for_panel(e, 36)))
                .unwrap_or_default()
        ),
        InputMode::Normal => {
            if state.f_prefix_pending {
                "command mode: f + p (package search), f + l (log search), q (quit)".to_string()
            } else if state.completed {
                "completed at 100% | q quit | v visual logs | f+p/f+l search | E export logs".to_string()
            } else {
                "q abort | v visual logs | f then p/l search | E export logs".to_string()
            }
        }
        InputMode::LogVisual => {
            if state.log_selection_active {
                "LOG SELECT: j/k/h/l move, gg top, G end, V toggle off, y copy, E export, v/esc exit"
                    .to_string()
            } else {
                "LOG NAV: j/k/h/l move, gg top, G end, V start selection, y copy line, E export, v/esc exit"
                    .to_string()
            }
        }
    };
    let footer_text = if let Some(status) = &state.status_message {
        format!("{footer_text} | {status}")
    } else {
        footer_text
    };
    let footer = Paragraph::new(footer_text)
        .style(Style::default().fg(Color::DarkGray))
        .block(Block::default().title("Command").borders(Borders::ALL));
    frame.render_widget(footer, chunks[4]);
}

fn wait_for_q_to_exit(session: &mut TuiSession, state: &mut TuiState) -> Result<(), String> {
    loop {
        state.maybe_refresh_resources();
        session.draw(state)?;
        if event::poll(TUI_TICK).map_err(|e| format!("failed to poll terminal input: {e}"))? {
            if let CEvent::Key(key) =
                event::read().map_err(|e| format!("failed to read terminal input: {e}"))?
            {
                if key.kind == KeyEventKind::Press
                    && handle_tui_key(state, key.code, session.log_panel_rows()) == KeyAction::Quit
                {
                    return Ok(());
                }
            }
        }
    }
}

fn gauge_widget<'a>(label: &'a str, pct: u16, color: Color) -> Gauge<'a> {
    Gauge::default()
        .block(Block::default().title(label).borders(Borders::ALL))
        .gauge_style(
            Style::default()
                .fg(color)
                .bg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .percent(pct.min(100))
        .label(format!("{:>3}%", pct.min(100)))
}

fn format_secs(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.2}s"))
        .unwrap_or_else(|| "--".to_string())
}

fn format_percent(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.1}%", v * 100.0))
        .unwrap_or_else(|| "--".to_string())
}

fn estimated_core_usage(core_count: usize, threads: u64) -> Vec<u16> {
    let mut remaining = threads as f64;
    let mut usage = Vec::with_capacity(core_count);
    for _ in 0..core_count {
        if remaining >= 1.0 {
            usage.push(100);
            remaining -= 1.0;
        } else if remaining > 0.0 {
            usage.push((remaining * 100.0).round() as u16);
            remaining = 0.0;
        } else {
            usage.push(0);
        }
    }
    usage
}

fn yes_no(flag: bool) -> &'static str {
    if flag { "Y" } else { "N" }
}

fn truncate_for_panel(text: &str, max: usize) -> String {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= max {
        return text.to_string();
    }
    let keep = max.saturating_sub(3);
    chars.into_iter().take(keep).collect::<String>() + "..."
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
    if let Some(lib) = metrics.install_lib {
        println!("LIBRARY {}", lib);
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
