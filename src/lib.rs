use md5::Md5;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Duration, sleep};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    pub cache_dir: PathBuf,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub dynamic: Option<DynamicConcurrencyConfig>,
    pub packages: Vec<PackageRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicConcurrencyConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_dynamic_mode")]
    pub mode: DynamicMode,
    #[serde(default)]
    pub min_concurrency: Option<usize>,
    #[serde(default)]
    pub max_concurrency: Option<usize>,
    #[serde(default = "default_rebalance_interval_ms")]
    pub rebalance_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DynamicMode {
    SharedServer,
    DedicatedBuilder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageRequest {
    pub package: String,
    #[serde(default)]
    pub version: Option<String>,
    pub urls: Vec<String>,
    pub checksum: Checksum,
    #[serde(default)]
    pub artifact_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checksum {
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    pub cache_dir: PathBuf,
    pub results: Vec<PackageResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageResult {
    pub package: String,
    #[serde(default)]
    pub version: Option<String>,
    pub status: FetchStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FetchStatus {
    Success {
        path: PathBuf,
        source_url: String,
        cached: bool,
        bytes: u64,
        checksum: String,
    },
    Error {
        code: String,
        message: String,
        attempts: Vec<FetchAttempt>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchAttempt {
    pub url: String,
    pub outcome: String,
}

#[derive(Debug, Clone)]
pub struct Fetcher {
    client: reqwest::Client,
}

impl Default for Fetcher {
    fn default() -> Self {
        let client = reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()
            .expect("client should build");
        Self { client }
    }
}

impl Fetcher {
    pub async fn fetch_all(&self, request: FetchRequest) -> FetchResponse {
        let cache_dir = request.cache_dir.clone();
        let initial_concurrency = request.concurrency.max(1);
        let dynamic = request.dynamic.filter(|cfg| cfg.enabled);
        let results = self
            .fetch_all_with_pool(cache_dir.clone(), request.packages, initial_concurrency, dynamic)
            .await;

        FetchResponse { cache_dir, results }
    }

    async fn fetch_all_with_pool(
        &self,
        cache_dir: PathBuf,
        packages: Vec<PackageRequest>,
        initial_concurrency: usize,
        dynamic: Option<DynamicConcurrencyConfig>,
    ) -> Vec<PackageResult> {
        if packages.is_empty() {
            return Vec::new();
        }

        let total = packages.len();
        let caps = WorkerCaps::from_request(initial_concurrency, total, dynamic.clone());
        let target_concurrency = Arc::new(AtomicUsize::new(caps.initial));
        let completed = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let queue = Arc::new(Mutex::new(
            packages
                .into_iter()
                .enumerate()
                .collect::<VecDeque<(usize, PackageRequest)>>(),
        ));
        let results = Arc::new(Mutex::new(vec![None; total]));

        let controller = dynamic.clone().map(|cfg| {
            tokio::spawn(run_rebalance_controller(
                cfg,
                Arc::clone(&queue),
                Arc::clone(&target_concurrency),
                Arc::clone(&completed),
                Arc::clone(&notify),
                caps,
                total,
            ))
        });

        let mut workers = Vec::with_capacity(caps.max);
        for worker_id in 0..caps.max {
            workers.push(tokio::spawn(run_fetch_worker(
                self.clone(),
                cache_dir.clone(),
                worker_id,
                Arc::clone(&queue),
                Arc::clone(&results),
                Arc::clone(&target_concurrency),
                Arc::clone(&completed),
                Arc::clone(&notify),
                total,
            )));
        }

        for worker in workers {
            worker.await.expect("fetch worker panicked");
        }

        if let Some(controller) = controller {
            let _ = controller.await;
        }

        let mut locked = results.lock().await;
        locked
            .drain(..)
            .map(|entry| entry.expect("fetch result should be populated"))
            .collect()
    }

    async fn fetch_package(&self, cache_dir: &Path, package: PackageRequest) -> PackageResult {
        let mut attempts = Vec::new();

        match validate_checksum(&package.checksum) {
            Ok(_) => {}
            Err(message) => {
                return PackageResult {
                    package: package.package,
                    version: package.version,
                    status: FetchStatus::Error {
                        code: "invalid_checksum".to_string(),
                        message,
                        attempts,
                    },
                };
            }
        }

        for url in &package.urls {
            match self.fetch_from_url(cache_dir, &package, url).await {
                Ok(hit) => {
                    return PackageResult {
                        package: package.package,
                        version: package.version,
                        status: FetchStatus::Success {
                            path: hit.path,
                            source_url: url.clone(),
                            cached: hit.cached,
                            bytes: hit.bytes,
                            checksum: package.checksum.value.clone(),
                        },
                    };
                }
                Err(err) => attempts.push(FetchAttempt {
                    url: url.clone(),
                    outcome: err,
                }),
            }
        }

        PackageResult {
            package: package.package,
            version: package.version,
            status: FetchStatus::Error {
                code: "all_urls_failed".to_string(),
                message: "every candidate URL failed".to_string(),
                attempts,
            },
        }
    }

    async fn fetch_from_url(
        &self,
        cache_dir: &Path,
        package: &PackageRequest,
        url: &str,
    ) -> Result<CacheHit, String> {
        let artifact_path = cached_artifact_path(
            cache_dir,
            url,
            &package.checksum,
            package.artifact_name.as_deref(),
        );
        if let Ok(bytes) = verify_cached_artifact(&artifact_path, &package.checksum).await {
            return Ok(CacheHit {
                path: artifact_path,
                cached: true,
                bytes,
            });
        }

        tokio::fs::create_dir_all(cache_dir)
            .await
            .map_err(|err| format!("create cache dir: {err}"))?;

        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|err| format!("request failed: {err}"))?;

        if response.status() != StatusCode::OK {
            return Err(format!("unexpected status {}", response.status()));
        }

        let body = response
            .bytes()
            .await
            .map_err(|err| format!("read body failed: {err}"))?;

        verify_bytes(&body, &package.checksum)
            .map_err(|err| format!("checksum mismatch: {err}"))?;

        let temp_path = artifact_path.with_extension("part");
        let mut file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|err| format!("create temp file: {err}"))?;
        file.write_all(&body)
            .await
            .map_err(|err| format!("write temp file: {err}"))?;
        file.flush()
            .await
            .map_err(|err| format!("flush temp file: {err}"))?;
        drop(file);

        tokio::fs::rename(&temp_path, &artifact_path)
            .await
            .map_err(|err| format!("persist artifact: {err}"))?;

        Ok(CacheHit {
            path: artifact_path,
            cached: false,
            bytes: body.len() as u64,
        })
    }
}

#[derive(Debug, Clone)]
struct CacheHit {
    path: PathBuf,
    cached: bool,
    bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct WorkerCaps {
    min: usize,
    max: usize,
    initial: usize,
}

impl WorkerCaps {
    fn from_request(
        initial_concurrency: usize,
        total_packages: usize,
        dynamic: Option<DynamicConcurrencyConfig>,
    ) -> Self {
        let fallback_max = default_dynamic_max_concurrency(total_packages);
        match dynamic {
            Some(cfg) => {
                let max = cfg
                    .max_concurrency
                    .unwrap_or(fallback_max)
                    .max(1)
                    .min(total_packages.max(1));
                let min = cfg
                    .min_concurrency
                    .unwrap_or(1)
                    .max(1)
                    .min(max);
                let initial = initial_concurrency.max(min).min(max);
                Self { min, max, initial }
            }
            None => {
                let fixed = initial_concurrency.max(1).min(total_packages.max(1));
                Self {
                    min: fixed,
                    max: fixed,
                    initial: fixed,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HostSnapshot {
    logical_cpus: usize,
    load_avg_1: Option<f64>,
    mem_total_bytes: Option<u64>,
    mem_available_bytes: Option<u64>,
}

async fn run_fetch_worker(
    fetcher: Fetcher,
    cache_dir: PathBuf,
    worker_id: usize,
    queue: Arc<Mutex<VecDeque<(usize, PackageRequest)>>>,
    results: Arc<Mutex<Vec<Option<PackageResult>>>>,
    target_concurrency: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    total: usize,
) {
    loop {
        if completed.load(Ordering::Relaxed) >= total {
            return;
        }

        let target = target_concurrency.load(Ordering::Relaxed).max(1);
        if worker_id >= target {
            tokio::select! {
                _ = notify.notified() => {}
                _ = sleep(Duration::from_millis(200)) => {}
            }
            continue;
        }

        let next = {
            let mut locked = queue.lock().await;
            locked.pop_front()
        };

        let Some((index, package)) = next else {
            return;
        };

        let result = fetcher.fetch_package(&cache_dir, package).await;
        {
            let mut locked = results.lock().await;
            locked[index] = Some(result);
        }
        completed.fetch_add(1, Ordering::Relaxed);
        notify.notify_waiters();
    }
}

async fn run_rebalance_controller(
    config: DynamicConcurrencyConfig,
    queue: Arc<Mutex<VecDeque<(usize, PackageRequest)>>>,
    target_concurrency: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    notify: Arc<Notify>,
    caps: WorkerCaps,
    total: usize,
) {
    let interval = Duration::from_millis(config.rebalance_interval_ms.max(250));
    loop {
        if completed.load(Ordering::Relaxed) >= total {
            return;
        }

        let pending = {
            let locked = queue.lock().await;
            locked.len()
        };
        if pending == 0 {
            return;
        }

        let snapshot = read_host_snapshot();
        let suggested = suggest_dynamic_concurrency(snapshot, config.mode, caps, pending);
        let current = target_concurrency.load(Ordering::Relaxed);
        if suggested != current {
            target_concurrency.store(suggested, Ordering::Relaxed);
            notify.notify_waiters();
        }

        sleep(interval).await;
    }
}

fn read_host_snapshot() -> HostSnapshot {
    let logical_cpus = std::thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(1);
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    let loadavg = std::fs::read_to_string("/proc/loadavg").ok();
    HostSnapshot {
        logical_cpus,
        load_avg_1: loadavg
            .as_deref()
            .and_then(|text| text.split_whitespace().next())
            .and_then(|value| value.parse::<f64>().ok()),
        mem_total_bytes: meminfo
            .as_deref()
            .and_then(|text| parse_meminfo_bytes(text, "MemTotal:")),
        mem_available_bytes: meminfo
            .as_deref()
            .and_then(|text| parse_meminfo_bytes(text, "MemAvailable:")),
    }
}

fn suggest_dynamic_concurrency(
    snapshot: HostSnapshot,
    mode: DynamicMode,
    caps: WorkerCaps,
    pending: usize,
) -> usize {
    let cpu_budget = match (mode, snapshot.load_avg_1) {
        (DynamicMode::SharedServer, Some(load)) => {
            ((snapshot.logical_cpus as f64 * 0.70) - load).floor() as isize
        }
        (DynamicMode::DedicatedBuilder, Some(load)) => {
            ((snapshot.logical_cpus as f64 * 1.05) - load).floor() as isize
        }
        (DynamicMode::SharedServer, None) => (snapshot.logical_cpus as f64 * 0.60).floor() as isize,
        (DynamicMode::DedicatedBuilder, None) => {
            (snapshot.logical_cpus as f64 * 0.90).floor() as isize
        }
    };

    let mut target = cpu_budget.max(1) as usize;
    if let (Some(total_mem), Some(available_mem)) =
        (snapshot.mem_total_bytes, snapshot.mem_available_bytes)
    {
        let available_ratio = available_mem as f64 / total_mem.max(1) as f64;
        let mem_scale = match mode {
            DynamicMode::SharedServer if available_ratio < 0.10 => 0.25,
            DynamicMode::SharedServer if available_ratio < 0.20 => 0.50,
            DynamicMode::SharedServer if available_ratio < 0.35 => 0.75,
            DynamicMode::DedicatedBuilder if available_ratio < 0.08 => 0.25,
            DynamicMode::DedicatedBuilder if available_ratio < 0.15 => 0.50,
            DynamicMode::DedicatedBuilder if available_ratio < 0.25 => 0.75,
            _ => 1.0,
        };
        target = ((target as f64) * mem_scale).round().max(1.0) as usize;
    }

    target.max(caps.min).min(caps.max).min(pending.max(1))
}

fn parse_meminfo_bytes(content: &str, key: &str) -> Option<u64> {
    content
        .lines()
        .find(|line| line.starts_with(key))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

pub fn cached_artifact_path(
    cache_dir: &Path,
    url: &str,
    checksum: &Checksum,
    artifact_name: Option<&str>,
) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    hasher.update([0]);
    hasher.update(checksum.algorithm.as_bytes());
    hasher.update([0]);
    hasher.update(checksum.value.as_bytes());
    let key = hex::encode(hasher.finalize());

    let suffix = artifact_name
        .map(sanitize_artifact_name)
        .unwrap_or_else(|| infer_artifact_name(url));
    cache_dir.join(format!("{key}-{suffix}"))
}

fn infer_artifact_name(url: &str) -> String {
    url.rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(sanitize_artifact_name)
        .unwrap_or_else(|| "artifact.bin".to_string())
}

fn sanitize_artifact_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '_' | '-' => ch,
            _ => '_',
        })
        .collect();
    if sanitized.is_empty() {
        "artifact.bin".to_string()
    } else {
        sanitized
    }
}

async fn verify_cached_artifact(path: &Path, checksum: &Checksum) -> Result<u64, String> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|err| format!("read cached artifact: {err}"))?;
    verify_bytes(&bytes, checksum)?;
    Ok(bytes.len() as u64)
}

fn verify_bytes(bytes: &[u8], checksum: &Checksum) -> Result<(), String> {
    match normalized_algorithm(&checksum.algorithm)? {
        ChecksumAlgorithm::Sha256 => {
            let actual = hex::encode(Sha256::digest(bytes));
            if actual.eq_ignore_ascii_case(&checksum.value) {
                Ok(())
            } else {
                Err(format!("expected {}, got {}", checksum.value, actual))
            }
        }
        ChecksumAlgorithm::Md5 => {
            let actual = hex::encode(Md5::digest(bytes));
            if actual.eq_ignore_ascii_case(&checksum.value) {
                Ok(())
            } else {
                Err(format!("expected {}, got {}", checksum.value, actual))
            }
        }
    }
}

fn validate_checksum(checksum: &Checksum) -> Result<(), String> {
    match normalized_algorithm(&checksum.algorithm)? {
        ChecksumAlgorithm::Sha256 => {
            if checksum.value.len() != 64
                || !checksum.value.chars().all(|ch| ch.is_ascii_hexdigit())
            {
                return Err("sha256 checksum must be 64 hex characters".to_string());
            }
        }
        ChecksumAlgorithm::Md5 => {
            if checksum.value.len() != 32
                || !checksum.value.chars().all(|ch| ch.is_ascii_hexdigit())
            {
                return Err("md5 checksum must be 32 hex characters".to_string());
            }
        }
    }

    Ok(())
}

fn normalized_algorithm(algorithm: &str) -> Result<ChecksumAlgorithm, String> {
    if algorithm.eq_ignore_ascii_case("sha256") {
        Ok(ChecksumAlgorithm::Sha256)
    } else if algorithm.eq_ignore_ascii_case("md5") {
        Ok(ChecksumAlgorithm::Md5)
    } else {
        Err(format!("unsupported algorithm {algorithm}"))
    }
}

enum ChecksumAlgorithm {
    Sha256,
    Md5,
}

fn default_concurrency() -> usize {
    8
}

fn default_dynamic_mode() -> DynamicMode {
    DynamicMode::SharedServer
}

fn default_rebalance_interval_ms() -> u64 {
    1_500
}

fn default_dynamic_max_concurrency(total_packages: usize) -> usize {
    let cpu_bound = std::thread::available_parallelism()
        .map(|value| value.get().saturating_mul(2))
        .unwrap_or(8)
        .max(2);
    cpu_bound.min(total_packages.max(1))
}

fn default_algorithm() -> String {
    "sha256".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use tempfile::tempdir;

    #[tokio::test]
    async fn returns_cached_artifact_without_network() {
        let body = b"source tarball bytes".to_vec();
        let checksum = hex::encode(Sha256::digest(&body));
        let cache_dir = tempdir().expect("tempdir");
        let url = "https://mirror.example.org/src/contrib/pkgA_1.0.0.tar.gz";
        let artifact_path = cached_artifact_path(
            cache_dir.path(),
            url,
            &Checksum {
                algorithm: "sha256".to_string(),
                value: checksum.clone(),
            },
            Some("pkgA_1.0.0.tar.gz"),
        );
        tokio::fs::write(&artifact_path, &body)
            .await
            .expect("seed cache");

        let request = FetchRequest {
            cache_dir: cache_dir.path().to_path_buf(),
            concurrency: 4,
            dynamic: None,
            packages: vec![PackageRequest {
                package: "pkgA".to_string(),
                version: Some("1.0.0".to_string()),
                urls: vec![url.to_string()],
                checksum: Checksum {
                    algorithm: "sha256".to_string(),
                    value: checksum.clone(),
                },
                artifact_name: Some("pkgA_1.0.0.tar.gz".to_string()),
            }],
        };

        let fetcher = Fetcher::default();
        let response = fetcher.fetch_all(request).await;

        assert!(matches!(
            response.results[0].status,
            FetchStatus::Success { cached: true, .. }
        ));
    }

    #[tokio::test]
    async fn rejects_invalid_checksum_before_fetch() {
        let cache_dir = tempdir().expect("tempdir");

        let request = FetchRequest {
            cache_dir: cache_dir.path().to_path_buf(),
            concurrency: 2,
            dynamic: None,
            packages: vec![PackageRequest {
                package: "pkgB".to_string(),
                version: None,
                urls: vec!["https://mirror.example.org/src/contrib/pkgB_1.0.0.tar.gz".to_string()],
                checksum: Checksum {
                    algorithm: "sha256".to_string(),
                    value: "not-a-real-checksum".to_string(),
                },
                artifact_name: None,
            }],
        };

        let response = Fetcher::default().fetch_all(request).await;
        assert!(matches!(
            response.results[0].status,
            FetchStatus::Error { .. }
        ));
    }

    #[tokio::test]
    async fn accepts_md5_checksums() {
        let body = b"source tarball bytes".to_vec();
        let checksum = hex::encode(Md5::digest(&body));
        let cache_dir = tempdir().expect("tempdir");
        let url = "https://mirror.example.org/src/contrib/pkgC_1.0.0.tar.gz";
        let artifact_path = cached_artifact_path(
            cache_dir.path(),
            url,
            &Checksum {
                algorithm: "md5".to_string(),
                value: checksum.clone(),
            },
            Some("pkgC_1.0.0.tar.gz"),
        );
        tokio::fs::write(&artifact_path, &body)
            .await
            .expect("seed cache");

        let response = Fetcher::default()
            .fetch_all(FetchRequest {
                cache_dir: cache_dir.path().to_path_buf(),
                concurrency: 2,
                dynamic: None,
                packages: vec![PackageRequest {
                    package: "pkgC".to_string(),
                    version: Some("1.0.0".to_string()),
                    urls: vec![url.to_string()],
                    checksum: Checksum {
                        algorithm: "md5".to_string(),
                        value: checksum,
                    },
                    artifact_name: Some("pkgC_1.0.0.tar.gz".to_string()),
                }],
            })
            .await;

        assert!(matches!(
            response.results[0].status,
            FetchStatus::Success { cached: true, .. }
        ));
    }
}
