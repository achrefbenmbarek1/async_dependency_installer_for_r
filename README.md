# async_dependency_installer_for_R

Rust fetch/cache layer for an R dependency installer that separates:

- dependency graph planning in R
- artifact download and cache reuse in Rust
- install ordering and optional parallel install execution back in R

## What the Rust side does

- downloads many package artifacts concurrently
- caches artifacts by `URL + checksum`
- verifies `sha256` or `md5` before persisting files
- returns structured success or error data per package
- supports multiple candidate URLs per package for mirror fallback

## CLI contract

Pass a JSON request on `stdin` or as the first positional argument. The response is emitted as JSON on `stdout`, or written to `--output <path>`.

```json
{
  "cache_dir": "/tmp/r-artifact-cache",
  "concurrency": 8,
  "dynamic": {
    "enabled": true,
    "mode": "shared_server",
    "min_concurrency": 1,
    "max_concurrency": 12,
    "rebalance_interval_ms": 1500
  },
  "packages": [
    {
      "package": "BiocGenerics",
      "version": "0.50.0",
      "urls": [
        "https://bioconductor.org/packages/3.21/bioc/src/contrib/BiocGenerics_0.50.0.tar.gz"
      ],
      "checksum": {
        "algorithm": "md5",
        "value": "REPLACE_WITH_MD5_FROM_PACKAGES_METADATA"
      },
      "artifact_name": "BiocGenerics_0.50.0.tar.gz"
    }
  ]
}
```

Example:

```bash
cargo run -- request.json
```

or

```bash
cat request.json | cargo run --
```

## du_hast_r CLI

This repository now includes a modern package-manager CLI:

- binary: `du_hast_r`
- manifest: `fer.json`
- lockfile: `nein.lock`

Create a new manifest:

```bash
du_hast_r init
```

Generate lockfile:

```bash
du_hast_r lock
```

Install from lockfile (`gefragt` is the install verb):

```bash
du_hast_r gefragt fer.json
```

Use full-screen TUI mode:

```bash
du_hast_r --tui gefragt fer.json
```

In TUI mode, press `q` to abort while running, and after success the 100% screen stays open until you press `q` to exit.
Command-mode shortcuts:
- `f` then `p`: package panel regex search (by package name)
- `f` then `l`: log panel regex search
- `v`: enter/leave log navigation mode (`j/k/h/l`, `gg`, `G`)
- `V` in log mode: toggle selection mode on/off from current line
- `y` in log mode: copy current line (or selected range if selection is on)
- `E`: export captured logs to `du_hast_r_logs_<unix_ts>.log`
- `Esc` or `Enter`: leave search mode

Show full compiler/install logs when needed:

```bash
du_hast_r --verbose gefragt fer.json
```

Remove dependency from manifest (`nein` is the delete verb):

```bash
du_hast_r nein Seurat fer.json --lock
```

Import existing project metadata:

```bash
du_hast_r import --from renv.lock fer.json
du_hast_r import --from DESCRIPTION fer.json
```

Generate shell completion scripts:

```bash
du_hast_r completions zsh > _du_hast_r
du_hast_r completions bash > du_hast_r.bash
```

Example `fer.json`:

```json
{
  "name": "my-neuro-project",
  "version": "0.1.0",
  "settings": {
    "dynamics": true,
    "dynamic_mode": "shared_server",
    "download_threads": 16,
    "install_ncpus": 4,
    "make_jobs": 4,
    "lib": "/path/to/your/R/library"
  },
  "dependencies": {
    "BiocGenerics": "0.56.0",
    "scater": "1.38.0",
    "scran": "1.38.1"
  }
}
```

When `settings.dynamics` is `false`, the installer uses the fixed `download_threads`, `install_ncpus`, and `make_jobs` values.
When `settings.dynamics` is `true`, those fixed values are ignored at runtime and replaced by:

- live-rebalanced download concurrency in the Rust fetcher
- batch-level install scheduling in the R runner
- memory- and swap-aware install throttling that can reduce `Ncpus` and `MAKEFLAGS` under pressure

`settings.dynamic_mode` controls the heuristic bias:

- `shared_server`: leaves more CPU and memory headroom
- `dedicated_builder`: pushes harder for throughput on a mostly dedicated machine

Install location precedence for `du_hast_r gefragt`:
- `settings.lib` in `fer.json` (if set and writable)
- then current `.libPaths()`
- then `R_LIBS_USER`

## R orchestration

The repository now includes an R shim in [R/async_install.R](/home/achref/Document/async_dependency_installer_for_R/R/async_install.R) that:

1. R computes the dependency graph and topological layers.
2. R prepares a fetch request with package names, candidate URLs, and checksums.
3. Rust downloads everything up front and returns local artifact paths.
4. R installs artifacts in dependency-safe order, optionally parallelizing only packages in the same independent layer.

Minimal example:

```r
source("R/async_install.R")

async_install_packages(
  packages = "BiocGenerics",
  fetcher = "./target/debug/async_dependency_installer_for_R",
  download_concurrency = 16L,
  install_ncpus = 2L,
  make_jobs = 2L
)
```

If `BiocManager` is installed, Bioconductor repositories are added automatically; otherwise the shim still works with standard CRAN repositories.

For a dry run that resolves dependencies and downloads artifacts without installing:

```r
source("R/async_install.R")
async_install_packages("BiocGenerics", dry_run = TRUE)
```

The helper script [scripts/demo_async_install.R](/home/achref/Document/async_dependency_installer_for_R/scripts/demo_async_install.R) wraps this for command-line use:

```bash
Rscript scripts/demo_async_install.R BiocGenerics
```

## Benchmark harness

This repository includes a benchmark runner for measuring async installer gains versus non-async baselines on heavy neurobiology-oriented stacks.

Configured methods:

- `async` (this project path, non-`turbo`)
- `serial` baseline (`install.packages` one-by-one)
- `tuned` baseline (`install.packages` with `Ncpus` + `MAKEFLAGS`)
- `renv` baseline (`renv::restore`)

Run a smoke benchmark first:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_smoke.json
```

Run the full benchmark:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config.json
```

Run the trimmed CLI dynamic benchmark against the real `du_hast_r gefragt` path:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_cli_dynamic.json
```

Run the heavy-only CLI dynamic benchmark with a safer tuned baseline for laptop-class machines:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_cli_dynamic_heavy.json
```

Run a realistic single-cell / Bioconductor CLI dynamic benchmark:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_cli_dynamic_singlecell.json
```

Run a tighter single-cell comparison focused on `tuned` versus `du_hast_dynamic_dedicated`:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_cli_dynamic_singlecell_dedicated.json
```

Run a 3-repetition confirmation benchmark for that same dedicated single-cell comparison:

```bash
Rscript scripts/benchmark_async_vs_baselines.R scripts/benchmark_config_cli_dynamic_singlecell_dedicated_confirm.json
```

On the `singlecell_realistic` cold benchmark (`Seurat`, `SingleCellExperiment`, `scater`, `scran`, `DropletUtils`, `BiocParallel`), `du_hast_dynamic_dedicated` consistently outperformed the tuned baseline across 3 repetitions. Median total time dropped from `1704.8s` to `1017.8s`, a `40.3%` improvement (`1.67x` as fast). The win held across both phases: mean download time fell from about `73.6s` to `38.6s` (`47.6%` faster), and mean install time fell from about `1629.5s` to `980.7s` (`39.8%` faster).

Summarize results:

```bash
Rscript scripts/summarize_benchmark_results.R benchmark_runs/<run_id>/benchmark_results.csv
```

Notes:

- The benchmark runs cold and warm scenarios for each repetition.
- Disk safety guard is controlled by `disk_guard.min_free_gb` in config.
- Cleanup is sequential and enabled by default to reduce SSD pressure.
- Results are checkpointed after each completed scenario, so partial runs still produce CSV/JSON output.
- The heavy-only CLI dynamic config lowers the `tuned` baseline to `install_ncpus = 1` and `make_jobs = 2` to reduce OOM risk while leaving async modes adaptive.
- Dynamic install scheduling now reacts to both RAM availability and swap usage, and may clamp `MAKEFLAGS` more aggressively than `Ncpus` on native-code-heavy batches.
- Dynamic installs are chunked more finely than whole dependency layers, so `shared` and `dedicated` can raise or lower install parallelism more often between batches.
- The dedicated mode now uses larger healthy-host install chunks and a higher healthy-host `MAKEFLAGS` cap than shared, while preserving the same low-memory backoff.
- The single-cell CLI dynamic config models a more day-to-day computational neurobiology workflow than the Stan-heavy stress stack.
- `renv` baseline requires the `renv` package to be installed.
- Benchmark `renv` flows force `DOWNLOAD_STATIC_LIBV8=1` to avoid host-specific libv8 toolchain failures.

## Integration testing

The CLI contract is covered by [tests/cli_cached_success.rs](/home/achref/Document/async_dependency_installer_for_R/tests/cli_cached_success.rs), which seeds a valid cached artifact, invokes the compiled binary, and verifies the structured JSON response.

## Notes

- checksum support includes `sha256` and `md5`
- cached artifacts are revalidated before reuse
- the Rust layer remains transport-focused; dependency resolution and install scheduling stay in R
