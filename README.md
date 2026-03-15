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
    "download_threads": 16,
    "install_ncpus": 4,
    "make_jobs": 4
  },
  "dependencies": {
    "BiocGenerics": "0.56.0",
    "scater": "1.38.0",
    "scran": "1.38.1"
  }
}
```

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

Summarize results:

```bash
Rscript scripts/summarize_benchmark_results.R benchmark_runs/<run_id>/benchmark_results.csv
```

Notes:

- The benchmark runs cold and warm scenarios for each repetition.
- Disk safety guard is controlled by `disk_guard.min_free_gb` in config.
- Cleanup is sequential and enabled by default to reduce SSD pressure.
- `renv` baseline requires the `renv` package to be installed.
- Benchmark `renv` flows force `DOWNLOAD_STATIC_LIBV8=1` to avoid host-specific libv8 toolchain failures.

## Integration testing

The CLI contract is covered by [tests/cli_cached_success.rs](/home/achref/Document/async_dependency_installer_for_R/tests/cli_cached_success.rs), which seeds a valid cached artifact, invokes the compiled binary, and verifies the structured JSON response.

## Notes

- checksum support includes `sha256` and `md5`
- cached artifacts are revalidated before reuse
- the Rust layer remains transport-focused; dependency resolution and install scheduling stay in R
