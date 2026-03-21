suppressPackageStartupMessages({
  library(jsonlite)
})

source("R/async_install.R")

`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

emit_event <- function(phase, status, ...) {
  payload <- list(phase = phase, status = status, ...)
  cat(sprintf("DHR_EVENT %s\n", toJSON(payload, auto_unbox = TRUE, null = "null")))
}

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 4) {
  stop(
    "Usage: Rscript scripts/du_hast_r_runner.R <lock|install> <manifest.json> <lockfile.json> <fetcher>",
    call. = FALSE
  )
}

mode <- args[[1]]
manifest_path <- args[[2]]
lock_path <- args[[3]]
fetcher <- args[[4]]

if (!file.exists(manifest_path)) {
  stop(sprintf("Manifest not found: %s", manifest_path), call. = FALSE)
}

read_manifest <- function(path) {
  fromJSON(path, simplifyVector = FALSE)
}

manifest_dependencies <- function(manifest) {
  deps <- manifest$dependencies
  if (is.null(deps)) {
    stop("Manifest has no dependencies object", call. = FALSE)
  }
  pkgs <- names(deps)
  pkgs <- pkgs[nzchar(pkgs)]
  unique(pkgs)
}

manifest_version_for <- function(manifest, pkg) {
  deps <- manifest$dependencies
  if (is.null(deps) || is.null(deps[[pkg]])) {
    return("*")
  }
  as.character(deps[[pkg]])
}

manifest_setting <- function(manifest, key, default) {
  settings <- manifest$settings
  if (is.null(settings)) {
    return(default)
  }
  val <- settings[[key]]
  if (is.null(val) || (is.character(val) && !nzchar(val))) {
    return(default)
  }
  val
}

manifest_repos <- function(manifest) {
  settings <- manifest$settings
  repos <- if (!is.null(settings)) settings$repos else NULL

  if (is.null(repos) || length(repos) == 0) {
    return(default_repositories())
  }

  if (is.null(names(repos)) || any(!nzchar(names(repos)))) {
    stop("settings.repos must be a named object of {name: url}", call. = FALSE)
  }

  repos <- unlist(repos)
  names(repos) <- names(settings$repos)
  repos
}

validate_requested_versions <- function(plan, manifest) {
  roots <- manifest_dependencies(manifest)
  for (pkg in roots) {
    requested <- manifest_version_for(manifest, pkg)
    if (requested == "*") {
      next
    }
    resolved <- unname(plan$metadata[pkg, "Version"])
    if (!identical(as.character(requested), as.character(resolved))) {
      stop(
        sprintf(
          "Version mismatch for '%s': requested %s but resolved %s. Update fer.json or repos.",
          pkg, requested, resolved
        ),
        call. = FALSE
      )
    }
  }
}

plan_to_lock <- function(plan, manifest) {
  list(
    lock_version = 1,
    generated_at_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    project = list(
      name = manifest$name %||% "du_hast_r_project",
      version = manifest$version %||% "0.1.0"
    ),
    settings = list(
      dynamics = isTRUE(manifest_setting(manifest, "dynamics", FALSE)),
      dynamic_mode = as.character(manifest_setting(manifest, "dynamic_mode", "shared_server")),
      repos = as.list(plan$repos)
    ),
    roots = manifest_dependencies(manifest),
    requested_versions = manifest$dependencies,
    plan = list(
      layers = plan$layers,
      packages = unname(plan$package_specs)
    )
  )
}

read_lock <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("Lock file not found: %s", path), call. = FALSE)
  }
  fromJSON(path, simplifyVector = FALSE)
}

lock_to_plan <- function(lock) {
  if (is.null(lock$plan) || is.null(lock$plan$layers) || is.null(lock$plan$packages)) {
    stop("Invalid lock file shape: expected plan.layers and plan.packages", call. = FALSE)
  }

  package_specs <- lock$plan$packages
  if (length(package_specs) == 0) {
    stop("Lock file has no packages", call. = FALSE)
  }

  package_names <- vapply(package_specs, function(x) x$package, "")
  names(package_specs) <- package_names

  repos <- unlist(lock$settings$repos)
  if (is.null(names(repos))) {
    names(repos) <- paste0("repo", seq_along(repos))
  }

  list(
    layers = lock$plan$layers,
    package_specs = package_specs,
    repos = repos
  )
}

normalize_dynamic_mode <- function(value) {
  mode <- tolower(as.character(value %||% "shared_server"))
  if (!mode %in% c("shared_server", "dedicated_builder")) {
    stop("settings.dynamic_mode must be 'shared_server' or 'dedicated_builder'", call. = FALSE)
  }
  mode
}

read_proc_meminfo <- function() {
  path <- "/proc/meminfo"
  if (!file.exists(path)) {
    return(NULL)
  }
  lines <- readLines(path, warn = FALSE)
  extract_kb <- function(key) {
    line <- grep(paste0("^", key), lines, value = TRUE)
    if (length(line) == 0) {
      return(NA_real_)
    }
    parts <- strsplit(trimws(line[[1]]), "\\s+")[[1]]
    value <- suppressWarnings(as.numeric(parts[[2]]))
    if (is.na(value)) NA_real_ else value * 1024
  }
  list(
    total_bytes = extract_kb("MemTotal:"),
    available_bytes = extract_kb("MemAvailable:")
  )
}

read_proc_loadavg <- function() {
  path <- "/proc/loadavg"
  if (!file.exists(path)) {
    return(NA_real_)
  }
  parts <- strsplit(readLines(path, warn = FALSE, n = 1), "\\s+")[[1]]
  suppressWarnings(as.numeric(parts[[1]]))
}

probe_host_state <- function() {
  logical <- suppressWarnings(parallel::detectCores(logical = TRUE))
  physical <- suppressWarnings(parallel::detectCores(logical = FALSE))
  meminfo <- read_proc_meminfo()
  list(
    logical_cpus = if (is.na(logical) || logical < 1) 1L else as.integer(logical),
    physical_cpus = if (is.na(physical) || physical < 1) {
      if (is.na(logical) || logical < 1) 1L else as.integer(logical)
    } else {
      as.integer(physical)
    },
    loadavg_1 = read_proc_loadavg(),
    mem_total_bytes = if (is.null(meminfo)) NA_real_ else meminfo$total_bytes,
    mem_available_bytes = if (is.null(meminfo)) NA_real_ else meminfo$available_bytes
  )
}

memory_scale_for_mode <- function(mode, state) {
  if (is.na(state$mem_total_bytes) || is.na(state$mem_available_bytes) || state$mem_total_bytes <= 0) {
    return(1)
  }
  available_ratio <- state$mem_available_bytes / state$mem_total_bytes
  if (mode == "shared_server") {
    if (available_ratio < 0.10) return(0.25)
    if (available_ratio < 0.20) return(0.50)
    if (available_ratio < 0.35) return(0.75)
    return(1)
  }
  if (available_ratio < 0.08) return(0.25)
  if (available_ratio < 0.15) return(0.50)
  if (available_ratio < 0.25) return(0.75)
  1
}

cpu_budget_for_mode <- function(mode, state) {
  logical <- max(1L, as.integer(state$logical_cpus))
  load <- state$loadavg_1
  if (is.na(load)) {
    return(if (mode == "shared_server") max(1L, floor(logical * 0.60)) else max(1L, floor(logical * 0.90)))
  }
  base <- if (mode == "shared_server") logical * 0.70 else logical * 1.05
  max(1L, floor(base - load))
}

resolve_dynamic_fetch <- function(manifest, package_count) {
  mode <- normalize_dynamic_mode(manifest_setting(manifest, "dynamic_mode", "shared_server"))
  state <- probe_host_state()
  cpu_budget <- cpu_budget_for_mode(mode, state)
  scaled <- max(1L, round(cpu_budget * memory_scale_for_mode(mode, state)))
  hard_cap <- if (mode == "shared_server") 12L else max(4L, min(24L, state$logical_cpus * 2L))
  initial <- min(as.integer(package_count), min(as.integer(hard_cap), as.integer(max(1L, scaled))))
  max_concurrency <- min(as.integer(package_count), as.integer(max(initial, hard_cap)))
  list(
    initial = max(1L, initial),
    dynamic = list(
      enabled = TRUE,
      mode = mode,
      min_concurrency = 1L,
      max_concurrency = max(1L, max_concurrency),
      rebalance_interval_ms = 1500L
    )
  )
}

resolve_install_runtime <- function(manifest, ready_count) {
  state <- probe_host_state()
  mode <- normalize_dynamic_mode(manifest_setting(manifest, "dynamic_mode", "shared_server"))
  cpu_budget <- cpu_budget_for_mode(mode, state)
  mem_scale <- memory_scale_for_mode(mode, state)
  physical_cap <- max(1L, if (mode == "shared_server") state$physical_cpus - 1L else state$physical_cpus)
  install_ncpus <- min(
    as.integer(ready_count),
    as.integer(max(1L, round(min(cpu_budget, physical_cap) * mem_scale)))
  )
  batch_size <- if (mode == "shared_server") {
    max(1L, min(as.integer(ready_count), install_ncpus))
  } else {
    max(1L, min(as.integer(ready_count), install_ncpus + 1L))
  }
  list(
    install_ncpus = max(1L, as.integer(install_ncpus)),
    make_jobs = max(1L, as.integer(install_ncpus)),
    batch_size = max(1L, as.integer(batch_size)),
    host = state,
    mode = mode
  )
}

with_make_jobs <- function(make_jobs, code) {
  old_makeflags <- Sys.getenv("MAKEFLAGS", unset = NA_character_)
  if (!is.null(make_jobs)) {
    Sys.setenv(MAKEFLAGS = sprintf("-j%d", as.integer(make_jobs)))
    on.exit({
      if (is.na(old_makeflags)) Sys.unsetenv("MAKEFLAGS") else Sys.setenv(MAKEFLAGS = old_makeflags)
    }, add = TRUE)
  }
  force(code)
}

install_layer_batches <- function(layer,
                                  layer_index,
                                  total_layers,
                                  results,
                                  target_lib,
                                  manifest,
                                  dynamics_enabled,
                                  fixed_install_ncpus,
                                  fixed_make_jobs,
                                  completed_packages,
                                  total_packages) {
  remaining <- layer
  while (length(remaining) > 0) {
    runtime <- if (dynamics_enabled) {
      resolve_install_runtime(manifest, length(remaining))
    } else {
      list(
        install_ncpus = as.integer(fixed_install_ncpus),
        make_jobs = as.integer(fixed_make_jobs),
        batch_size = length(remaining)
      )
    }

    batch <- remaining[seq_len(min(length(remaining), runtime$batch_size))]
    local_paths <- vapply(batch, function(pkg) results[[pkg]]$status$path, "")
    emit_event(
      "install",
      "batch",
      layer = as.integer(layer_index),
      layers = as.integer(total_layers),
      packages = as.integer(length(batch)),
      threads = as.integer(runtime$install_ncpus),
      make_jobs = as.integer(runtime$make_jobs),
      message = sprintf(
        "installing batch of %d package(s) with Ncpus=%d MAKEFLAGS=-j%d",
        length(batch),
        runtime$install_ncpus,
        runtime$make_jobs
      )
    )

    with_make_jobs(runtime$make_jobs, {
      utils::install.packages(
        local_paths,
        repos = NULL,
        type = "source",
        Ncpus = as.integer(runtime$install_ncpus),
        lib = target_lib
      )
    })

    completed_packages <- completed_packages + length(batch)
    emit_event(
      "install",
      "progress",
      layer = as.integer(layer_index),
      layers = as.integer(total_layers),
      completed_packages = as.integer(completed_packages),
      total_packages = as.integer(total_packages)
    )
    remaining <- remaining[-seq_len(length(batch))]
  }

  completed_packages
}

manifest <- read_manifest(manifest_path)

if (mode == "lock") {
  roots <- manifest_dependencies(manifest)
  repos <- manifest_repos(manifest)

  emit_event("resolve", "start", total_roots = length(roots), message = "building lock graph")
  t_resolve <- system.time({
    plan <- build_plan(
      packages = roots,
      repos = repos,
      dependency_fields = c("Depends", "Imports", "LinkingTo"),
      include_suggests = FALSE
    )
  })[["elapsed"]]

  validate_requested_versions(plan, manifest)
  lock <- plan_to_lock(plan, manifest)
  write_json(lock, path = lock_path, pretty = TRUE, auto_unbox = TRUE, null = "null")
  emit_event("resolve", "done", packages = length(plan$package_specs), seconds = as.numeric(t_resolve))
  emit_event("done", "done", total_seconds = as.numeric(t_resolve))
  cat(sprintf("Wrote lock file: %s\n", lock_path))
} else if (mode == "install") {
  lock <- read_lock(lock_path)
  plan <- lock_to_plan(lock)

  cache_dir <- manifest_setting(manifest, "cache_dir", file.path(tempdir(), "du-hast-r-cache"))
  dynamics_enabled <- isTRUE(manifest_setting(manifest, "dynamics", FALSE))
  download_threads <- as.integer(manifest_setting(manifest, "download_threads", 16L))
  install_ncpus <- as.integer(manifest_setting(manifest, "install_ncpus", 2L))
  make_jobs <- as.integer(manifest_setting(manifest, "make_jobs", 4L))
  lib <- manifest_setting(manifest, "lib", NULL)
  fetch_runtime <- if (dynamics_enabled) resolve_dynamic_fetch(manifest, length(plan$package_specs)) else NULL
  fetch_threads <- if (is.null(fetch_runtime)) download_threads else fetch_runtime$initial

  emit_event("fetch", "start", threads = fetch_threads)
  t_fetch <- system.time({
    fetch_response <- run_fetcher(
      plan = plan,
      cache_dir = cache_dir,
      fetcher = fetcher,
      download_concurrency = fetch_threads,
      dynamic_config = if (is.null(fetch_runtime)) NULL else fetch_runtime$dynamic
    )
  })[["elapsed"]]

  statuses <- fetch_response$results
  kinds <- vapply(statuses, function(x) x$status$kind, "")
  if (any(kinds != "success")) {
    bad <- vapply(statuses[kinds != "success"], function(x) x$package, "")
    stop(sprintf("Fetch failed for: %s", paste(bad, collapse = ", ")), call. = FALSE)
  }

  downloaded_bytes <- sum(vapply(
    statuses,
    function(x) if (isTRUE(x$status$cached)) 0 else as.numeric(x$status$bytes),
    numeric(1)
  ))
  reused_bytes <- sum(vapply(
    statuses,
    function(x) if (isTRUE(x$status$cached)) as.numeric(x$status$bytes) else 0,
    numeric(1)
  ))
  cache_hit_rate <- mean(vapply(statuses, function(x) isTRUE(x$status$cached), logical(1)))

  emit_event(
    "fetch",
    "done",
    seconds = as.numeric(t_fetch),
    downloaded_bytes = as.numeric(downloaded_bytes),
    reused_bytes = as.numeric(reused_bytes),
    cache_hit_rate = as.numeric(cache_hit_rate)
  )

  results <- setNames(fetch_response$results, vapply(fetch_response$results, `[[`, "", "package"))
  failed <- vapply(results, function(entry) entry$status$kind != "success", logical(1))
  if (any(failed)) {
    bad <- names(results)[failed]
    stop(sprintf("Fetch failed for: %s", paste(bad, collapse = ", ")), call. = FALSE)
  }

  target_lib <- resolve_install_library(lib)
  emit_event("install", "target", lib = target_lib, message = sprintf("installing into %s", target_lib))

  total_packages <- length(plan$package_specs)
  install_start_threads <- if (dynamics_enabled) {
    resolve_install_runtime(manifest, max(lengths(plan$layers), 1L))$install_ncpus
  } else {
    as.integer(install_ncpus)
  }
  emit_event(
    "install",
    "start",
    layers = length(plan$layers),
    total_packages = total_packages,
    threads = as.integer(install_start_threads)
  )
  completed_packages <- 0L
  t_install <- system.time({
    for (idx in seq_along(plan$layers)) {
      layer <- plan$layers[[idx]]
      completed_packages <- install_layer_batches(
        layer = layer,
        layer_index = idx,
        total_layers = length(plan$layers),
        results = results,
        target_lib = target_lib,
        manifest = manifest,
        dynamics_enabled = dynamics_enabled,
        fixed_install_ncpus = install_ncpus,
        fixed_make_jobs = make_jobs,
        completed_packages = completed_packages,
        total_packages = total_packages
      )
    } 
  })[["elapsed"]]

  emit_event("install", "done", seconds = as.numeric(t_install))
  emit_event("done", "done", total_seconds = as.numeric(t_fetch + t_install))
  cat("Install completed.\n")
} else {
  stop(sprintf("Unknown mode: %s", mode), call. = FALSE)
}
