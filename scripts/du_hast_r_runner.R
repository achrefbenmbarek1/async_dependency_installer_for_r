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
      edges = plan$edges,
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
    edges = if (!is.null(lock$plan$edges)) lock$plan$edges else derive_layer_edges(lock$plan$layers),
    package_specs = package_specs,
    repos = repos
  )
}

derive_layer_edges <- function(layers) {
  packages <- unique(unlist(layers))
  edges <- setNames(vector("list", length(packages)), packages)
  seen <- character()
  for (layer in layers) {
    for (pkg in layer) {
      edges[[pkg]] <- seen
    }
    seen <- c(seen, layer)
  }
  edges
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
    available_bytes = extract_kb("MemAvailable:"),
    swap_total_bytes = extract_kb("SwapTotal:"),
    swap_free_bytes = extract_kb("SwapFree:")
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
    mem_available_bytes = if (is.null(meminfo)) NA_real_ else meminfo$available_bytes,
    swap_total_bytes = if (is.null(meminfo)) NA_real_ else meminfo$swap_total_bytes,
    swap_free_bytes = if (is.null(meminfo)) NA_real_ else meminfo$swap_free_bytes
  )
}

host_pressure_for_mode <- function(mode, state) {
  if (is.na(state$mem_total_bytes) || is.na(state$mem_available_bytes) || state$mem_total_bytes <= 0) {
    return(list(
      available_ratio = NA_real_,
      swap_used_ratio = NA_real_,
      swap_active = FALSE,
      install_scale = 1,
      make_scale = 1
    ))
  }
  available_ratio <- state$mem_available_bytes / state$mem_total_bytes
  swap_used_ratio <- NA_real_
  swap_active <- FALSE
  if (!is.na(state$swap_total_bytes) && state$swap_total_bytes > 0 &&
      !is.na(state$swap_free_bytes) && state$swap_free_bytes >= 0) {
    swap_used_ratio <- max(0, min(1, (state$swap_total_bytes - state$swap_free_bytes) / state$swap_total_bytes))
    swap_active <- swap_used_ratio > 0.02
  }

  install_scale <- 1
  make_scale <- 1
  if (mode == "shared_server") {
    if (available_ratio < 0.10) {
      install_scale <- 0.25
      make_scale <- 0.25
    } else if (available_ratio < 0.20) {
      install_scale <- 0.50
      make_scale <- 0.35
    } else if (available_ratio < 0.35) {
      install_scale <- 0.75
      make_scale <- 0.60
    }
  } else {
    if (available_ratio < 0.08) {
      install_scale <- 0.25
      make_scale <- 0.25
    } else if (available_ratio < 0.15) {
      install_scale <- 0.50
      make_scale <- 0.35
    } else if (available_ratio < 0.25) {
      install_scale <- 0.75
      make_scale <- 0.60
    }
  }

  if (!is.na(swap_used_ratio)) {
    if (swap_used_ratio >= 0.25) {
      install_scale <- min(install_scale, 0.35)
      make_scale <- min(make_scale, 0.25)
    } else if (swap_used_ratio >= 0.10) {
      install_scale <- min(install_scale, 0.60)
      make_scale <- min(make_scale, 0.40)
    } else if (swap_active) {
      install_scale <- min(install_scale, if (mode == "shared_server") 0.75 else 0.85)
      make_scale <- min(make_scale, if (mode == "shared_server") 0.50 else 0.65)
    }
  }

  list(
    available_ratio = available_ratio,
    swap_used_ratio = swap_used_ratio,
    swap_active = swap_active,
    install_scale = install_scale,
    make_scale = make_scale
  )
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
  pressure <- host_pressure_for_mode(mode, state)
  scaled <- max(1L, round(cpu_budget * pressure$install_scale))
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
  pressure <- host_pressure_for_mode(mode, state)
  physical_cap <- max(1L, if (mode == "shared_server") state$physical_cpus - 1L else state$physical_cpus)
  base_install_cap <- min(cpu_budget, physical_cap)
  install_ncpus <- min(
    as.integer(ready_count),
    as.integer(max(1L, round(base_install_cap * pressure$install_scale)))
  )
  healthy_host <- is.na(pressure$available_ratio) ||
    (pressure$available_ratio >= if (mode == "shared_server") 0.35 else 0.25 &&
     (is.na(pressure$swap_used_ratio) || pressure$swap_used_ratio < 0.10))
  make_jobs_cap <- if (mode == "shared_server") {
    max(1L, min(2L, install_ncpus))
  } else {
    if (healthy_host) {
      max(1L, min(4L, install_ncpus + 2L))
    } else {
      max(1L, min(3L, install_ncpus + 1L))
    }
  }
  make_jobs <- max(1L, as.integer(round(make_jobs_cap * pressure$make_scale)))
  chunk_cap <- if (mode == "shared_server") {
    if (healthy_host) 2L else 1L
  } else {
    if (healthy_host) 4L else if (pressure$swap_active) 1L else 2L
  }
  batch_size <- max(
    1L,
    min(
      as.integer(ready_count),
      as.integer(max(1L, min(install_ncpus, chunk_cap)))
    )
  )
  list(
    install_ncpus = max(1L, as.integer(install_ncpus)),
    make_jobs = max(1L, as.integer(min(make_jobs, install_ncpus))),
    batch_size = max(1L, as.integer(batch_size)),
    host = state,
    mode = mode,
    pressure = pressure
  )
}

format_percent <- function(value) {
  if (is.na(value)) return("n/a")
  sprintf("%.0f%%", value * 100)
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

safe_pkg_filename <- function(pkg) {
  gsub("[^A-Za-z0-9_.-]+", "_", pkg)
}

build_install_graph <- function(plan) {
  packages <- names(plan$package_specs)
  edges <- plan$edges %||% derive_layer_edges(plan$layers)
  normalized_edges <- setNames(vector("list", length(packages)), packages)
  for (pkg in packages) {
    deps <- edges[[pkg]] %||% character()
    if (is.list(deps)) {
      deps <- unlist(deps, use.names = FALSE)
    }
    deps <- as.character(deps %||% character())
    deps <- deps[nzchar(deps)]
    normalized_edges[[pkg]] <- sort(unique(intersect(deps, packages)))
  }

  reverse_edges <- setNames(vector("list", length(packages)), packages)
  for (pkg in packages) {
    for (dep in normalized_edges[[pkg]]) {
      reverse_edges[[dep]] <- sort(unique(c(reverse_edges[[dep]], pkg)))
    }
  }

  remaining <- setNames(vapply(normalized_edges, length, integer(1)), names(normalized_edges))
  list(
    edges = normalized_edges,
    reverse_edges = reverse_edges,
    remaining = remaining,
    ready = sort(names(remaining)[remaining == 0L]),
    completed = character(),
    failed = character()
  )
}

install_one_package <- function(pkg, path, target_lib, make_jobs, log_path) {
  dir.create(dirname(log_path), recursive = TRUE, showWarnings = FALSE)
  log_con <- file(log_path, open = "at")
  sink(log_con, split = FALSE)
  sink(log_con, type = "message")
  on.exit({
    sink(type = "message")
    sink()
    close(log_con)
  }, add = TRUE)

  result <- tryCatch({
    elapsed <- system.time({
      with_make_jobs(make_jobs, {
        utils::install.packages(
          path,
          repos = NULL,
          type = "source",
          Ncpus = 1L,
          lib = target_lib
        )
      })
    })[["elapsed"]]
    list(ok = TRUE, package = pkg, seconds = as.numeric(elapsed), error_message = NA_character_)
  }, error = function(e) {
    list(ok = FALSE, package = pkg, seconds = NA_real_, error_message = conditionMessage(e))
  })

  flush(log_con)
  result
}

kill_install_jobs <- function(active) {
  for (entry in active) {
    pid <- entry$job$pid %||% NA_integer_
    if (!is.na(pid)) {
      suppressWarnings(tools::pskill(pid, 15L))
    }
  }
  invisible(NULL)
}

install_with_scheduler <- function(plan,
                                   results,
                                   target_lib,
                                   manifest,
                                   dynamics_enabled,
                                   fixed_install_ncpus,
                                   fixed_make_jobs,
                                   total_packages,
                                   log_dir) {
  graph <- build_install_graph(plan)
  active <- list()
  package_layer <- setNames(integer(0), character(0))
  for (idx in seq_along(plan$layers)) {
    for (pkg in plan$layers[[idx]]) {
      package_layer[[pkg]] <- idx
    }
  }

  completed_packages <- 0L
  while (completed_packages < total_packages) {
    runtime <- if (dynamics_enabled) {
      resolve_install_runtime(manifest, max(1L, length(graph$ready)))
    } else {
      list(
        install_ncpus = as.integer(fixed_install_ncpus),
        make_jobs = as.integer(fixed_make_jobs),
        pressure = NULL
      )
    }

    worker_limit <- max(1L, as.integer(runtime$install_ncpus))
    while (length(active) < worker_limit && length(graph$ready) > 0) {
      pkg <- graph$ready[[1]]
      graph$ready <- graph$ready[-1]
      local_path <- results[[pkg]]$status$path
      log_path <- file.path(log_dir, sprintf("%03d_%s.log", completed_packages + length(active) + 1L, safe_pkg_filename(pkg)))
      emit_event(
        "install",
        "package_start",
        package = pkg,
        layer = as.integer(package_layer[[pkg]] %||% 0L),
        layers = as.integer(length(plan$layers)),
        threads = as.integer(worker_limit),
        make_jobs = as.integer(runtime$make_jobs),
        mem_available_ratio = if (!is.null(runtime$pressure)) runtime$pressure$available_ratio else NA_real_,
        swap_used_ratio = if (!is.null(runtime$pressure)) runtime$pressure$swap_used_ratio else NA_real_,
        message = sprintf(
          "starting %s with worker_limit=%d MAKEFLAGS=-j%d | mem_avail=%s swap_used=%s",
          pkg,
          worker_limit,
          runtime$make_jobs,
          if (!is.null(runtime$pressure)) format_percent(runtime$pressure$available_ratio) else "n/a",
          if (!is.null(runtime$pressure)) format_percent(runtime$pressure$swap_used_ratio) else "n/a"
        )
      )
      job <- parallel::mcparallel(
        install_one_package(pkg, local_path, target_lib, runtime$make_jobs, log_path),
        silent = TRUE
      )
      active[[pkg]] <- list(job = job, runtime = runtime, log_path = log_path)
    }

    if (length(active) == 0) {
      if (length(graph$ready) == 0) {
        stop("Install scheduler stalled with no ready or active packages", call. = FALSE)
      }
      Sys.sleep(0.2)
      next
    }

    jobs <- lapply(active, `[[`, "job")
    names(jobs) <- names(active)
    completed_jobs <- parallel::mccollect(jobs, wait = FALSE, timeout = 0.5)
    if (is.null(completed_jobs)) {
      next
    }

    for (job_name in names(completed_jobs)) {
      result <- completed_jobs[[job_name]]
      pkg <- result$package %||% job_name
      runtime <- active[[pkg]]$runtime
      active[[pkg]] <- NULL

      if (is.null(result) || !isTRUE(result$ok)) {
        error_message <- if (is.null(result)) {
          "install worker exited without a result"
        } else {
          result$error_message %||% "unknown install failure"
        }
        emit_event(
          "install",
          "package_fail",
          package = pkg,
          layer = as.integer(package_layer[[pkg]] %||% 0L),
          layers = as.integer(length(plan$layers)),
          threads = as.integer(runtime$install_ncpus %||% 1L),
          make_jobs = as.integer(runtime$make_jobs %||% 1L),
          message = sprintf("package %s failed: %s", pkg, error_message)
        )
        if (length(active) > 0) {
          kill_install_jobs(active)
        }
        stop(sprintf("Install failed for %s: %s", pkg, error_message), call. = FALSE)
      }

      completed_packages <- completed_packages + 1L
      graph$completed <- c(graph$completed, pkg)
      emit_event(
        "install",
        "package_done",
        package = pkg,
        layer = as.integer(package_layer[[pkg]] %||% 0L),
        layers = as.integer(length(plan$layers)),
        seconds = as.numeric(result$seconds %||% NA_real_),
        threads = as.integer(runtime$install_ncpus %||% 1L),
        make_jobs = as.integer(runtime$make_jobs %||% 1L),
        mem_available_ratio = if (!is.null(runtime$pressure)) runtime$pressure$available_ratio else NA_real_,
        swap_used_ratio = if (!is.null(runtime$pressure)) runtime$pressure$swap_used_ratio else NA_real_,
        message = sprintf("installed %s in %.2fs", pkg, as.numeric(result$seconds %||% 0))
      )

      for (dependent in graph$reverse_edges[[pkg]] %||% character()) {
        graph$remaining[[dependent]] <- max(0L, graph$remaining[[dependent]] - 1L)
        if (graph$remaining[[dependent]] == 0L && !dependent %in% names(active) &&
            !dependent %in% graph$completed && !dependent %in% graph$ready) {
          graph$ready <- sort(c(graph$ready, dependent))
        }
      }

      emit_event(
        "install",
        "progress",
        layer = as.integer(package_layer[[pkg]] %||% 0L),
        layers = as.integer(length(plan$layers)),
        completed_packages = as.integer(completed_packages),
        total_packages = as.integer(total_packages),
        message = sprintf("completed %s (%d/%d)", pkg, completed_packages, total_packages)
      )
    }
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
  install_log_dir <- file.path(dirname(manifest_path), "install_logs")
  dir.create(install_log_dir, recursive = TRUE, showWarnings = FALSE)

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
    completed_packages <- install_with_scheduler(
      plan = plan,
      results = results,
      target_lib = target_lib,
      manifest = manifest,
      dynamics_enabled = dynamics_enabled,
      fixed_install_ncpus = install_ncpus,
      fixed_make_jobs = make_jobs,
      total_packages = total_packages,
      log_dir = install_log_dir
    )
  })[["elapsed"]]

  emit_event("install", "done", seconds = as.numeric(t_install))
  emit_event("done", "done", total_seconds = as.numeric(t_fetch + t_install))
  cat("Install completed.\n")
} else {
  stop(sprintf("Unknown mode: %s", mode), call. = FALSE)
}
