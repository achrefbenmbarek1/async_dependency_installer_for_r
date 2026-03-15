suppressPackageStartupMessages({
  library(jsonlite)
})

source("R/async_install.R")

args <- commandArgs(trailingOnly = TRUE)
config_path <- if (length(args) >= 1) args[[1]] else "scripts/benchmark_config.json"
out_dir <- if (length(args) >= 2) args[[2]] else file.path("benchmark_runs", format(Sys.time(), "%Y%m%d_%H%M%S"))

if (!file.exists(config_path)) {
  stop(sprintf("Config file not found: %s", config_path), call. = FALSE)
}

config <- fromJSON(config_path, simplifyVector = FALSE)

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

now_utc <- function() format(Sys.time(), tz = "UTC", usetz = TRUE)

bytes_from_files <- function(paths) {
  if (length(paths) == 0) return(0)
  sum(file.info(paths)$size, na.rm = TRUE)
}

safe_path <- function(...) normalizePath(file.path(...), winslash = "/", mustWork = FALSE)

ensure_writable_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(path) || file.access(path, mode = 2) != 0) {
    stop(sprintf("Directory is not writable: %s", path), call. = FALSE)
  }
  invisible(path)
}

free_space_bytes <- function(path) {
  probe <- normalizePath(path, winslash = "/", mustWork = FALSE)
  parent <- dirname(probe)
  while (!dir.exists(probe) && probe != parent) {
    probe <- parent
    parent <- dirname(probe)
  }
  out <- suppressWarnings(system2("df", c("-Pk", probe), stdout = TRUE, stderr = TRUE))
  if (length(out) < 2) return(NA_real_)
  fields <- strsplit(trimws(out[length(out)]), "\\s+")[[1]]
  if (length(fields) < 4) return(NA_real_)
  as.numeric(fields[4]) * 1024
}

check_disk_guard <- function(path, min_free_bytes) {
  free_bytes <- free_space_bytes(path)
  if (!is.na(free_bytes) && free_bytes < min_free_bytes) {
    stop(
      sprintf(
        "Disk guard triggered at %s: free %.2f GB < required %.2f GB",
        path,
        free_bytes / 1024^3,
        min_free_bytes / 1024^3
      ),
      call. = FALSE
    )
  }
  free_bytes
}

peak_tracker_new <- function(path) {
  structure(list(path = path, peak = 0), class = "peak_tracker")
}

peak_tracker_sample <- function(tracker) {
  out <- suppressWarnings(system2("du", c("-sb", tracker$path), stdout = TRUE, stderr = TRUE))
  if (length(out) == 0) return(tracker)
  fields <- strsplit(trimws(out[[1]]), "\\s+")[[1]]
  if (length(fields) < 1) return(tracker)
  used <- as.numeric(fields[[1]])
  if (!is.na(used) && used > tracker$peak) {
    tracker$peak <- used
  }
  tracker
}

stage_artifacts_from_plan <- function(plan, cache_dir) {
  ensure_writable_dir(cache_dir)
  results <- vector("list", length(plan$package_specs))
  names(results) <- names(plan$package_specs)

  for (pkg in names(plan$package_specs)) {
    spec <- plan$package_specs[[pkg]]
    target <- file.path(cache_dir, spec$artifact_name)
    expected_md5 <- tolower(spec$checksum$value)
    cached <- FALSE

    if (file.exists(target)) {
      actual_md5 <- tolower(unname(tools::md5sum(target)))
      if (!is.na(actual_md5) && actual_md5 == expected_md5) {
        cached <- TRUE
      } else {
        unlink(target)
      }
    }

    if (!cached) {
      ok <- FALSE
      errs <- character()
      for (url in spec$urls) {
        status <- tryCatch({
          utils::download.file(url, destfile = target, mode = "wb", quiet = TRUE)
          0L
        }, error = function(e) {
          errs <<- c(errs, sprintf("%s -> %s", url, conditionMessage(e)))
          1L
        })
        if (identical(status, 0L) && file.exists(target)) {
          actual_md5 <- tolower(unname(tools::md5sum(target)))
          if (!is.na(actual_md5) && actual_md5 == expected_md5) {
            ok <- TRUE
            break
          }
          errs <- c(errs, sprintf("%s -> checksum mismatch", url))
          unlink(target)
        } else if (!identical(status, 0L)) {
          errs <- c(errs, sprintf("%s -> download status %s", url, as.character(status)))
        }
      }
      if (!ok) {
        stop(sprintf("Failed to stage %s: %s", pkg, paste(errs, collapse = " | ")), call. = FALSE)
      }
    }

    size <- file.info(target)$size
    results[[pkg]] <- list(path = target, bytes = as.numeric(size), cached = cached)
  }

  results
}

install_serial_from_plan <- function(plan, staged, lib) {
  for (layer in plan$layers) {
    for (pkg in layer) {
      utils::install.packages(
        staged[[pkg]]$path,
        repos = NULL,
        type = "source",
        Ncpus = 1L,
        lib = lib
      )
    }
  }
}

install_tuned_from_plan <- function(plan, staged, lib, install_ncpus, make_jobs) {
  old_makeflags <- Sys.getenv("MAKEFLAGS", unset = NA_character_)
  if (!is.null(make_jobs)) {
    Sys.setenv(MAKEFLAGS = sprintf("-j%d", as.integer(make_jobs)))
    on.exit({
      if (is.na(old_makeflags)) Sys.unsetenv("MAKEFLAGS") else Sys.setenv(MAKEFLAGS = old_makeflags)
    }, add = TRUE)
  }

  for (layer in plan$layers) {
    local_paths <- vapply(layer, function(pkg) staged[[pkg]]$path, "")
    utils::install.packages(
      local_paths,
      repos = NULL,
      type = "source",
      Ncpus = as.integer(install_ncpus),
      lib = lib
    )
  }
}

prepare_renv_lockfile <- function(stack_name, plan, stack_root, repos) {
  if (!requireNamespace("renv", quietly = TRUE)) {
    stop("renv baseline requested but 'renv' package is not installed", call. = FALSE)
  }

  lockfile <- file.path(stack_root, sprintf("%s.renv.lock", stack_name))
  if (file.exists(lockfile)) {
    return(lockfile)
  }

  prep_project <- file.path(stack_root, "renv_lockfile_prep")
  unlink(prep_project, recursive = TRUE, force = TRUE)
  dir.create(prep_project, recursive = TRUE, showWarnings = FALSE)
  renv_root <- file.path(stack_root, "renv_paths", "prep")
  ensure_writable_dir(renv_root)

  old_repos <- getOption("repos")
  old_renv_root <- Sys.getenv("RENV_PATHS_ROOT", unset = NA_character_)
  old_download_static_v8 <- Sys.getenv("DOWNLOAD_STATIC_LIBV8", unset = NA_character_)
  on.exit(options(repos = old_repos), add = TRUE)
  on.exit({
    if (is.na(old_renv_root)) Sys.unsetenv("RENV_PATHS_ROOT") else Sys.setenv(RENV_PATHS_ROOT = old_renv_root)
  }, add = TRUE)
  on.exit({
    if (is.na(old_download_static_v8)) Sys.unsetenv("DOWNLOAD_STATIC_LIBV8") else Sys.setenv(DOWNLOAD_STATIC_LIBV8 = old_download_static_v8)
  }, add = TRUE)

  options(repos = repos)
  Sys.setenv(RENV_PATHS_ROOT = renv_root)
  Sys.setenv(DOWNLOAD_STATIC_LIBV8 = "1")
  renv::consent(provided = TRUE)
  renv::init(project = prep_project, bare = TRUE)

  pkg_names <- names(plan$package_specs)
  renv::install(pkg_names, project = prep_project, prompt = FALSE)
  renv::snapshot(project = prep_project, lockfile = lockfile, prompt = FALSE)

  lockfile
}

run_async <- function(plan, env_paths, fetcher, download_concurrency, install_ncpus, make_jobs) {
  t_fetch <- system.time({
    fetch <- run_fetcher(
      plan = plan,
      cache_dir = env_paths$cache_dir,
      fetcher = fetcher,
      download_concurrency = as.integer(download_concurrency)
    )
  })[["elapsed"]]

  t_install <- system.time({
    install_from_fetch(
      plan = plan,
      fetch_response = fetch,
      install_ncpus = as.integer(install_ncpus),
      make_jobs = make_jobs,
      lib = env_paths$lib_dir
    )
  })[["elapsed"]]

  results <- fetch$results
  kinds <- vapply(results, function(x) x$status$kind, "")
  ok <- all(kinds == "success")
  bytes <- vapply(
    results,
    function(x) {
      if (!identical(x$status$kind, "success")) return(0)
      as.numeric(x$status$bytes)
    },
    numeric(1)
  )
  cached <- vapply(
    results,
    function(x) {
      if (!identical(x$status$kind, "success")) return(FALSE)
      isTRUE(x$status$cached)
    },
    logical(1)
  )
  attempts <- vapply(
    results,
    function(x) {
      if (!identical(x$status$kind, "error")) return(0L)
      length(x$status$attempts)
    },
    integer(1)
  )

  list(
    ok = ok,
    total_seconds = t_fetch + t_install,
    download_seconds = t_fetch,
    install_seconds = t_install,
    downloaded_bytes = sum(bytes[!cached]),
    reused_bytes = sum(bytes[cached]),
    cache_hit_rate = mean(cached),
    retry_count = sum(attempts),
    fetch_errors = sum(kinds != "success")
  )
}

run_serial_or_tuned <- function(plan, env_paths, tuned, install_ncpus, make_jobs) {
  t_download <- system.time({
    staged <- stage_artifacts_from_plan(plan, env_paths$cache_dir)
  })[["elapsed"]]

  t_install <- system.time({
    if (tuned) {
      install_tuned_from_plan(plan, staged, env_paths$lib_dir, install_ncpus, make_jobs)
    } else {
      install_serial_from_plan(plan, staged, env_paths$lib_dir)
    }
  })[["elapsed"]]

  bytes <- vapply(staged, function(x) x$bytes, numeric(1))
  cached <- vapply(staged, function(x) x$cached, logical(1))

  list(
    ok = TRUE,
    total_seconds = t_download + t_install,
    download_seconds = t_download,
    install_seconds = t_install,
    downloaded_bytes = sum(bytes[!cached]),
    reused_bytes = sum(bytes[cached]),
    cache_hit_rate = mean(cached),
    retry_count = 0L,
    fetch_errors = 0L
  )
}

run_renv <- function(lockfile, env_paths, repos) {
  if (!requireNamespace("renv", quietly = TRUE)) {
    stop("renv baseline requested but 'renv' package is not installed", call. = FALSE)
  }

  project <- env_paths$project_dir
  ensure_writable_dir(project)
  file.copy(lockfile, file.path(project, "renv.lock"), overwrite = TRUE)

  old_repos <- getOption("repos")
  old_renv_root <- Sys.getenv("RENV_PATHS_ROOT", unset = NA_character_)
  old_download_static_v8 <- Sys.getenv("DOWNLOAD_STATIC_LIBV8", unset = NA_character_)
  on.exit(options(repos = old_repos), add = TRUE)
  on.exit({
    if (is.na(old_renv_root)) Sys.unsetenv("RENV_PATHS_ROOT") else Sys.setenv(RENV_PATHS_ROOT = old_renv_root)
  }, add = TRUE)
  on.exit({
    if (is.na(old_download_static_v8)) Sys.unsetenv("DOWNLOAD_STATIC_LIBV8") else Sys.setenv(DOWNLOAD_STATIC_LIBV8 = old_download_static_v8)
  }, add = TRUE)

  options(repos = repos)
  Sys.setenv(RENV_PATHS_ROOT = env_paths$renv_paths_root)
  Sys.setenv(DOWNLOAD_STATIC_LIBV8 = "1")
  renv::consent(provided = TRUE)

  t_total <- system.time({
    renv::init(project = project, bare = TRUE)
    renv::restore(project = project, lockfile = lockfile, prompt = FALSE, clean = TRUE)
  })[["elapsed"]]

  list(
    ok = TRUE,
    total_seconds = t_total,
    download_seconds = NA_real_,
    install_seconds = NA_real_,
    downloaded_bytes = NA_real_,
    reused_bytes = NA_real_,
    cache_hit_rate = NA_real_,
    retry_count = 0L,
    fetch_errors = 0L
  )
}

run_single_scenario <- function(stack_name,
                                packages,
                                method,
                                cache_state,
                                repetition,
                                benchmark_root,
                                cfg,
                                repos,
                                lockfile = NULL) {
  stack_root <- file.path(benchmark_root, stack_name)
  method_root <- file.path(stack_root, method)
  run_root <- file.path(method_root, sprintf("rep_%02d", repetition))

  ensure_writable_dir(run_root)
  tracker <- peak_tracker_new(run_root)
  tracker <- peak_tracker_sample(tracker)

  min_free <- as.numeric(cfg$disk_guard$min_free_gb) * 1024^3
  free_before <- check_disk_guard(run_root, min_free)

  plan <- build_plan(
    packages = packages,
    repos = repos,
    dependency_fields = unlist(cfg$dependency_fields),
    include_suggests = isTRUE(cfg$include_suggests)
  )

  env_paths <- list(
    cache_dir = file.path(run_root, "cache"),
    lib_dir = file.path(run_root, "library"),
    project_dir = file.path(run_root, "renv_project"),
    renv_paths_root = file.path(method_root, "renv_paths")
  )

  ensure_writable_dir(env_paths$lib_dir)
  ensure_writable_dir(env_paths$cache_dir)
  ensure_writable_dir(env_paths$renv_paths_root)

  if (identical(cache_state, "cold")) {
    unlink(env_paths$cache_dir, recursive = TRUE, force = TRUE)
    unlink(env_paths$lib_dir, recursive = TRUE, force = TRUE)
    unlink(env_paths$project_dir, recursive = TRUE, force = TRUE)
    ensure_writable_dir(env_paths$cache_dir)
    ensure_writable_dir(env_paths$lib_dir)
  }

  tracker <- peak_tracker_sample(tracker)

  started_at <- now_utc()
  scenario <- tryCatch({
    if (method == "async") {
      run_async(
        plan = plan,
        env_paths = env_paths,
        fetcher = cfg$fetcher,
        download_concurrency = cfg$download_concurrency,
        install_ncpus = cfg$install_ncpus,
        make_jobs = cfg$make_jobs
      )
    } else if (method == "serial") {
      run_serial_or_tuned(
        plan = plan,
        env_paths = env_paths,
        tuned = FALSE,
        install_ncpus = cfg$install_ncpus,
        make_jobs = cfg$make_jobs
      )
    } else if (method == "tuned") {
      run_serial_or_tuned(
        plan = plan,
        env_paths = env_paths,
        tuned = TRUE,
        install_ncpus = cfg$install_ncpus,
        make_jobs = cfg$make_jobs
      )
    } else if (method == "renv") {
      run_renv(lockfile = lockfile, env_paths = env_paths, repos = repos)
    } else {
      stop(sprintf("Unknown method: %s", method), call. = FALSE)
    }
  }, error = function(e) {
    list(
      ok = FALSE,
      total_seconds = NA_real_,
      download_seconds = NA_real_,
      install_seconds = NA_real_,
      downloaded_bytes = NA_real_,
      reused_bytes = NA_real_,
      cache_hit_rate = NA_real_,
      retry_count = NA_integer_,
      fetch_errors = NA_integer_,
      error_message = conditionMessage(e)
    )
  })

  tracker <- peak_tracker_sample(tracker)
  free_after <- free_space_bytes(run_root)

  result <- list(
    stack = stack_name,
    package_count = length(packages),
    method = method,
    cache_state = cache_state,
    repetition = repetition,
    started_at_utc = started_at,
    finished_at_utc = now_utc(),
    ok = isTRUE(scenario$ok),
    total_seconds = scenario$total_seconds,
    download_seconds = scenario$download_seconds,
    install_seconds = scenario$install_seconds,
    downloaded_bytes = scenario$downloaded_bytes,
    reused_bytes = scenario$reused_bytes,
    cache_hit_rate = scenario$cache_hit_rate,
    retry_count = scenario$retry_count,
    fetch_errors = scenario$fetch_errors,
    peak_run_dir_bytes = tracker$peak,
    free_space_before_bytes = free_before,
    free_space_after_bytes = free_after,
    run_root = run_root,
    error_message = if (!is.null(scenario$error_message)) scenario$error_message else NA_character_
  )

  should_cleanup_now <- identical(cache_state, "warm")
  if (isTRUE(cfg$cleanup$enabled) && should_cleanup_now) {
    if (isTRUE(result$ok) || isTRUE(cfg$cleanup$on_failure)) {
      unlink(run_root, recursive = TRUE, force = TRUE)
    }
  }

  result
}

write_results <- function(results, out_dir, config) {
  json_path <- file.path(out_dir, "benchmark_results.json")
  csv_path <- file.path(out_dir, "benchmark_results.csv")

  payload <- list(
    generated_at_utc = now_utc(),
    config = config,
    results = results
  )
  write_json(payload, path = json_path, pretty = TRUE, auto_unbox = TRUE, null = "null")

  flatten_row <- function(x) {
    data.frame(
      stack = x$stack,
      package_count = x$package_count,
      method = x$method,
      cache_state = x$cache_state,
      repetition = x$repetition,
      ok = x$ok,
      total_seconds = x$total_seconds,
      download_seconds = x$download_seconds,
      install_seconds = x$install_seconds,
      downloaded_bytes = x$downloaded_bytes,
      reused_bytes = x$reused_bytes,
      cache_hit_rate = x$cache_hit_rate,
      retry_count = x$retry_count,
      fetch_errors = x$fetch_errors,
      peak_run_dir_bytes = x$peak_run_dir_bytes,
      free_space_before_bytes = x$free_space_before_bytes,
      free_space_after_bytes = x$free_space_after_bytes,
      error_message = x$error_message,
      stringsAsFactors = FALSE
    )
  }

  rows <- do.call(rbind, lapply(results, flatten_row))
  write.csv(rows, file = csv_path, row.names = FALSE)
}

validate_stack_packages <- function(stacks, repos, dependency_fields, include_suggests) {
  # Keep signature aligned with benchmark config knobs even though availability check
  # currently depends only on resolved repository metadata.
  invisible(dependency_fields)
  invisible(include_suggests)
  metadata <- available_source_packages(repos)
  missing_by_stack <- list()

  for (stack_name in names(stacks)) {
    packages <- unique(unlist(stacks[[stack_name]]))
    missing <- setdiff(packages, rownames(metadata))
    if (length(missing) > 0) {
      missing_by_stack[[stack_name]] <- missing
    }
  }

  if (length(missing_by_stack) > 0) {
    details <- vapply(
      names(missing_by_stack),
      function(stack_name) {
        sprintf("%s -> %s", stack_name, paste(sort(missing_by_stack[[stack_name]]), collapse = ", "))
      },
      character(1)
    )
    stop(
      paste(
        "Preflight failed: some requested packages are not available in configured repositories.",
        paste(details, collapse = " | "),
        sep = "\n"
      ),
      call. = FALSE
    )
  }

  invisible(TRUE)
}

run_benchmark <- function(config, out_dir) {
  repos <- default_repositories(include_bioconductor = isTRUE(config$include_bioconductor))
  benchmark_root <- safe_path(out_dir)
  ensure_writable_dir(benchmark_root)

  methods <- unlist(config$methods)
  cache_states <- c("cold", "warm")
  repetitions <- as.integer(config$repetitions)

  results <- list()
  idx <- 1L

  stacks <- config$stacks
  validate_stack_packages(
    stacks = stacks,
    repos = repos,
    dependency_fields = unlist(config$dependency_fields),
    include_suggests = isTRUE(config$include_suggests)
  )
  for (stack_name in names(stacks)) {
    packages <- unlist(stacks[[stack_name]])
    stack_root <- file.path(benchmark_root, stack_name)
    ensure_writable_dir(stack_root)

    lockfile <- NULL
    if ("renv" %in% methods) {
      lockfile <- prepare_renv_lockfile(
        stack_name,
        build_plan(
          packages,
          repos = repos,
          dependency_fields = unlist(config$dependency_fields),
          include_suggests = isTRUE(config$include_suggests)
        ),
        stack_root,
        repos
      )
    }

    for (method in methods) {
      for (rep in seq_len(repetitions)) {
        for (cache_state in cache_states) {
          cat(sprintf(
            "[%s] stack=%s method=%s rep=%d cache=%s\n",
            now_utc(), stack_name, method, rep, cache_state
          ))

          result <- run_single_scenario(
            stack_name = stack_name,
            packages = packages,
            method = method,
            cache_state = cache_state,
            repetition = rep,
            benchmark_root = benchmark_root,
            cfg = config,
            repos = repos,
            lockfile = lockfile
          )

          results[[idx]] <- result
          idx <- idx + 1L

          if (!isTRUE(result$ok) && isTRUE(config$stop_on_error)) {
            write_results(results, out_dir, config)
            stop(sprintf("Benchmark aborted on first failure: %s", result$error_message), call. = FALSE)
          }
        }
      }

      if (isTRUE(config$cleanup$enabled)) {
        method_root <- file.path(benchmark_root, stack_name, method)
        if (dir.exists(method_root)) {
          unlink(method_root, recursive = TRUE, force = TRUE)
        }
      }
    }
  }

  write_results(results, out_dir, config)
  invisible(results)
}

cat(sprintf("Loading benchmark config: %s\n", config_path))
cat(sprintf("Writing results to: %s\n", out_dir))

run_benchmark(config, out_dir)
cat("Benchmark run completed.\n")
