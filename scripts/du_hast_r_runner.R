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
      download_threads = as.integer(manifest_setting(manifest, "download_threads", 16L)),
      install_ncpus = as.integer(manifest_setting(manifest, "install_ncpus", 2L)),
      make_jobs = as.integer(manifest_setting(manifest, "make_jobs", 4L)),
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
  download_threads <- as.integer(manifest_setting(manifest, "download_threads", 16L))
  install_ncpus <- as.integer(manifest_setting(manifest, "install_ncpus", 2L))
  make_jobs <- as.integer(manifest_setting(manifest, "make_jobs", 4L))
  lib <- manifest_setting(manifest, "lib", NULL)

  emit_event("fetch", "start", threads = download_threads)
  t_fetch <- system.time({
    fetch_response <- run_fetcher(
      plan = plan,
      cache_dir = cache_dir,
      fetcher = fetcher,
      download_concurrency = download_threads
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
  old_makeflags <- Sys.getenv("MAKEFLAGS", unset = NA_character_)
  if (!is.null(make_jobs)) {
    Sys.setenv(MAKEFLAGS = sprintf("-j%d", as.integer(make_jobs)))
    on.exit({
      if (is.na(old_makeflags)) Sys.unsetenv("MAKEFLAGS") else Sys.setenv(MAKEFLAGS = old_makeflags)
    }, add = TRUE)
  }

  total_packages <- length(plan$package_specs)
  emit_event("install", "start", layers = length(plan$layers), total_packages = total_packages)
  completed_packages <- 0L
  t_install <- system.time({
    for (idx in seq_along(plan$layers)) {
      layer <- plan$layers[[idx]]
      local_paths <- vapply(layer, function(pkg) results[[pkg]]$status$path, "")
      utils::install.packages(
        local_paths,
        repos = NULL,
        type = "source",
        Ncpus = as.integer(install_ncpus),
        lib = target_lib
      )
      completed_packages <- completed_packages + length(layer)
      emit_event(
        "install",
        "progress",
        layer = idx,
        layers = length(plan$layers),
        completed_packages = as.integer(completed_packages),
        total_packages = as.integer(total_packages)
      )
    }
  })[["elapsed"]]

  emit_event("install", "done", seconds = as.numeric(t_install))
  emit_event("done", "done", total_seconds = as.numeric(t_fetch + t_install))
  cat("Install completed.\n")
} else {
  stop(sprintf("Unknown mode: %s", mode), call. = FALSE)
}
