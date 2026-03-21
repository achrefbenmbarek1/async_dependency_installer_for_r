default_repositories <- function(include_bioconductor = TRUE) {
  repos <- getOption("repos")
  repos <- repos[repos != "@CRAN@"]
  if (!"CRAN" %in% names(repos)) {
    repos <- c(repos, CRAN = "https://cloud.r-project.org")
  }

  if (include_bioconductor && requireNamespace("BiocManager", quietly = TRUE)) {
    bioc_repos <- BiocManager::repositories()
    repos <- c(repos, bioc_repos[setdiff(names(bioc_repos), names(repos))])
  }

  repos
}

available_source_packages <- function(repos) {
   message("Calling available.packages() on repos:")
   print(repos)
  utils::available.packages(
    contriburl = unname(vapply(repos, utils::contrib.url, "", type = "source")),
    type = "source",
    filters = list()
  )
}

extract_dependencies <- function(field) {
  if (is.null(field) || is.na(field) || !nzchar(field)) {
    return(character())
  }

  entries <- trimws(unlist(strsplit(field, ",")))
  entries <- gsub("\\s*\\(.*\\)", "", entries)
  entries <- entries[nzchar(entries)]
  setdiff(entries, "R")
}

resolve_base_repo <- function(repository_value, repos) {
  if (is.null(repository_value) || is.na(repository_value) || !nzchar(repository_value)) {
    if ("CRAN" %in% names(repos)) {
      return(repos[["CRAN"]])
    }
    return(unname(repos[[1]]))
  }

  if (repository_value %in% names(repos)) {
    return(repos[[repository_value]])
  }

  if (repository_value %in% unname(repos)) {
    return(repository_value)
  }

  if (grepl("^https?://", repository_value)) {
    if (grepl("/src/contrib/?$", repository_value)) {
      return(sub("/src/contrib/?$", "", repository_value))
    }
    return(repository_value)
  }

  if (toupper(repository_value) == "CRAN" && "CRAN" %in% names(repos)) {
    return(repos[["CRAN"]])
  }

  if ("CRAN" %in% names(repos)) {
    repos[["CRAN"]]
  } else {
    unname(repos[[1]])
  }
}

build_plan <- function(packages,
                       repos = default_repositories(),
                       dependency_fields = c("Depends", "Imports", "LinkingTo"),
                       include_suggests = FALSE) {
  metadata <- available_source_packages(repos)
  fields <- dependency_fields
  if (include_suggests) {
    fields <- c(fields, "Suggests")
  }

  resolved <- character()
  queue <- unique(packages)
  edges <- list()

  while (length(queue) > 0) {
    pkg <- queue[[1]]
    queue <- queue[-1]
    if (pkg %in% resolved) {
      next
    }

    if (!pkg %in% rownames(metadata)) {
      stop(sprintf("Package '%s' was not found in configured repositories", pkg), call. = FALSE)
    }

    deps <- unique(unlist(lapply(fields, function(field) extract_dependencies(metadata[pkg, field]))))
    deps <- deps[deps %in% rownames(metadata)]
    edges[[pkg]] <- deps
    resolved <- c(resolved, pkg)
    queue <- unique(c(queue, setdiff(deps, resolved)))
  }

  layers <- topological_layers(edges)
  package_specs <- lapply(resolved, function(pkg) {
    row <- metadata[pkg, ]
    checksum <- unname(row[["MD5sum"]])
    if (is.na(checksum) || !nzchar(checksum)) {
      stop(sprintf("Package '%s' is missing MD5 metadata in PACKAGES", pkg), call. = FALSE)
    }
    repo_name <- row[["Repository"]]
    base_repo <- resolve_base_repo(repo_name, repos)

    filename <- sprintf("%s_%s.tar.gz", pkg, row[["Version"]])
    list(
      package = pkg,
      version = unname(row[["Version"]]),
      urls = list(sprintf("%s/%s", utils::contrib.url(base_repo, type = "source"), filename)),
      checksum = list(
        algorithm = "md5",
        value = checksum
      ),
      artifact_name = filename
    )
  })
  names(package_specs) <- resolved

  list(
    repos = repos,
    metadata = metadata[resolved, , drop = FALSE],
    edges = edges,
    layers = layers,
    package_specs = package_specs
  )
}

topological_layers <- function(edges) {
  remaining <- lapply(edges, unique)
  layers <- list()

  while (length(remaining) > 0) {
    ready <- sort(names(remaining)[vapply(remaining, length, integer(1)) == 0])
    if (length(ready) == 0) {
      stop("Dependency graph contains a cycle or unresolved package reference", call. = FALSE)
    }

    layers[[length(layers) + 1L]] <- ready
    remaining <- remaining[setdiff(names(remaining), ready)]
    remaining <- lapply(remaining, setdiff, ready)
  }

  layers
}

resolve_install_library <- function(lib = NULL) {
  candidates <- character()
  if (!is.null(lib) && !is.na(lib) && nzchar(lib)) {
    candidates <- c(candidates, lib)
  }
  candidates <- c(candidates, .libPaths(), Sys.getenv("R_LIBS_USER", unset = ""))
  candidates <- unique(candidates[nzchar(candidates)])

  for (candidate in candidates) {
    dir.create(candidate, recursive = TRUE, showWarnings = FALSE)
    if (dir.exists(candidate) && file.access(candidate, mode = 2) == 0) {
      return(normalizePath(candidate, winslash = "/", mustWork = FALSE))
    }
  }

  stop(
    "No writable R library path found. Set R_LIBS_USER or pass a writable 'lib' path.",
    call. = FALSE
  )
}

run_fetcher <- function(plan,
                        cache_dir,
                        fetcher = "./target/debug/async_dependency_installer_for_R",
                        download_concurrency = 16L,
                        dynamic_config = NULL) {
  request <- list(
    cache_dir = normalizePath(cache_dir, winslash = "/", mustWork = FALSE),
    concurrency = as.integer(download_concurrency),
    packages = unname(plan$package_specs)
  )
  if (!is.null(dynamic_config)) {
    request$dynamic <- dynamic_config
  }

  request_path <- tempfile("rust-fetch-request-", fileext = ".json")
  json <- jsonlite::toJSON(request, auto_unbox = TRUE, pretty = TRUE)
  writeLines(json, request_path)

  response <- system2(fetcher, request_path, stdout = TRUE, stderr = TRUE)
  status <- attr(response, "status")
  if (!is.null(status) && status != 0) {
    stop(paste(response, collapse = "\n"), call. = FALSE)
  }

  jsonlite::fromJSON(paste(response, collapse = "\n"), simplifyVector = FALSE)
}

install_from_fetch <- function(plan,
                               fetch_response,
                               install_ncpus = 1L,
                               make_jobs = NULL,
                               lib = NULL) {
  results <- setNames(fetch_response$results, vapply(fetch_response$results, `[[`, "", "package"))
  failed <- vapply(results, function(entry) entry$status$kind != "success", logical(1))
  if (any(failed)) {
    bad <- names(results)[failed]
    stop(sprintf("Fetch failed for: %s", paste(bad, collapse = ", ")), call. = FALSE)
  }
  target_lib <- resolve_install_library(lib)
  message(sprintf("Installing into library: %s", target_lib))

  old_makeflags <- Sys.getenv("MAKEFLAGS", unset = NA_character_)
  if (!is.null(make_jobs)) {
    Sys.setenv(MAKEFLAGS = sprintf("-j%d", as.integer(make_jobs)))
    on.exit({
      if (is.na(old_makeflags)) Sys.unsetenv("MAKEFLAGS") else Sys.setenv(MAKEFLAGS = old_makeflags)
    }, add = TRUE)
  }

  for (layer in plan$layers) {
    local_paths <- vapply(layer, function(pkg) results[[pkg]]$status$path, "")
    utils::install.packages(
      local_paths,
      repos = NULL,
      type = "source",
      Ncpus = as.integer(install_ncpus),
      lib = target_lib
    )
  }

  invisible(TRUE)
}

async_install_packages <- function(packages,
                                   repos = default_repositories(),
                                   fetcher = "./target/debug/async_dependency_installer_for_R",
                                   cache_dir = file.path(tempdir(), "r-artifact-cache"),
                                   download_concurrency = 16L,
                                   install_ncpus = 1L,
                                   make_jobs = NULL,
                                   lib = NULL,
                                   dependency_fields = c("Depends", "Imports", "LinkingTo"),
                                   include_suggests = FALSE,
                                   dry_run = FALSE) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("jsonlite is required", call. = FALSE)
  }
  message("Building Plan ...")
  plan <- build_plan(
    packages = packages,
    repos = repos,
    dependency_fields = dependency_fields,
    include_suggests = include_suggests
  )
   message("Plan built. Running fetcher...")
  fetch_response <- run_fetcher(
    plan = plan,
    cache_dir = cache_dir,
    fetcher = fetcher,
    download_concurrency = download_concurrency
  )
  message("Fetcher finished.")

  if (!dry_run) {
    install_from_fetch(
      plan = plan,
      fetch_response = fetch_response,
      install_ncpus = install_ncpus,
      make_jobs = make_jobs,
      lib = lib
    )
  }

  invisible(list(plan = plan, fetch = fetch_response))
}
