# the functions below let you:
# 1. discover DBR release-note URLs
# 2. scrape the R stack (version + package set) for a runtime, and
# 3. create out a project-local renv snapshot that mirrors the DBR

library(rvest)
library(xml2)
library(stringr)
library(purrr)
library(tidyr)
library(dplyr)
library(jsonlite)

DBR_RUNTIME_INDEX <- "https://docs.databricks.com/aws/en/release-notes/runtime/"

#' Discover available Databricks Runtime release note pages.
#'
#' @param index_url Runtime release index to scrape.
#' @return Tibble with columns slug, label, url, variant.
get_available_dbr_runtimes <- function(index_url = DBR_RUNTIME_INDEX) {
  page <- read_html(index_url)
  anchors <- page |> html_elements("main a[href*='/release-notes/runtime/']")
  hrefs <- anchors |> html_attr("href")
  labels <- anchors |> html_text2()

  urls <- purrr::map_chr(hrefs, ~ xml2::url_absolute(.x, base = index_url))
  slugs <- basename(urls)
  is_version <- str_detect(slugs, "^\\d+\\.")

  tibble(
    slug = slugs[is_version],
    label = labels[is_version],
    url = urls[is_version]
  ) |>
    filter(!grepl("ml$", slug)) |>
    distinct()
}

#' Scrape the R toolchain definition for a Databricks Runtime release.
#'
#' @param runtime Slug (for example "17.3lts"), release-note URL, or local HTML file.
#' @param runtime_index Optional pre-computed index from get_available_dbr_runtimes().
#' @return List containing runtime metadata, R version, snapshot info, and package tibble.
get_dbr_r_spec <- function(runtime, runtime_index = NULL) {
  target <- resolve_runtime_input(runtime, runtime_index)
  page <- read_html(target$url)

  runtime_name <- page |> html_element("main h1") |> html_text2()

  sys_list <- page |>
    html_elements(
      xpath = "//h2[@id='system-environment']/following-sibling::ul[1]/li"
    ) |>
    html_text2()
  r_version_line <- sys_list[str_detect(sys_list, "^R\\s*:")]
  if (length(r_version_line) == 0) {
    stop(
      "Could not find R version in system environment section for ",
      target$slug
    )
  }
  r_version <- str_match(r_version_line, "R\\s*:\\s*([0-9\\.]+)")[, 2]

  snapshot_paragraph <- page |>
    html_element(
      xpath = "//h3[@id='installed-r-libraries']/following-sibling::p[1]"
    )
  snapshot_link <- if (inherits(snapshot_paragraph, "xml_missing")) {
    NULL
  } else {
    snapshot_paragraph |> html_element("a")
  }
  snapshot_url <- if (
    is.null(snapshot_link) || inherits(snapshot_link, "xml_missing")
  ) {
    NA_character_
  } else {
    snapshot_link |> html_attr("href")
  }
  snapshot_date <- if (is.na(snapshot_url)) {
    NA_character_
  } else {
    str_match(snapshot_url, "(\\d{4}-\\d{2}-\\d{2})")[, 2]
  }
  snapshot_repo <- if (is.na(snapshot_url)) {
    NA_character_
  } else {
    capture <- str_match(snapshot_url, "(.+/cran/\\d{4}-\\d{2}-\\d{2})")
    capture[, 2]
  }
  snapshot_name <- if (!is.na(snapshot_date)) {
    paste0("Posit-CRAN-", snapshot_date)
  } else {
    "Posit-CRAN"
  }
  repo_url <- snapshot_repo
  if (is.null(repo_url) || length(repo_url) == 0 || is.na(repo_url)) {
    repo_url <- snapshot_url
  }

  pkg_table_node <- page |>
    html_element(
      xpath = "//h3[@id='installed-r-libraries']/following-sibling::div[1]//table"
    )
  if (inherits(pkg_table_node, "xml_missing")) {
    stop("Could not locate installed R libraries table for ", target$slug)
  }
  raw_table <- pkg_table_node |> html_table(fill = TRUE)

  col_pairs <- seq(1, ncol(raw_table), by = 2)
  packages <- map_dfr(col_pairs, function(idx) {
    if (idx + 1 > ncol(raw_table)) {
      return(tibble(Package = character(), Version = character()))
    }
    tibble(
      Package = raw_table[[idx]],
      Version = raw_table[[idx + 1]]
    )
  }) |>
    mutate(across(everything(), ~ str_trim(as.character(.x)))) |>
    filter(!is.na(Package), Package != "", !is.na(Version), Version != "") |>
    distinct(Package, .keep_all = TRUE) |>
    arrange(Package)

  list(
    runtime = runtime_name,
    slug = target$slug,
    url = target$url,
    r_version = r_version,
    snapshot = list(
      url = snapshot_url,
      date = snapshot_date,
      repo_url = repo_url,
      name = snapshot_name
    ),
    packages = packages
  )
}

as_lockfile_packages <- function(packages, repository_name) {
  recs <- purrr::map2(packages$Package, packages$Version, function(pkg, ver) {
    list(
      Package = pkg,
      Version = ver,
      Source = "Repository",
      Repository = repository_name
    )
  })
  names(recs) <- packages$Package
  recs
}

#' Create a project-local renv snapshot for a DBR release.
#'
#' The function scaffolds an renv project within `target_dir/<slug>` and writes a
#' lockfile populated with the scraped R toolchain. Package tarballs are not
#' installed; the lockfile is ready for `renv::restore()` to reproduce the DBR stack.
#'
#' @param runtime Slug, release-note URL, or local HTML file.
#' @param target_dir Destination directory that will contain per-runtime projects.
#' @param overwrite Overwrite an existing runtime directory if TRUE.
#' @param runtime_index Optional pre-computed index.
#' @return Invisible list containing the project path and lockfile data.
build_dbr_renv_snapshot <- function(
  runtime,
  target_dir = "envs",
  overwrite = FALSE,
  runtime_index = NULL
) {
  if (!requireNamespace("renv", quietly = TRUE)) {
    stop("The 'renv' package must be installed to scaffold environments.")
  }

  spec <- get_dbr_r_spec(runtime, runtime_index)
  project_dir <- file.path(target_dir, spec$slug)

  if (dir.exists(project_dir)) {
    if (!overwrite) {
      stop(
        "Path '",
        project_dir,
        "' already exists. Supply overwrite = TRUE to replace it."
      )
    }
    unlink(project_dir, recursive = TRUE, force = TRUE)
  }

  dir.create(project_dir, recursive = TRUE, showWarnings = FALSE)
  renv::scaffold(project = project_dir)

  repo_name <- spec$snapshot$name
  if (is.null(repo_name) || length(repo_name) == 0 || is.na(repo_name)) {
    repo_name <- "Posit-CRAN"
  }

  repo_url <- spec$snapshot$repo_url
  if (is.null(repo_url) || length(repo_url) == 0 || is.na(repo_url)) {
    repo_url <- spec$snapshot$url
  }
  if (is.null(repo_url) || length(repo_url) == 0 || is.na(repo_url)) {
    repo_url <- "https://packagemanager.posit.co/cran/latest"
  }

  lockfile <- list(
    R = list(
      Version = spec$r_version,
      Repositories = list(
        list(
          Name = repo_name,
          URL = repo_url
        )
      )
    ),
    Packages = as_lockfile_packages(spec$packages, repo_name),
    renv = list(
      Version = as.character(utils::packageVersion("renv"))
    )
  )

  jsonlite::write_json(
    lockfile,
    file.path(project_dir, "renv.lock"),
    auto_unbox = TRUE,
    pretty = TRUE
  )

  invisible(list(
    project = project_dir,
    lockfile = lockfile,
    spec = spec
  ))
}
