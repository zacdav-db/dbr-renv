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
library(renv)

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
    filter(!str_detect(slug, "ml$")) |>
    distinct()
}

#' Scrape the R toolchain definition for a Databricks Runtime release.
#'
#' @param runtime a row from get_available_dbr_runtimes as a list
#' @return List containing runtime metadata, R version, snapshot info, and package tibble.
get_dbr_r_spec <- function(runtime) {
  page <- read_html(runtime$url)

  # detect R version
  sys_list <- page |>
    html_elements(
      xpath = "//h2[@id='system-environment']/following-sibling::ul[1]/li"
    ) |>
    html_text2()

  r_version_line <- sys_list[str_detect(sys_list, "^R\\s*:")]
  r_version <- str_extract(r_version_line, "R: ([0-9\\.]+)", 1)

  # detect package snapshot
  # try two methods:
  #   1. extract link
  #   2. extract code element
  snapshot_link <- page |>
    html_element(
      xpath = "//h3[@id='installed-r-libraries']/following-sibling::p[1]"
    ) |>
    html_element("a") |>
    html_attr("href")

  if (is.na(snapshot_link)) {
    snapshot_link <- page |>
      html_element(
        xpath = "//h3[@id='installed-r-libraries']/following-sibling::p"
      ) |>
      html_element("code") |>
      html_text2() |>
      paste0("src/contrib/PACKAGES")
  }

  if (is.na(snapshot_link)) {
    stop("issue finding snapshot link")
  }

  repo_url <- str_extract(snapshot_link, "(.*\\d{4}-\\d{2}-\\d{2})")

  # detect packages
  pkg_table <- page |>
    html_element(
      xpath = "//h3[@id='installed-r-libraries']/following-sibling::div[1]//table"
    ) |>
    html_table(fill = TRUE)

  # detect version of renv was available at time of snapshot
  snapshot_renv_version <- read_html(snapshot_link) |>
    html_text2() |>
    str_extract(pattern = "Package: renv Version: (.*?) Imports", group = 1)

  # package table is in an awkward format that is N*6, but is actually N*3(lib,version)
  packages <- bind_rows(
    pkg_table[, 1:2],
    pkg_table[, 3:4],
    pkg_table[, 5:6]
  ) |>
    distinct() |>
    arrange() |>
    filter(Version != "", Library != "SparkR") |>
    mutate(Library = str_trim(str_extract(Library, "([A-z0-9\\.]+)\\s*?.*", 1)))

  list(
    runtime = runtime$label,
    slug = runtime$slug,
    url = runtime$url,
    r_version = r_version,
    snapshot = list(
      url = snapshot_link,
      repo_url = repo_url,
      name = "PPM Snapshot"
    ),
    packages = packages,
    snapshot_renv_version = snapshot_renv_version
  )
}

#' Create a project-local renv snapshot for a DBR release.
#'
#' The function scaffolds an renv project within `target_dir/<slug>` and writes a
#' lockfile populated with the scraped R toolchain. Package tarballs are not
#' installed; the lockfile is ready for `renv::restore()` to reproduce DBR R packages.
#'
#' @param runtime Slug, release-note URL, or local HTML file.
#' @param target_dir Destination directory that will contain per-runtime projects.
#' @return Invisible list containing the project path and lockfile data.
build_dbr_renv_snapshot <- function(
  spec,
  target_dir = "envs"
) {
  project_dir <- file.path(target_dir, spec$slug)

  # create renv from scaffold
  scaffold <- renv::scaffold(
    project = project_dir,
    version = spec$snapshot_renv_version,
    repos = c(
      setNames(spec$snapshot$repo_url, spec$snapshot$name),
      "PPM Latest" = "https://packagemanager.posit.co/cran"
    ),
    settings = list(
      ppm.enabled = TRUE,
      r.version = spec$r_version,
      snapshot.type = 'explicit'
    )
  )

  # add all packages into lockfile
  pkgs <- map2(
    spec$packages$Library,
    spec$packages$Version,
    ~ {
      list(
        Package = .x,
        Version = .y,
        Source = "Repository",
        Repository = "PPM Snapshot"
      )
    }
  )

  renv::record(pkgs, project = project_dir)
  return(project_dir)
}

# generate renvs
runtimes <- get_available_dbr_runtimes() |>
  filter(slug >= "17")

runtimes |>
  transpose() |>
  map(get_dbr_r_spec) |>
  walk(build_dbr_renv_snapshot)
