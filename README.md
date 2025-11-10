# Databricks Runtime `{renv}` Lockfiles

> \[!WARNING\] This repository is highly experimental, extensive testing hasn't been undertaken. It's recommended you only use the contents of the repository if you are experienced with package management in R.

A collection of ready-to-use `{renv}` environments that aims to mirror Databricks Runtime releases that pins the R version, CRAN snapshot, and included packages. This project does not aim to be a 1:1 copy of the R environment on Databricks, but to simplify development of workloads.

There will be differences between R on Databricks and externally, specifically:

-   Databricks Utility (dbutils) functions

-   `.libPaths` values

-   `{SparkR}` is not installed

The lockfile's do not configure:

-   Python

-   Spark (PySpark or SparkR)

## Runtime Inventory

| Runtime  | R version | PPM Snapshot Date (Posit Package Manager) |
|----------|-----------|-------------------------------------------|
| 17.0     | 4.4.2     | 2025-03-20                                |
| 17.1     | 4.4.2     | 2025-03-20                                |
| 17.2     | 4.4.2     | 2025-03-20                                |
| 17.3 LTS | 4.4.2     | 2025-03-20                                |

## Getting Started

Do not develop within the existing `envs/<runtime>` directories, instead make a new folder and point `renv::restore` and directly at the `renv.lock` file.

``` r
# restore from lockfile
renv::restore(lockfile = "<path-to-repo>/envs/17.3lts/renv.lock")
```

## Generating a Lockfile

The helper script in `scripts/generate.R` scrapes the Databricks Runtime release notes and builds lockfiles programmatically.

``` r
source("scripts/generate.R")

# discover runtimes from the docs
runtimes <- get_available_dbr_runtimes()

# turn a specific runtime into an R toolchain spec
spec <- runtimes |>
  dplyr::filter(slug == "17.3lts") |>
  purrr::transpose() |>
  purrr::pluck(1) |>
  get_dbr_r_spec()

# write envs/17.3lts/ with renv scaffolding + lockfile
build_dbr_renv_snapshot(spec, target_dir = "envs")
```

The script can also regenerate every supported runtime in one go (see the code at the bottom of `scripts/generate.R`), which keeps `envs/` in sync whenever Databricks publishes a new release.

> \[!IMPORTANT\] Some manual changes may be required to ensure things work as intended. For example, `{sparklyr}` was upgraded within DBR 17+ after it's release, and the version specified is not available in specified snapshot.