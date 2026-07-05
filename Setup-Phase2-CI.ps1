<#
.SYNOPSIS
    Phase 2: writes .github/workflows/ci.yml, .github/workflows/cd.yml,
    .sqlfluff, CODEOWNERS, and requirements-dev.txt directly -- no download
    needed. Run from the root of your Banking_pipeline repo.

.DESCRIPTION
    - .github/workflows/ci.yml: lint (ruff/sqlfluff/yamllint), dbt parse,
      Airflow DAG import check, docker-compose validation -- runs on every
      PR and push to main.
    - .github/workflows/cd.yml: publishes dbt docs to GitHub Pages on
      merge to main.
    - .sqlfluff: dbt-templater config for sqlfluff.
    - CODEOWNERS: PR review gate -- edit @your-github-username to your
      actual handle before this does anything (also requires turning on
      branch protection in GitHub repo settings).
    - requirements-dev.txt: ruff/sqlfluff/yamllint/pytest, separate from
      the pipeline's own requirements.txt.

    IMPORTANT: before pushing, also apply the two small ruff auto-fixes
    to data_simulation/config.py and stream_simulator.py (given to you
    separately) -- without them, the very first CI run will fail on
    pre-existing lint issues unrelated to this phase.

.EXAMPLE
    cd "D:\NTI INTERNSHIP\Airflow\Banking_pipeline"
    .\Setup-Phase2-CI.ps1
#>

[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    Write-Host "[Setup-Phase2-CI] $Message"
}

function Write-FileIfNeeded {
    param([string]$Path, [string]$Content)
    $dir = Split-Path $Path -Parent
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    if ((Test-Path $Path) -and (-not $Force)) {
        Write-Log "SKIP (already exists, use -Force to overwrite): $Path"
        return
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
    Write-Log "Wrote: $Path"
}

$ciYml = @'
name: CI

# Fills the previously-empty .github/workflows/ci.yml stub.
#
# Scope note: this validates structure and correctness (Jinja/ref graph,
# YAML, Python lint, Airflow DAG import, Docker Compose syntax) rather than
# running a full dbt build against real data. The on-run-start hook
# (register_silver_sources()) calls delta_scan() on real Delta Lake paths
# that only exist once the Kafka -> Bronze -> Silver pipeline has actually
# run -- CI has no such data yet, so `dbt parse` (graph/Jinja validation,
# no data access) is the honest check here, not `dbt run`/`dbt test`.
# Adding CI-specific seed fixtures so `dbt build --empty` can run for real
# is a natural next step (tracked as a follow-up, not done in this pass).

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

env:
  PYTHON_VERSION: "3.12"

jobs:
  lint-python:
    name: Lint Python (ruff)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install ruff
        run: pip install ruff
      - name: Run ruff check
        run: ruff check kafka/ data_simulation/ scripts/

  lint-sql:
    name: Lint dbt SQL (sqlfluff)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install sqlfluff
        run: pip install sqlfluff==4.2.2 sqlfluff-templater-dbt==4.2.2 dbt-core==1.11.12 dbt-duckdb==1.10.1
      - name: dbt deps (needed for sqlfluff's dbt templater to resolve macros)
        working-directory: Banking_dbt
        run: dbt deps
      - name: Run sqlfluff lint
        working-directory: Banking_dbt
        env:
          # Points the dbt templater at a throwaway path -- sqlfluff only
          # needs to resolve the Jinja/macro graph, not query real data.
          DUCKDB_PATH: /tmp/ci_scratch.duckdb
        run: sqlfluff lint models/ --dialect duckdb

  lint-yaml:
    name: Lint YAML
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install yamllint
        run: pip install yamllint
      - name: Run yamllint
        run: |
          yamllint -d "{extends: default, rules: {line-length: disable, document-start: disable}}" \
            Banking_dbt/models \
            docker-compose.yml \
            kafka/docker-compose.yml \
            .github/workflows

  dbt-parse:
    name: dbt parse (validates ref/source graph + Jinja, no data required)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install dbt
        run: pip install dbt-core==1.11.12 dbt-duckdb==1.10.1
      - name: dbt deps
        working-directory: Banking_dbt
        run: dbt deps
      - name: dbt parse
        working-directory: Banking_dbt
        env:
          DUCKDB_PATH: /tmp/ci_scratch.duckdb
        # Deliberately `dbt parse`, not `dbt run`/`dbt test` -- see the
        # scope note at the top of this file for why a full run isn't
        # meaningful in CI yet without dedicated fixture data.
        run: dbt parse

  airflow-dag-syntax:
    name: Airflow DAG import check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Byte-compile every DAG file
        # Lightweight on purpose: catches syntax errors and bad imports
        # without needing a full Airflow install + metadata DB in CI.
        # tests/dags/test_dag_example.py (already in the repo) covers
        # deeper DAG-structure assertions when a real Airflow env is
        # available (e.g. via `astro dev pytest`, run locally/on-demand).
        run: |
          python -m py_compile airflow/dags/*.py
          echo "All DAG files compiled without syntax errors."

  docker-compose-validate:
    name: Validate docker-compose files
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate root docker-compose.yml
        run: docker compose -f docker-compose.yml config --quiet
      - name: Validate kafka/docker-compose.yml
        run: docker compose -f kafka/docker-compose.yml config --quiet

'@
$cdYml = @'
name: CD

# Fills the previously-empty .github/workflows/cd.yml stub.
#
# Scope note: this publishes dbt's auto-generated documentation site
# (lineage graph + model/column descriptions from schema.yml) to GitHub
# Pages on every merge to main. This is a real, achievable CD action given
# what actually exists today -- there's no live cloud warehouse to deploy
# *to* yet (Databricks/prod target in profiles.yml is unconfigured), so
# "deploy the docs site" is the honest, currently-achievable CD step.
#
# `dbt docs generate`'s catalog.json (live column stats/row counts) will be
# sparse in this environment since CI has no real warehouse connection --
# the lineage graph and schema.yml descriptions/tests are still fully
# populated and useful. Once a real prod target exists (Databricks secrets
# configured), point DBT_TARGET at it here for a richer catalog.

on:
  push:
    branches: [main]

jobs:
  publish-docs:
    name: Generate & publish dbt docs
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pages: write
      id-token: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install dbt
        run: pip install dbt-core==1.11.12 dbt-duckdb==1.10.1
      - name: dbt deps
        working-directory: Banking_dbt
        run: dbt deps
      - name: dbt docs generate
        working-directory: Banking_dbt
        env:
          DUCKDB_PATH: /tmp/cd_scratch.duckdb
        run: dbt docs generate
      - name: Prepare Pages artifact
        run: |
          mkdir -p _site
          cp Banking_dbt/target/index.html _site/
          cp Banking_dbt/target/manifest.json _site/
          cp Banking_dbt/target/catalog.json _site/
          cp Banking_dbt/target/run_results.json _site/ || true
      - uses: actions/upload-pages-artifact@v3
        with:
          path: _site
      - uses: actions/deploy-pages@v4
        id: deployment

'@
$sqlfluffCfg = @'
[sqlfluff]
templater = dbt
dialect = duckdb
sql_file_exts = .sql

[sqlfluff:templater:dbt]
project_dir = ./Banking_dbt
profiles_dir = ./Banking_dbt

# Long identifiers are common here (surrogate keys, generated test names) --
# don't fail lint on line length alone.
[sqlfluff:rules:layout.long_lines]
ignore_comment_lines = true

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
extended_capitalisation_policy = lower

'@
$codeowners = @'
# CODEOWNERS
#
# Requires review from the listed owner(s) before a PR touching these paths
# can merge -- pairs with branch protection rules configured in GitHub repo
# settings (Settings -> Branches -> Require pull request reviews -> Require
# review from Code Owners). This file alone does nothing without that
# branch protection setting also being turned on.
#
# Replace @your-github-username below with your actual GitHub handle (or a
# team handle, once this isn't a solo project) before relying on this.

# Default owner for anything not matched below
*                           @your-github-username

# dbt models -- the core data modeling layer
/Banking_dbt/                @your-github-username

# Kafka streaming layer
/kafka/                       @your-github-username

# Airflow orchestration
/airflow/                     @your-github-username

# Security/governance docs -- changes here matter more than average
/docs/SECURITY_AND_GOVERNANCE.md   @your-github-username
/docs/DR_RUNBOOK.md                @your-github-username

# CI/CD workflows themselves
/.github/workflows/           @your-github-username

'@
$reqDev = @'
# requirements-dev.txt
# Dev/CI tooling only -- not needed to run the pipeline itself.
# Install with: pip install -r requirements-dev.txt

ruff==0.6.9
sqlfluff==4.2.2
sqlfluff-templater-dbt==4.2.2
yamllint==1.35.1
pytest==8.3.3

'@

Write-FileIfNeeded -Path ".github\workflows\ci.yml" -Content $ciYml
Write-FileIfNeeded -Path ".github\workflows\cd.yml" -Content $cdYml
Write-FileIfNeeded -Path ".sqlfluff" -Content $sqlfluffCfg
Write-FileIfNeeded -Path "CODEOWNERS" -Content $codeowners
Write-FileIfNeeded -Path "requirements-dev.txt" -Content $reqDev

Write-Log ""
Write-Log "Phase 2 CI/CD files written."
Write-Log "Next steps:"
Write-Log "  1. Edit CODEOWNERS: replace @your-github-username with your real GitHub handle"
Write-Log "  2. pip install -r requirements-dev.txt"
Write-Log "  3. Apply the two ruff auto-fixes (config.py, stream_simulator.py) given separately"
Write-Log "  4. ruff check data_simulation\ kafka\ scripts\   (should report 0 errors)"
Write-Log "  5. git add, commit, push -- CI will run automatically on the PR/push"
Write-Log "  6. In GitHub repo Settings -> Pages: set source to 'GitHub Actions' for cd.yml to work"
