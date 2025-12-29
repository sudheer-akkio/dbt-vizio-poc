# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dbt project for Vizio TV viewership analytics on Databricks. Processes 700GB+ daily viewing data from Delta Share sources into analytical tables for content consumption, ad attribution, and demographic analysis.

## Common Commands

```bash
# Setup
python3 -m venv venv && source venv/bin/activate
pip install dbt-core dbt-databricks
dbt deps

# Run models
dbt run                                    # All models
dbt run --select vizio                     # All Vizio models
dbt run --select vizio_daily_fact_content_summary  # Single model
dbt run --select +vizio_daily_fact_content_summary # Model with upstream deps
dbt run --select vizio_daily_fact_content_detail+  # Model with downstream deps

# Incremental models with date range (batch mode)
dbt run --select vizio_daily_fact_content_detail --vars '{"start_date": "2024-10-01", "end_date": "2024-10-31"}'

# Full refresh for incremental models
dbt run --full-refresh --select vizio_daily_fact_commercial_detail vizio_daily_fact_commercial_summary

# Tests
dbt test --select vizio                    # All Vizio model tests
dbt test --select vizio_daily_fact_content_summary  # Single model tests

# Build (run + test)
dbt build --select vizio

# Debug/validate
dbt debug                                  # Check connection
dbt compile --select vizio                 # Compile SQL without running
```

## Architecture

### Data Flow
```
Delta Share Sources (vizio-poc-share.akkio)
    ↓
Enrichment (genre/timezone/category mappings)
    ↓
Detail Tables (incremental, session-level)
    ↓
Summary Tables (daily aggregates)
    ↓
Output: akkio.vizio_poc schema
```

### Model Categories (11 total)

**Content Pipeline** (`production_r2079_content_with_null`)
- `vizio_daily_fact_content_detail` → `vizio_daily_fact_content_summary`

**Commercial Pipeline** (`production_r2080_commercialfeedmodular`)
- `vizio_daily_fact_commercial_detail` → `vizio_daily_fact_commercial_summary`

**Device Activity** (`production_r2081_ipage`)
- `vizio_daily_fact_standard_detail`

**Campaign Attribution**
- `vizio_campaign_nothing_bundt_cakes`
- `vizio_campaign_farm_bureau_financial_services`

**Demographics** (`mk_akkio_experian_demo`)
- `v_akkio_attributes_latest` → `v_agg_akkio_hh`, `v_agg_akkio_ind`, `v_agg_akkio_media`

### Key Patterns

**Text Standardization:**
- Most fields: `lower(replace(column, ' ', '-'))`
- Input devices: `lower(replace(column, ' ', '_'))`
- Input categories: `upper(column)`

**Filtering:**
- All models: NULL device hashes excluded
- Content: NULL titles excluded, sessions < 10 seconds excluded
- Commercial: duration <= 0 excluded

**Clustering:** All tables clustered by `(partition_date, akkio_id)` via post-hook

**Incremental Strategy:** Detail tables use incremental with merge; support batch mode via `start_date`/`end_date` vars

### Key Joins
- Genre enrichment: `mk_akkio_genre_title_mapping` on `episode_id`
- Timezone: `mk_akkio_tvtimezone_mapping` on `hash`
- Commercial category: `mk_commercialcategory_mapping` on `creative`
- Location: Latest from `production_r2081_ipage` using `ROW_NUMBER() OVER (PARTITION BY hash ORDER BY date_partition DESC)`

## Configuration

**Profile:** `vizio_poc_databricks` in `~/.dbt/profiles.yml`

Required env vars:
- `DBT_DATABRICKS_HOST`
- `DBT_DATABRICKS_HTTP_PATH`
- `DBT_DATABRICKS_TOKEN`

**Output:** `akkio.vizio_poc` catalog/schema

## Documentation

- `docs/INTERNAL_README.md` - Detailed technical documentation with lineage diagrams
- `docs/VIZIO_MODELS_README.md` - Model overview and use cases
- `docs/CLIENT_README.md` - Client-facing data model documentation
- `models/vizio/schema.yml` - Column descriptions and tests
