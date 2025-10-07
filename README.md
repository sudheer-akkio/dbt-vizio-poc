# DBT Pipeline for Vizio POC

## Quick Start

### Setup

1. Create and activate virtual environment
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Install dependencies
    ```bash
    pip install dbt-core dbt-databricks
    ```

3. Install dbt packages
    ```bash
    dbt deps
    ```

4. Configure profile in `~/.dbt/profiles.yml`
    ```yaml
    vizio_poc_databricks:
        outputs:
            dev:
                type: databricks
                host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
                http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
                token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
                catalog: akkio
                schema: vizio_poc
                threads: 4
        target: dev 
    ```

### Running the Pipeline

```bash
# Run all models
dbt run

# Run specific model
dbt run --select vizio_daily_fact_content_summary

# For models that have set materialized='incremental', if you want to do a full refresh on data load:
dbt run --full-refresh --select vizio_daily_fact_commercial_detail vizio_daily_fact_commercial_summary

# Build and test together
dbt build --select vizio
```

## Project Structure

```
dbt-vizio-poc/
├── models/vizio/              # Vizio data models
│   ├── schema.yml             # Model documentation + generic tests
│   ├── vizio_daily_fact_content_summary.sql
│   ├── vizio_daily_fact_content_detail.sql
│   ├── vizio_daily_fact_commercial_summary.sql
│   ├── vizio_daily_fact_commercial_detail.sql
│   ├── vizio_daily_fact_standard_summary.sql
│   ├── vizio_daily_fact_standard_detail.sql
│   ├── vizio_campaign_nothing_bundt_cakes.sql
│   ├── vizio_campaign_farm_bureau_financial_services.sql
│   ├── v_akkio_attributes_latest.sql
│   ├── v_agg_akkio_hh.sql
│   └── v_agg_akkio_ind.sql
├── tests/                     # Singular SQL tests
├── packages.yml               # dbt package dependencies
├── dbt_project.yml            # Project configuration
└── VIZIO_MODELS_README.md     # Model documentation

```

## Models

### Fact Tables

1. **Content Consumption**
   - `vizio_daily_fact_content_summary` - Aggregated daily content viewing
   - `vizio_daily_fact_content_detail` - Granular content viewing sessions

2. **Commercial/Advertisement**
   - `vizio_daily_fact_commercial_summary` - Aggregated daily ad views
   - `vizio_daily_fact_commercial_detail` - Granular ad viewing sessions

3. **Device Activity**
   - `vizio_daily_fact_standard_summary` - Aggregated daily device activity
   - `vizio_daily_fact_standard_detail` - Granular device activity sessions

4. **Campaign Attribution**
   - `vizio_campaign_nothing_bundt_cakes` - Nothing Bundt Cakes campaign impression and market data
   - `vizio_campaign_farm_bureau_financial_services` - Farm Bureau Financial Services campaign data

### Dimension Tables

1. **Device & Household Attributes**
   - `v_akkio_attributes_latest` - Latest demographic attributes from Experian (decoded)
   - `v_agg_akkio_hh` - Household-level aggregation of demographic attributes
   - `v_agg_akkio_ind` - Individual-level aggregation with IP addresses from activity tables

## Development Workflow

## Resources

### dbt Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [dbt_utils package documentation](https://github.com/dbt-labs/dbt-utils)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions
- Find [dbt events](https://events.getdbt.com) near you

### Project Resources
- [VIZIO_MODELS_README.md](VIZIO_MODELS_README.md) - Detailed model documentation
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)