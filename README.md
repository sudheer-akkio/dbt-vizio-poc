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

# Build and test together
dbt build --select vizio
```

## Testing Framework

This project includes a comprehensive testing framework designed with Databricks engineering best practices.

### Quick Test Commands

```bash
# Test everything
dbt test

# Test one model
dbt test --select vizio_daily_fact_content_summary

# Test by category
dbt test --select test_name:*has_data              # Data existence
dbt test --select test_name:*aggregation_integrity # Aggregation accuracy
dbt test --select test_name:*temporal_consistency  # Timestamp validation

# Run only critical tests
dbt test --select severity:error
```

### Test Documentation

- **[TESTING_GUIDE.md](TESTING_GUIDE.md)** - Comprehensive testing guide with architecture, best practices, and CI/CD integration
- **[TEST_QUICK_REFERENCE.md](TEST_QUICK_REFERENCE.md)** - Quick command reference for common testing scenarios
- **[tests/README.md](tests/README.md)** - Details on singular test organization and writing new tests

### Test Coverage

- ✅ **72+ tests** across 7 models
- ✅ Generic schema tests (not_null, unique, relationships, etc.)
- ✅ Aggregation integrity tests (summary vs detail validation)
- ✅ Temporal consistency tests (timestamp logic validation)
- ✅ Business logic tests (viewing duration, partition alignment)
- ✅ Integration tests (cross-model referential integrity)

### Why Use Tests Instead of `dbt run`?

Tests allow you to:
- ✅ Validate model correctness without rebuilding
- ✅ Run faster (seconds vs minutes)
- ✅ Test specific models in isolation
- ✅ Catch data quality issues early
- ✅ Integrate easily into CI/CD pipelines

## Project Structure

```
dbt-vizio-poc/
├── models/vizio/              # Vizio data models
│   ├── schema.yml             # Model documentation + generic tests
│   ├── vizio_daily_fact_content_summary.sql
│   ├── vizio_daily_fact_content_detail.sql
│   ├── vizio_daily_fact_commercial_summary.sql
│   ├── vizio_daily_fact_commerical_detail.sql
│   ├── vizio_daily_fact_standard_summary.sql
│   ├── vizio_daily_fact_standard_detail.sql
│   ├── vizio_campaign_attribution.sql
│   └── vizio_attributes_latest.sql
├── tests/                     # Singular SQL tests
│   ├── README.md              # Test organization guide
│   ├── test_*_has_data.sql    # Data existence tests
│   ├── test_*_aggregation_integrity.sql  # Summary validation tests
│   ├── test_*_temporal_consistency.sql   # Timestamp tests
│   └── test_*.sql             # Business logic tests
├── macros/                    # Custom test macros
│   ├── test_row_count_threshold.sql
│   └── test_recent_data.sql
├── packages.yml               # dbt package dependencies
├── dbt_project.yml            # Project configuration
├── TESTING_GUIDE.md           # Comprehensive testing documentation
├── TEST_QUICK_REFERENCE.md    # Quick test commands
└── VIZIO_MODELS_README.md     # Model documentation

```

## Models

### Fact Tables

1. **Content Consumption**
   - `vizio_daily_fact_content_summary` - Aggregated daily content viewing
   - `vizio_daily_fact_content_detail` - Granular content viewing sessions

2. **Commercial/Advertisement**
   - `vizio_daily_fact_commercial_summary` - Aggregated daily ad views
   - `vizio_daily_fact_commerical_detail` - Granular ad viewing sessions

3. **Device Activity**
   - `vizio_daily_fact_standard_summary` - Aggregated daily device activity
   - `vizio_daily_fact_standard_detail` - Granular device activity sessions

4. **Campaign Attribution**
   - `vizio_campaign_attribution` - Campaign impression and market data

### Dimension Tables

1. **Device Attributes**
   - `vizio_attributes_latest` - Latest device attributes (placeholder)

## Development Workflow

### Working on a Model

```bash
# 1. Make changes to model
vim models/vizio/vizio_daily_fact_content_summary.sql

# 2. Build just that model
dbt run --select vizio_daily_fact_content_summary

# 3. Test the model
dbt test --select vizio_daily_fact_content_summary

# 4. Build and test together
dbt build --select vizio_daily_fact_content_summary
```

### Before Committing

```bash
# Quick validation
dbt test --select test_name:*has_data

# Full test suite
dbt test

# Generate documentation
dbt docs generate
```

## Resources

### dbt Resources
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- [dbt_utils package documentation](https://github.com/dbt-labs/dbt-utils)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions
- Find [dbt events](https://events.getdbt.com) near you

### Project Resources
- [VIZIO_MODELS_README.md](VIZIO_MODELS_README.md) - Detailed model documentation
- [TESTING_GUIDE.md](TESTING_GUIDE.md) - Testing framework guide
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)