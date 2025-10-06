# dbt Vizio POC Testing Guide

This guide provides comprehensive instructions for testing dbt models without running a full `dbt run`. The testing framework is designed with Databricks engineering best practices in mind.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Test Architecture](#test-architecture)
4. [Running Tests](#running-tests)
5. [Test Categories](#test-categories)
6. [CI/CD Integration](#cicd-integration)
7. [Troubleshooting](#troubleshooting)

---

## Overview

The testing framework includes:

- **Generic Tests**: Schema-level tests defined in `schema.yml` for data quality checks
- **Singular Tests**: Custom SQL tests in the `tests/` directory for business logic validation
- **Custom Macros**: Reusable test logic in the `macros/` directory
- **Integration Tests**: Cross-model relationship and referential integrity checks

All tests are designed to run **independently** without requiring a full `dbt run`, assuming models have been built at least once.

---

## Prerequisites

### 1. Install dbt_utils Package

```bash
cd /Users/snuggehalli/Documents/Customer_Projects/Vizio/dbt-vizio-poc
dbt deps
```

This installs the `dbt_utils` package required for advanced generic tests.

### 2. Build Models (First Time Only)

If models haven't been built yet, run:

```bash
dbt run --select vizio
```

After the initial build, you can test without rebuilding using the commands below.

---

## Test Architecture

### Test Layers

```
┌─────────────────────────────────────────────────────────────┐
│ Layer 1: Generic Tests (schema.yml)                        │
│ - not_null, unique, accepted_values                        │
│ - relationships, expression_is_true                         │
│ - unique_combination_of_columns                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 2: Singular Data Quality Tests                       │
│ - Row count validation                                      │
│ - Aggregation integrity                                     │
│ - Temporal consistency                                      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 3: Business Logic Tests                              │
│ - Partition date consistency                                │
│ - Array/string list consistency                             │
│ - Viewing duration reasonableness                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ Layer 4: Integration Tests                                 │
│ - Cross-model TV_ID consistency                             │
│ - Market data integrity                                     │
│ - Composite key uniqueness                                  │
└─────────────────────────────────────────────────────────────┘
```

---

## Running Tests

### Test All Models

```bash
dbt test
```

### Test a Specific Model

```bash
# Test content summary model
dbt test --select vizio_daily_fact_content_summary

# Test commercial detail model
dbt test --select vizio_daily_fact_commerical_detail

# Test campaign attribution model
dbt test --select vizio_campaign_attribution
```

### Test by Tag

```bash
# Test all vizio models
dbt test --select tag:vizio
```

### Test with Warnings Treated as Errors

```bash
# Stricter testing for production deployments
dbt test --warn-error
```

### Run Only Failed Tests

```bash
# Useful for iterative debugging
dbt test --select result:fail
```

### Test Specific Test Files

```bash
# Test aggregation integrity
dbt test --select test_name:test_content_summary_aggregation_integrity

# Test temporal consistency
dbt test --select test_name:test_temporal_consistency_content
```

---

## Test Categories

### 1. Model Existence Tests

**Purpose**: Verify each model builds and contains data

**Files**:
- `test_content_summary_has_data.sql`
- `test_content_detail_has_data.sql`
- `test_commercial_summary_has_data.sql`
- `test_commercial_detail_has_data.sql`
- `test_standard_summary_has_data.sql`
- `test_standard_detail_has_data.sql`
- `test_campaign_attribution_has_data.sql`

**Run Command**:
```bash
dbt test --select test_name:*has_data
```

### 2. Aggregation Integrity Tests

**Purpose**: Ensure summary tables correctly aggregate detail tables

**Files**:
- `test_content_summary_aggregation_integrity.sql`
- `test_commercial_summary_aggregation_integrity.sql`
- `test_standard_summary_aggregation_integrity.sql`

**Run Command**:
```bash
dbt test --select test_name:*aggregation_integrity
```

**What They Check**:
- Total seconds match between summary and detail
- Row counts aggregate correctly
- First/last activity times are accurate
- No data loss during aggregation

### 3. Temporal Consistency Tests

**Purpose**: Validate timestamp logic and session durations

**Files**:
- `test_temporal_consistency_content.sql`
- `test_temporal_consistency_commercial.sql`
- `test_temporal_consistency_standard.sql`
- `test_session_time_calculation_accuracy.sql`

**Run Command**:
```bash
dbt test --select test_name:*temporal_consistency
```

**What They Check**:
- End times are after start times
- Duration calculations are accurate
- No negative durations
- Session times match calculated differences

### 4. Data Quality Tests

**Purpose**: Check for data anomalies and business rule violations

**Files**:
- `test_partition_date_consistency.sql`
- `test_reasonable_viewing_duration.sql`
- `test_array_str_list_consistency.sql`
- `test_no_duplicate_composite_keys.sql`

**Run Command**:
```bash
dbt test --select test_name:test_partition_date_consistency,test_name:test_reasonable_viewing_duration
```

**What They Check**:
- Partition dates align with event timestamps
- Viewing durations are within reasonable bounds (< 24 hours)
- Array and string list columns have consistent data
- No duplicate composite keys

### 5. Integration Tests

**Purpose**: Verify referential integrity and cross-model consistency

**Files**:
- `test_tv_id_consistency.sql`
- `test_campaign_attribution_market_integrity.sql`

**Run Command**:
```bash
dbt test --select test_name:*integrity,test_name:*consistency
```

**What They Check**:
- TV_IDs exist across related tables
- Market population data is valid
- No orphaned records

### 6. Generic Schema Tests

**Purpose**: Standard dbt tests for columns and relationships

**Location**: Defined in `models/vizio/schema.yml`

**What They Check**:
- `not_null`: Critical columns are never null
- `unique_combination_of_columns`: Composite keys are unique
- `accepted_values`: Categorical fields have valid values
- `relationships`: Foreign keys reference valid records
- `expression_is_true`: Numeric bounds and business rules

**Run Command**:
```bash
# Run only generic tests
dbt test --select test_type:generic

# Run only singular tests
dbt test --select test_type:singular
```

---

## CI/CD Integration

### Recommended CI Pipeline

```yaml
# Example GitHub Actions workflow
name: dbt Test Pipeline

on:
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          pip install dbt-databricks
          dbt deps
      
      - name: Run critical tests only
        run: |
          dbt test --select test_type:generic severity:error
          dbt test --select test_name:*has_data
          dbt test --select test_name:*aggregation_integrity
          dbt test --select test_name:test_no_duplicate_composite_keys
      
      - name: Run warning-level tests
        run: |
          dbt test --select severity:warn
        continue-on-error: true
```

### Test Execution Strategy

For fast feedback loops, run tests in this order:

1. **Smoke Tests** (< 10 seconds)
   ```bash
   dbt test --select test_name:*has_data
   ```

2. **Critical Tests** (< 1 minute)
   ```bash
   dbt test --select severity:error test_type:generic
   ```

3. **Aggregation Tests** (< 5 minutes)
   ```bash
   dbt test --select test_name:*aggregation_integrity
   ```

4. **Full Test Suite** (< 10 minutes)
   ```bash
   dbt test
   ```

---

## Testing Individual Models During Development

### Example: Testing Content Summary Model

```bash
# Step 1: Build just this model
dbt run --select vizio_daily_fact_content_summary

# Step 2: Run all tests for this model
dbt test --select vizio_daily_fact_content_summary

# Step 3: Check specific test
dbt test --select test_name:test_content_summary_has_data

# Step 4: Verify aggregation integrity
dbt test --select test_name:test_content_summary_aggregation_integrity
```

### Example: Testing Campaign Attribution Model

```bash
# Build and test in one command
dbt build --select vizio_campaign_attribution

# Or separately
dbt run --select vizio_campaign_attribution
dbt test --select vizio_campaign_attribution
```

---

## Troubleshooting

### Test Failures

#### "dbt_utils not found"

**Solution**:
```bash
dbt deps
```

#### "Compilation Error: relation does not exist"

**Cause**: Model hasn't been built yet

**Solution**:
```bash
dbt run --select <model_name>
dbt test --select <model_name>
```

#### Tests Pass Locally but Fail in CI

**Cause**: Stale data or different environment configurations

**Solution**:
1. Check `profiles.yml` for environment differences
2. Ensure CI has access to the same data sources
3. Verify partition filters aren't excluding data in CI

### Performance Issues

If tests are running slowly:

1. **Add partition filters** to test queries:
   ```sql
   WHERE PARTITION_DATE >= CURRENT_DATE - INTERVAL 7 DAYS
   ```

2. **Limit result sets** in warning-level tests:
   ```sql
   LIMIT 100
   ```

3. **Run tests in parallel** (dbt Cloud or dbt Core with threads):
   ```bash
   dbt test --threads 4
   ```

### Viewing Test Results

```bash
# See test results in terminal
dbt test --store-failures

# Generate documentation with test results
dbt docs generate
dbt docs serve
```

---

## Test Coverage by Model

| Model | Generic Tests | Singular Tests | Integration Tests | Total |
|-------|--------------|----------------|-------------------|-------|
| vizio_daily_fact_content_summary | 7 | 2 | 3 | 12 |
| vizio_daily_fact_content_detail | 5 | 2 | 2 | 9 |
| vizio_daily_fact_commercial_summary | 8 | 2 | 2 | 12 |
| vizio_daily_fact_commerical_detail | 6 | 2 | 2 | 10 |
| vizio_daily_fact_standard_summary | 7 | 2 | 2 | 11 |
| vizio_daily_fact_standard_detail | 5 | 2 | 2 | 9 |
| vizio_campaign_attribution | 6 | 2 | 1 | 9 |

**Total Test Count: 72+**

---

## Best Practices

### 1. Run Tests Before Committing

```bash
# Quick smoke test
dbt test --select test_name:*has_data,test_name:test_no_duplicate_composite_keys

# Full validation
dbt test
```

### 2. Use Appropriate Severity Levels

- `error`: Data quality issues that break downstream processes
- `warn`: Data anomalies that should be investigated but don't block deployment

### 3. Monitor Test Performance

```bash
# See test execution times
dbt test --log-level debug
```

### 4. Document Test Failures

When tests fail, document:
- Which test failed
- The root cause
- Whether it's a data issue or test issue
- Resolution steps

### 5. Keep Tests Fast

- Use `LIMIT` clauses for exploratory tests
- Add partition filters where appropriate
- Archive old test results

---

## Additional Resources

- [dbt Testing Documentation](https://docs.getdbt.com/docs/building-a-dbt-project/tests)
- [dbt_utils Package](https://github.com/dbt-labs/dbt-utils)
- [Databricks SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)

---

## Contact

For questions or issues with the testing framework, contact your dbt/Databricks engineering team.

---

**Last Updated**: October 6, 2025

