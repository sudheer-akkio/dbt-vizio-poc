# Singular Tests Directory

This directory contains custom SQL-based tests that validate the Vizio dbt models. These tests complement the generic tests defined in `schema.yml`.

## Test Organization

### 1. Data Existence Tests
Tests that verify models contain data and were built successfully.

- `test_content_summary_has_data.sql`
- `test_content_detail_has_data.sql`
- `test_commercial_summary_has_data.sql`
- `test_commercial_detail_has_data.sql`
- `test_standard_summary_has_data.sql`
- `test_standard_detail_has_data.sql`
- `test_campaign_attribution_has_data.sql`

**Run**: `dbt test --select test_name:*has_data`

### 2. Aggregation Integrity Tests
Tests that verify summary tables correctly aggregate from detail tables.

- `test_content_summary_aggregation_integrity.sql`
- `test_commercial_summary_aggregation_integrity.sql`
- `test_standard_summary_aggregation_integrity.sql`

**Run**: `dbt test --select test_name:*aggregation_integrity`

### 3. Temporal Consistency Tests
Tests that validate timestamp logic and duration calculations.

- `test_temporal_consistency_content.sql`
- `test_temporal_consistency_commercial.sql`
- `test_temporal_consistency_standard.sql`
- `test_session_time_calculation_accuracy.sql`

**Run**: `dbt test --select test_name:*temporal_consistency`

### 4. Data Quality Tests
Tests that check for data anomalies and business rule violations.

- `test_partition_date_consistency.sql` - Ensures partition dates align with event timestamps
- `test_reasonable_viewing_duration.sql` - Flags viewing sessions > 24 hours
- `test_array_str_list_consistency.sql` - Validates array vs string list transformations
- `test_no_duplicate_composite_keys.sql` - Ensures grain integrity

### 5. Integration Tests
Tests that verify cross-model consistency and referential integrity.

- `test_tv_id_consistency.sql` - Checks TV_IDs exist across related tables
- `test_campaign_attribution_market_integrity.sql` - Validates market population data

## Writing New Tests

### Test Structure

All singular tests should follow this pattern:

```sql
-- Clear description of what this test validates
-- Explain the business logic or rule being tested

{{ config(severity='error') }}  -- or 'warn'

SELECT 
    -- Columns identifying the problematic rows
FROM {{ ref('model_name') }}
WHERE -- Condition that should NOT be true
```

**Key Points**:
- Tests PASS when they return 0 rows
- Tests FAIL when they return > 0 rows
- Each returned row represents a failure case

### Severity Levels

**`error`**: Critical issues that should block deployment
- Data doesn't exist
- Duplicate keys
- Broken aggregations
- Missing required fields

**`warn`**: Issues to investigate but don't block deployment
- Data anomalies (e.g., unusually long viewing times)
- Inconsistencies across tables
- Missing optional fields

### Example: Creating a New Test

```sql
-- Test that all commercial ads have a valid creative_id
-- Ensures we can track ads back to campaigns

{{ config(severity='error') }}

SELECT 
    PARTITION_DATE,
    TV_ID,
    AD_MATCH_START_TIME_UTC
FROM {{ ref('vizio_daily_fact_commerical_detail') }}
WHERE CREATIVE_ID IS NULL
   OR CREATIVE_ID = ''
```

Save as `test_commercial_has_creative_id.sql` and run:

```bash
dbt test --select test_name:test_commercial_has_creative_id
```

## Test Performance

### Optimization Tips

1. **Add partition filters** for large tables:
   ```sql
   WHERE PARTITION_DATE >= CURRENT_DATE - INTERVAL 30 DAYS
   ```

2. **Limit results** for warning-level tests:
   ```sql
   LIMIT 100  -- Prevent overwhelming results
   ```

3. **Use EXISTS** instead of JOIN when possible:
   ```sql
   WHERE NOT EXISTS (
       SELECT 1 FROM {{ ref('other_table') }} o
       WHERE o.id = t.id
   )
   ```

## Test Maintenance

### When Models Change

If you add/modify a model:

1. **Update related tests** to reflect schema changes
2. **Add new tests** for new business logic
3. **Run tests locally** before committing:
   ```bash
   dbt test --select vizio_your_new_model
   ```

### When Tests Fail

1. **Investigate the failure**:
   ```bash
   dbt test --select test_name:failing_test --store-failures
   ```

2. **Check the compiled SQL**:
   ```bash
   cat target/compiled/vizio_poc_databricks/tests/failing_test.sql
   ```

3. **Determine if it's**:
   - A data quality issue → Fix upstream data
   - A model bug → Fix the model
   - A test bug → Fix the test

4. **Document findings** in ticket/PR

## Test Coverage Goals

- ✅ **100%** of models have data existence tests
- ✅ **100%** of summary models have aggregation integrity tests
- ✅ **100%** of models with timestamps have temporal consistency tests
- ✅ **All composite keys** tested for uniqueness
- ✅ **Critical foreign keys** tested with relationship tests

## Running Tests in CI/CD

### GitHub Actions Example

```yaml
- name: Run Tests
  run: |
    # Critical tests that block deployment
    dbt test --select severity:error
    
    # Warning tests (logged but don't block)
    dbt test --select severity:warn --warn-error-options '{"include": []}'
```

### Pre-commit Hook Example

```bash
#!/bin/bash
# .git/hooks/pre-commit

echo "Running dbt tests..."
dbt test --select test_name:*has_data

if [ $? -ne 0 ]; then
    echo "Tests failed! Please fix before committing."
    exit 1
fi
```

## Useful Test Queries

### See All Tests
```bash
dbt list --resource-type test
```

### See Tests for One Model
```bash
dbt list --resource-type test --select vizio_daily_fact_content_summary
```

### Generate Test Documentation
```bash
dbt docs generate
dbt docs serve
```

Open http://localhost:8080 and click on tests to see their definitions and results.

---

For more detailed information, see:
- `../TESTING_GUIDE.md` - Comprehensive testing documentation
- `../TEST_QUICK_REFERENCE.md` - Quick command reference
- `../models/vizio/schema.yml` - Generic test definitions

