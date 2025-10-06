# dbt Testing Quick Reference

## Quick Commands

### Test Everything
```bash
dbt test
```

### Test One Model
```bash
dbt test --select vizio_daily_fact_content_summary
dbt test --select vizio_daily_fact_commercial_summary
dbt test --select vizio_campaign_attribution
```

### Test by Category
```bash
# Data existence tests
dbt test --select test_name:*has_data

# Aggregation integrity tests
dbt test --select test_name:*aggregation_integrity

# Temporal consistency tests
dbt test --select test_name:*temporal_consistency

# Only generic tests
dbt test --select test_type:generic

# Only singular tests
dbt test --select test_type:singular

# Only error-level tests
dbt test --select severity:error

# Only warning-level tests
dbt test --select severity:warn
```

### Test Model + Upstream Dependencies
```bash
dbt test --select vizio_daily_fact_content_summary+
```

### Test Model + Downstream Dependencies
```bash
dbt test --select +vizio_daily_fact_standard_detail
```

### Build and Test Together
```bash
dbt build --select vizio_daily_fact_content_summary
```

### Rerun Only Failed Tests
```bash
dbt test --select result:fail
```

## Test Development Workflow

### 1. First Time Setup
```bash
cd /Users/snuggehalli/Documents/Customer_Projects/Vizio/dbt-vizio-poc
dbt deps              # Install dbt_utils
dbt run --select vizio  # Build all models once
```

### 2. Fast Iteration on One Model
```bash
# Rebuild just one model
dbt run --select vizio_daily_fact_content_summary

# Test just that model
dbt test --select vizio_daily_fact_content_summary
```

### 3. Pre-Commit Check
```bash
# Quick smoke test (< 10 seconds)
dbt test --select test_name:*has_data

# Full test suite (< 10 minutes)
dbt test
```

## Test Files by Model

| Model | Test Files |
|-------|-----------|
| **content_summary** | `test_content_summary_has_data.sql`<br>`test_content_summary_aggregation_integrity.sql` |
| **content_detail** | `test_content_detail_has_data.sql`<br>`test_temporal_consistency_content.sql` |
| **commercial_summary** | `test_commercial_summary_has_data.sql`<br>`test_commercial_summary_aggregation_integrity.sql` |
| **commercial_detail** | `test_commercial_detail_has_data.sql`<br>`test_temporal_consistency_commercial.sql` |
| **standard_summary** | `test_standard_summary_has_data.sql`<br>`test_standard_summary_aggregation_integrity.sql` |
| **standard_detail** | `test_standard_detail_has_data.sql`<br>`test_temporal_consistency_standard.sql` |
| **campaign_attribution** | `test_campaign_attribution_has_data.sql`<br>`test_campaign_attribution_market_integrity.sql` |

## Common Test Patterns

### Check if Model Has Data
```sql
SELECT COUNT(*) FROM {{ ref('my_model') }}
HAVING COUNT(*) = 0
```

### Check Composite Key Uniqueness
```sql
SELECT key1, key2, COUNT(*)
FROM {{ ref('my_model') }}
GROUP BY key1, key2
HAVING COUNT(*) > 1
```

### Check Aggregation Accuracy
```sql
WITH detail_agg AS (
    SELECT key, SUM(metric) AS total
    FROM {{ ref('detail_table') }}
    GROUP BY key
),
summary AS (
    SELECT key, total_metric
    FROM {{ ref('summary_table') }}
)
SELECT *
FROM summary s
JOIN detail_agg d ON s.key = d.key
WHERE s.total_metric != d.total
```

### Check Temporal Consistency
```sql
SELECT *
FROM {{ ref('my_model') }}
WHERE end_time <= start_time
```

## Severity Levels

### Error (Blocks Deployment)
- Data existence tests
- Duplicate key tests
- Aggregation integrity tests
- Critical not_null tests

### Warn (Logged but Doesn't Block)
- Viewing duration reasonableness
- TV_ID consistency across tables
- Partition date alignment
- Array/string list consistency

## Debugging Failed Tests

### 1. See Which Tests Failed
```bash
dbt test --store-failures
```

### 2. Investigate Failed Test
```bash
# Look at compiled SQL
cat target/compiled/vizio_poc_databricks/tests/test_name.sql

# Run test manually to see results
dbt test --select test_name:test_content_summary_has_data --store-failures
```

### 3. Check Logs
```bash
cat logs/dbt.log
```

## Pro Tips

1. **Use `--select` to run subsets**: Much faster than running all tests
2. **Run `dbt deps` after pulling**: Ensures packages are up to date
3. **Check `--warn-error` in CI**: Treats warnings as errors in production
4. **Use `dbt build`**: Runs and tests in one command
5. **Parallelize with `--threads`**: Speed up test execution

## Environment-Specific Testing

### Development
```bash
dbt test --select severity:error
```

### Staging
```bash
dbt test --warn-error-options '{"include": "all"}'
```

### Production
```bash
dbt test --warn-error
```

---

For detailed explanations, see `TESTING_GUIDE.md`

