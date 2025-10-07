# Vizio DBT Models - Implementation Summary

## Overview
This implementation creates a comprehensive data modeling layer for Vizio TV viewing data, following the same detail/summary pattern as the LG Ads reference models. All models output to `akkio.vizio_poc` database/schema.

## Data Architecture

### 1. Content Viewing Models
**Source:** `production_r2079_content_with_null`

- **vizio_daily_fact_content_detail.sql**: Granular content viewing sessions with genre enrichment from mapping table
- **vizio_daily_fact_content_summary.sql**: Daily aggregation by TV_ID with array and string list fields

**Key Features:**
- Joins with `mk_akkio_genre_title_mapping` for genre enrichment
- Joins with `mk_akkio_tvtimezone_mapping` for timezone enrichment
- Standardizes text fields to lowercase with hyphens
- Captures viewing duration, input device, app service, and network details
- Filters out null TV_IDs and show titles
- Includes device timezone for local time conversion analysis

### 2. Commercial/Advertisement Models
**Source:** `production_r2080_commercialfeedmodular`

- **vizio_daily_fact_commercial_detail.sql**: Individual commercial views with surrounding content context
- **vizio_daily_fact_commercial_summary.sql**: Daily aggregation with commercial metrics

**Key Features:**
- Joins with `mk_commercialcategory_mapping` for commercial categorization
- Joins with `mk_akkio_tvtimezone_mapping` for timezone enrichment
- Captures previous and next content context (episode ID, title, network, callsign)
- Tracks brand names, ad titles, and creative IDs
- Includes ad length and total ad viewing metrics
- Includes device timezone for local time conversion analysis

### 3. Standard Device Activity Models
**Source:** `production_r2081_ipage`

- **vizio_daily_fact_standard_detail.sql**: Device activity sessions with location data
- **vizio_daily_fact_standard_summary.sql**: Daily aggregation with activity metrics

**Key Features:**
- Joins with `mk_akkio_tvtimezone_mapping` for timezone enrichment
- Tracks device location (city, state, DMA, zip code)
- Captures session start/end times and duration
- Provides first/last activity times and total session counts
- Includes device timezone for local time conversion analysis

### 4. Campaign Attribution Model
**Source:** `nothing_bundt_cakes_attr_data_akkio_poc` + `nothing_bundt_cakes_pop_data_akkio_poc`

- **vizio_campaign_attribution.sql**: Campaign impression tracking with market reach data

**Key Features:**
- Joins with `mk_akkio_tvtimezone_mapping` for timezone enrichment
- Links impressions to market TV population for reach analysis
- Captures show context during impression
- Tracks local vs national broadcast
- Includes session type and source information
- Includes device timezone for local time conversion analysis

### 5. Attributes Model (Placeholder)
- **vizio_attributes_latest.sql**: Left empty as requested (awaiting additional data source)

## Data Transformations

### Common Patterns Across All Models:
1. **Text Standardization**: `lower(replace(column, ' ', '-'))` for all text fields
2. **Partition Date**: Uses `date_partition` from source tables
3. **TV_ID**: Maps from `hash` field in source tables
4. **Timezone Enrichment**: All detail tables join with `mk_akkio_tvtimezone_mapping` to add device timezone
5. **Clustering**: All tables clustered by `(partition_date, tv_id)` for query performance
6. **Array Fields**: Summary tables use `collect_set()` for unique value arrays
7. **String Lists**: Pipe-delimited strings using `string_agg(DISTINCT column, '|')`
8. **Genre Handling**: Special handling for semicolon-separated genre values with array flattening

## Schema Documentation

The `schema.yml` file includes:
- **Source definitions** for all Delta Share tables from `vizio-poc-share.akkio`
- **Model descriptions** with short-name tags for Akkio integration
- **Column descriptions** with data type indicators (`:lower`, `:space-to-hyphen`, `:str-list-col-pregen`, `:akkio-context-ignore`)
- **Data tests** for primary key fields (not_null tests on PARTITION_DATE and TV_ID)

## Performance Considerations

### For 700GB+ Scale:
1. **Use Summary Tables First**: Always try summary tables before detail tables
2. **Clustering**: All tables clustered on partition_date and tv_id for efficient filtering
3. **Partition Pruning**: Filter on PARTITION_DATE for date range queries
4. **String Lists**: Use `_STR_LIST` columns (marked `:akkio-context-ignore`) for simple text matching
5. **Array Columns**: Use `_ARRAY` columns for set operations and complex queries

### Query Patterns:
- **Daily aggregations**: Use summary tables
- **Individual session analysis**: Use detail tables
- **Cross-table joins**: Join on (PARTITION_DATE, TV_ID)
- **Temporal analysis**: Use timestamp fields in detail tables

## Data Quality

### Built-in Filters:
- All models filter out NULL TV_IDs
- Content models filter out NULL show titles
- Commercial data includes all valid creative IDs
- Standard models include all device activity

### Data Tests:
- `not_null` tests on PARTITION_DATE and TV_ID for all models
- Tests can be run with: `dbt test --models vizio`

## Build Order

Dependency graph:
```
Sources (Delta Share)
  ↓
Detail Models (content, commercial, standard, attribution)
  ↓
Summary Models (content, commercial, standard)
```

Build with: `dbt build --models vizio`

## Campaign Attribution Use Cases

The Nothing Bundt Cakes attribution data can be analyzed against:
1. **Content detail/summary**: Match TV_IDs and dates to see what content viewers watched
2. **Commercial detail/summary**: Identify co-viewing patterns with other commercials
3. **Standard detail/summary**: Analyze device activity patterns of exposed viewers

Join pattern:
```sql
SELECT 
  ca.*,
  cs.*
FROM vizio_campaign_attribution ca
LEFT JOIN vizio_daily_fact_content_summary cs
  ON ca.tv_id = cs.tv_id 
  AND ca.partition_date = cs.partition_date
```

## Tags and Annotations

Models include Akkio-specific tags:
- `:short-name:` - Short descriptive names for model categories
- `:lower` - Indicates lowercase transformation applied
- `:space-to-hyphen` - Indicates space-to-hyphen transformation
- `:str-list-col-pregen` - Pre-generated string list columns for list operations
- `:akkio-context-ignore` - Fields to ignore for certain Akkio contexts
- `:all-unique-values` - Indicates all unique values should be indexed

## Next Steps

1. **Configure profiles.yml**: Ensure connection to Databricks with write access to `akkio.vizio_poc`
2. **Test source connections**: `dbt debug` to verify Delta Share access
3. **Run models**: `dbt run --models vizio`
4. **Validate data**: `dbt test --models vizio`
5. **Add attributes source**: Update `vizio_attributes_latest.sql` when location data source is available

## Notes

- All source tables reference `vizio-poc-share.akkio` via Delta Share
- All output tables write to `akkio.vizio_poc` schema
- Model materialization: All tables (not views) for performance at 700GB+ scale
- Schema evolution: Easy to add new columns or models following the established patterns

