# Vizio DBT Models - Implementation Summary

## Overview
This implementation creates a comprehensive data modeling layer for Vizio TV viewing data, following the same detail/summary pattern as the LG Ads reference models. All models output to `akkio.vizio_poc` database/schema.

### Model Inventory (11 Total Models)

**Fact Tables (5 models):**
- `vizio_daily_fact_content_detail` - Granular content viewing sessions
- `vizio_daily_fact_content_summary` - Daily aggregated content viewing
- `vizio_daily_fact_commercial_detail` - Granular commercial/ad views
- `vizio_daily_fact_commercial_summary` - Daily aggregated commercial/ad views
- `vizio_daily_fact_standard_detail` - Device activity sessions (aggregated to one row per device per day)

**Campaign Attribution Tables (2 models):**
- `vizio_campaign_nothing_bundt_cakes` - Nothing Bundt Cakes campaign impressions
- `vizio_campaign_farm_bureau_financial_services` - Farm Bureau Financial Services campaign impressions

**Demographic & Household Tables (4 models):**
- `v_akkio_attributes_latest` - Latest demographic attributes from Experian (decoded)
- `v_agg_akkio_hh` - Household-level demographic aggregation
- `v_agg_akkio_ind` - Individual-level demographic aggregation
- `v_agg_akkio_media` - Individual-level media viewing behavior aggregation

## Data Architecture

### 1. Content Viewing Models
**Source:** `production_r2079_content_with_null`

- **vizio_daily_fact_content_detail.sql**: Granular content viewing sessions with genre enrichment from mapping table
- **vizio_daily_fact_content_summary.sql**: Daily aggregation by TV_ID with array and string list fields

**Key Features:**
- Joins with `mk_akkio_genre_title_mapping` on episode_id for genre enrichment (LEFT JOIN)
- Joins with `mk_akkio_tvtimezone_mapping` on hash for timezone enrichment (LEFT JOIN)
- Standardizes text fields: lowercase with spaces replaced by hyphens (networks, titles, genres, callsigns)
- Input device names use underscores instead of hyphens
- Input categories converted to uppercase
- Filters out NULL device hashes and NULL show titles
- Minimum viewing duration filter: sessions must be > 10 seconds
- Calculates viewing duration: `DATEDIFF(SECOND, ts_start, ts_end)`
- Genre normalization: replaces ', ' with ',' before processing
- Includes device timezone for local time conversion analysis

### 2. Commercial/Advertisement Models
**Source:** `production_r2080_commercialfeedmodular`

- **vizio_daily_fact_commercial_detail.sql**: Individual commercial views with surrounding content context
- **vizio_daily_fact_commercial_summary.sql**: Daily aggregation with commercial metrics

**Key Features:**
- Joins with `mk_commercialcategory_mapping` on creative value for commercial categorization (LEFT JOIN)
- Joins with `mk_akkio_tvtimezone_mapping` on hash for timezone enrichment (LEFT JOIN)
- Filters out NULL device hashes and ads with duration <= 0
- Advanced text standardization for brand names and ad titles:
  - Removes special characters (quotes, apostrophes)
  - Standardizes separators (spaces, commas, colons, periods) to hyphens using regex
  - Converts to lowercase
- Commercial category normalization: normalizes forward slashes (e.g., "Food / Beverage" → "food/beverage")
- Captures previous and next content context (episode ID, title, network, callsign, timestamps)
- Tracks brand names, ad titles, and creative IDs
- Includes ad length and total ad viewing metrics
- Uses incremental materialization with merge strategy for efficient updates
- Includes device timezone for local time conversion analysis

### 3. Standard Device Activity Models
**Source:** `production_r2081_ipage`

- **vizio_daily_fact_standard_detail.sql**: Device activity sessions with location data
- **vizio_daily_fact_standard_summary.sql**: Daily aggregation with activity metrics

**Key Features:**
- Joins with `mk_akkio_tvtimezone_mapping` on hash for timezone enrichment (LEFT JOIN)
- Filters out NULL device hashes
- Deduplication logic: Uses window functions to aggregate multiple sessions per device per day
  - `ROW_NUMBER() OVER (PARTITION BY date_partition, hash ORDER BY ts_start)` to select first session
  - `SUM(DATEDIFF(SECOND, ts_start, ts_end)) OVER (PARTITION BY date_partition, hash)` to sum total seconds
  - Ensures one row per device per day
- Tracks device location (city, state, DMA, zip code)
- Captures total activity duration across all sessions for the day
- Includes device timezone for local time conversion analysis

### 4. Campaign Attribution Models
**Sources:** Campaign-specific attribution and population data tables

- **vizio_campaign_nothing_bundt_cakes.sql**: Nothing Bundt Cakes campaign impression tracking
- **vizio_campaign_farm_bureau_financial_services.sql**: Farm Bureau Financial Services campaign tracking

**Key Features:**
- Joins with market population tables on market name for reach calculations (LEFT JOIN)
- Joins with `mk_akkio_tvtimezone_mapping` on hashed_tvid for timezone enrichment (LEFT JOIN)
- Joins with `v_akkio_attributes_latest` on device ID to enrich with latest location data (city, state, DMA)
- Filters out NULL device identifiers (hashed_tvid)
- Text standardization: show titles, station call signs, channel affiliates converted to lowercase with spaces replaced by hyphens
- Show titles also remove special characters (quotes, commas, colons, periods) using regex
- Links impressions to market TV population for reach analysis
- Captures show context during impression (title, station, channel affiliate)
- Tracks local vs national broadcast
- Includes session type and source information
- Includes device timezone for local time conversion analysis

### 5. Demographic Attributes Models
**Source:** `mk_akkio_experian_demo` (Experian demographic data)

- **v_akkio_attributes_latest.sql**: Latest demographic and household attributes with decoded Experian fields
- **v_agg_akkio_hh.sql**: Household-level aggregation for audience targeting
- **v_agg_akkio_ind.sql**: Individual-level aggregation with IP addresses collected from activity tables

**Key Features - v_akkio_attributes_latest:**
- Decodes Experian demographic data from household-level counts into categorical columns
- Source data contains household counts (e.g., "2 males aged 25-34") not individual flags
- Each device hash represents one unique household (source already deduplicated)
- Joins with `production_r2081_ipage` to enrich with latest location data (city, state, DMA, zip)
  - Uses `ROW_NUMBER() OVER (PARTITION BY hash ORDER BY date_partition DESC)` to get most recent location
- Decoding logic:
  - **Gender**: Determined by presence of any male/female household members (M/F/NULL)
  - **Age**: First matching age range bucket found (18, 25, 35, 45, 55, 65)
  - **Ethnicity**: First matching ethnicity found in household composition
  - **Education**: Highest level found (Graduate > College > Some College > High School)
  - **Income**: First matching income bracket found (lower bound in thousands)
- Household composition flags: Home ownership (1/0/NULL), marital status, presence of children/babies
- Data quality indicators: LOW_QUALITY_FLAG, DEMO_INCOMPLETE_FLAG
- Geographic attributes: STATE, ZIP11 (11-digit padded), CITY, DMA_NAME from latest device location

**Key Features - v_agg_akkio_hh:**
- Household-level grain (AKKIO_HH_ID)
- Fixed weight value of 1 per requirements
- Home ownership (1/0/NULL), household income (in dollars: HOUSEHOLD_INCOME_K * 1000), and income bracket
- Presence of children indicator (1/0) derived from HAS_CHILDREN_0_18
- Clustered by (PARTITION_DATE, AKKIO_HH_ID)

**Key Features - v_agg_akkio_ind:**
- Individual-level grain (AKKIO_ID)
- Fixed weight value of 1 per requirements
- Demographics: Gender (with NULL handling → 'UNDETERMINED'), Age, Age Bucket
- Additional attributes: ETHNICITY_PREDICTION, EDUCATION, MARITAL_STATUS (all with NULL handling)
- State: NULL handling for empty strings → 'Unknown'
- Household-level attributes: HOMEOWNER, INCOME (in dollars), INCOME_BUCKET
- ZIP_CODE: Right 5 digits extracted from ZIP11
- NET_WORTH_BUCKET: Cast to string, defaults to 'Unknown' (not available in Vizio data)
- Placeholder fields for future enrichment: MAIDS, IPS, EMAILS, PHONES (currently 0)
- Clustered by (PARTITION_DATE, AKKIO_ID)

**Key Features - v_agg_akkio_media:**
- Individual-level grain (AKKIO_ID, PARTITION_DATE)
- Aggregates media viewing behavior into categorical maps
- Uses `MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v)))` to create category → session count maps
- Flattens comma-separated genres using `LATERAL VIEW EXPLODE(SPLIT(GENRE, ','))`
- Maps provided: TITLES_WATCHED, GENRES_WATCHED, NETWORKS_WATCHED, INPUT_DEVICES_WATCHED, APP_SERVICES_WATCHED
- Handles NULL values by converting to 'unknown' category
- Clustered by (PARTITION_DATE, AKKIO_ID)

## Data Transformations

### Common Patterns Across All Models:
1. **Text Standardization**: 
   - Most text fields: `lower(replace(column, ' ', '-'))`
   - Input device names: `lower(replace(column, ' ', '_'))`
   - Input categories: `upper(replace(column, ' ', '-'))`
   - Commercial brand/title: Advanced regex cleaning with special character removal
2. **Partition Date**: Uses `date_partition` from source tables, mapped to `PARTITION_DATE` in output
3. **TV_ID/AKKIO_ID**: Maps from `hash` field in source tables to `AKKIO_ID` in output (optimized - same value)
4. **Timezone Enrichment**: All detail tables join with `mk_akkio_tvtimezone_mapping` on hash to add device timezone (LEFT JOIN)
5. **Clustering**: All tables clustered by date and device ID for query performance (post-hook)
6. **String Lists**: Comma-delimited strings using `string_agg(DISTINCT column, ',')`
7. **Genre Handling**: Special handling for comma-separated genre values:
   - Content summary: `array_join(ARRAY_DISTINCT(flatten(ARRAY_AGG(SPLIT(GENRE, ',')))), ',')`
   - Genre normalization: replaces ', ' with ',' before processing
8. **NULL Filtering**: All models filter out NULL device identifiers
9. **Incremental Processing**: Detail tables use incremental materialization with merge strategy where applicable

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
- **All Models**: Filter out NULL device identifiers (hash/TV_ID/AKKIO_ID)
- **Content Models**: 
  - Filter out NULL show titles
  - Minimum viewing duration: sessions must be > 10 seconds
- **Commercial Models**: 
  - Filter out ads with duration <= 0
  - Includes all valid creative IDs (even without category mappings)
- **Standard Models**: 
  - Aggregates multiple sessions per device per day into one record
  - Sums total activity seconds across all sessions
- **Campaign Models**: Filter out NULL device identifiers (hashed_tvid)
- **Demographic Models**: Filter out NULL device hashes, uses latest location per device

### Data Tests:
- `not_null` tests on PARTITION_DATE and TV_ID for all models
- Tests can be run with: `dbt test --models vizio`

## Build Order

Dependency graph:
```
Sources (Delta Share)
  ↓
├─ Detail Models (content, commercial, standard)
│    ↓
├─ Summary Models (content, commercial, standard)
│
├─ Campaign Models (nothing_bundt_cakes, farm_bureau_financial_services)
│    └─ Depends on: v_akkio_attributes_latest (for location enrichment)
│
└─ Attributes Models
     └─ v_akkio_attributes_latest
          │    └─ Depends on: production_r2081_ipage (for location)
          ↓
     ├─ v_agg_akkio_hh (household aggregation)
     ├─ v_agg_akkio_ind (individual aggregation)
     └─ v_agg_akkio_media (media aggregation)
          └─ Depends on: vizio_daily_fact_content_detail
```

Build with: `dbt build --models vizio`

## Campaign Attribution Use Cases

The campaign attribution data (Nothing Bundt Cakes and Farm Bureau Financial Services) can be analyzed against:
1. **Content detail/summary**: Match TV_IDs and dates to see what content viewers watched
2. **Commercial detail/summary**: Identify co-viewing patterns with other commercials
3. **Standard detail/summary**: Analyze device activity patterns of exposed viewers
4. **Attributes & Aggregations**: Enrich campaign viewers with demographic and household attributes

Join pattern examples:
```sql
-- Campaign + Content viewing
SELECT 
  ca.*,
  cs.*
FROM vizio_campaign_nothing_bundt_cakes ca
LEFT JOIN vizio_daily_fact_content_summary cs
  ON ca.tv_id = cs.tv_id 
  AND ca.partition_date = cs.partition_date

-- Campaign + Demographics
SELECT 
  ca.*,
  attr.*,
  hh.*
FROM vizio_campaign_farm_bureau_financial_services ca
LEFT JOIN v_akkio_attributes_latest attr
  ON ca.akkio_id = attr.akkio_id
LEFT JOIN v_agg_akkio_hh hh
  ON ca.akkio_id = hh.akkio_hh_id
  AND ca.partition_date = hh.partition_date
```

## Demographic Enrichment Use Cases

The new demographic attributes models enable rich audience segmentation:

1. **Household Analysis**: Use `v_agg_akkio_hh` for household-level insights
   - Income bracket analysis
   - Home ownership patterns
   - Household composition

2. **Individual Analysis**: Use `v_agg_akkio_ind` for device/person-level insights
   - Age and gender demographics
   - IP address tracking across activities
   - Cross-device behavior (when enriched with MAIDs)

3. **Campaign + Demographics**: Join campaign data with attributes
   - Target audience verification
   - Lookalike audience building
   - Campaign effectiveness by demographic segment

4. **Content + Demographics**: Analyze viewing patterns by demographics
   - Genre preferences by age/gender
   - Viewing duration by household income
   - Network affinity by education level

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
5. **Explore demographic data**: Query `v_akkio_attributes_latest` to understand audience composition
6. **Build audience segments**: Use aggregation tables for targeting and campaign analysis

## Notes

- All source tables reference `vizio-poc-share.akkio` via Delta Share
- All output tables write to `akkio.vizio_poc` schema
- Model materialization: All tables (not views) for performance at 700GB+ scale
- Schema evolution: Easy to add new columns or models following the established patterns

