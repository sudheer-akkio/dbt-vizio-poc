# Vizio DBT Models - Internal Technical Documentation

## Overview
This implementation creates a comprehensive data modeling layer for Vizio TV viewing data, following a detail/summary pattern for performance optimization. All models output to `akkio.vizio_poc` database/schema and are built using dbt on Databricks.

**Scale:** Processing 700GB+ of daily viewing data across 11 analytical models.

### Model Inventory (11 Total Models)

**Fact Tables (5 models):**
- `vizio_daily_fact_content_detail` - Granular content viewing sessions
- `vizio_daily_fact_content_summary` - Daily aggregated content viewing
- `vizio_daily_fact_commercial_detail` - Granular commercial/ad views
- `vizio_daily_fact_commercial_summary` - Daily aggregated commercial/ad views
- `vizio_daily_fact_standard_detail` - Granular device activity sessions (aggregated to one row per device per day)

**Campaign Attribution Tables (2 models):**
- `vizio_campaign_nothing_bundt_cakes` - Nothing Bundt Cakes campaign impressions
- `vizio_campaign_farm_bureau_financial_services` - Farm Bureau Financial Services campaign impressions

**Demographic & Household Tables (4 models):**
- `v_akkio_attributes_latest` - Latest demographic attributes from Experian (decoded)
- `v_agg_akkio_hh` - Household-level demographic aggregation
- `v_agg_akkio_ind` - Individual-level demographic aggregation
- `v_agg_akkio_media` - Individual-level media viewing behavior aggregation

---

## Data Architecture & Lineage

### Complete Lineage Diagram

```
SOURCE LAYER (Delta Share: vizio-poc-share.akkio)
══════════════════════════════════════════════════
┌─────────────────────────────────────────────────────────────────────┐
│ Core Activity Sources (Green in Diagram)                            │
├─────────────────────────────────────────────────────────────────────┤
│ • production_r2079_content_with_null (Content viewing)              │
│ • production_r2080_commercialfeedmodular (Commercials)              │
│ • production_r2081_ipage (Device activity)                          │
│ • production_r2067_optout (Opt-out tracking)                        │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Mapping/Reference Tables (Green in Diagram)                         │
├─────────────────────────────────────────────────────────────────────┤
│ • mk_akkio_genre_title_mapping (Episode → Genre)                    │
│ • mk_akkio_tvtimezone_mapping (Hash → Timezone)                     │
│ • mk_commercialcategory_mapping (Creative → Category)               │
│ • mk_akkio_experian_demo (Demographic attributes)                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Campaign Sources (Green in Diagram)                                 │
├─────────────────────────────────────────────────────────────────────┤
│ • nothing_bundt_cakes_attr_data_akkio_poc (Attribution)             │
│ • nothing_bundt_cakes_pop_data_akkio_poc (Population)               │
│ • farm_bureau_financial_services_attr_data_akkio_poc (Attribution)  │
│ • farm_bureau_financial_services_pop_data_akkio_poc (Population)    │
└─────────────────────────────────────────────────────────────────────┘

                              ↓↓↓

TRANSFORMATION LAYER (dbt Models: akkio.vizio_poc)
══════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────┐
│ CONTENT VIEWING PIPELINE                                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  production_r2079_content_with_null                                 │
│           +                                                          │
│  mk_akkio_genre_title_mapping                                       │
│           +                                                          │
│  mk_akkio_tvtimezone_mapping                                        │
│           ↓                                                          │
│  vizio_daily_fact_content_detail (Teal)                             │
│           ↓                                                          │
│  vizio_daily_fact_content_summary (Blue)                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ COMMERCIAL/AD PIPELINE                                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  production_r2080_commercialfeedmodular                             │
│           +                                                          │
│  mk_commercialcategory_mapping                                      │
│           +                                                          │
│  mk_akkio_tvtimezone_mapping                                        │
│           ↓                                                          │
│  vizio_daily_fact_commercial_detail (Teal)                          │
│           ↓                                                          │
│  vizio_daily_fact_commercial_summary (Blue)                         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ DEVICE ACTIVITY PIPELINE                                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  production_r2081_ipage                                             │
│           +                                                          │
│  mk_akkio_tvtimezone_mapping                                        │
│           ↓                                                          │
│  vizio_daily_fact_standard_detail (Teal)                            │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ CAMPAIGN ATTRIBUTION PIPELINES                                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  nothing_bundt_cakes_attr_data_akkio_poc                            │
│           +                                                          │
│  nothing_bundt_cakes_pop_data_akkio_poc                             │
│           +                                                          │
│  mk_akkio_tvtimezone_mapping                                        │
│           ↓                                                          │
│  vizio_campaign_nothing_bundt_cakes (Blue)                          │
│                                                                      │
│  farm_bureau_financial_services_attr_data_akkio_poc                 │
│           +                                                          │
│  farm_bureau_financial_services_pop_data_akkio_poc                  │
│           +                                                          │
│  mk_akkio_tvtimezone_mapping                                        │
│           ↓                                                          │
│  vizio_campaign_farm_bureau_financial_services (Blue)               │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ DEMOGRAPHIC ENRICHMENT PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  mk_akkio_experian_demo                                             │
│           +                                                          │
│  production_r2081_ipage (for location data)                         │
│           ↓                                                          │
│  v_akkio_attributes_latest (Teal)                                   │
│           ↓                                                          │
│           ├─────────────────┬─────────────────┐                     │
│           ↓                 ↓                 ↓                     │
│    v_agg_akkio_hh    v_agg_akkio_ind   (uses IPs from             │
│        (Blue)            (Blue)         content/commercial/        │
│                                         standard detail tables)    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Dependency Graph

```
Sources (Delta Share)
  ↓
├─ Content Pipeline
│   └─ production_r2079_content_with_null
│       + mk_akkio_genre_title_mapping
│       + mk_akkio_tvtimezone_mapping
│       ↓
│   └─ vizio_daily_fact_content_detail (incremental)
│       ↓
│   └─ vizio_daily_fact_content_summary
│
├─ Commercial Pipeline
│   └─ production_r2080_commercialfeedmodular
│       + mk_commercialcategory_mapping
│       + mk_akkio_tvtimezone_mapping
│       ↓
│   └─ vizio_daily_fact_commercial_detail (incremental)
│       ↓
│   └─ vizio_daily_fact_commercial_summary
│
├─ Device Activity Pipeline
│   └─ production_r2081_ipage
│       + mk_akkio_tvtimezone_mapping
│       ↓
│   └─ vizio_daily_fact_standard_detail (incremental)
│
├─ Campaign Pipelines
│   └─ vizio_campaign_nothing_bundt_cakes
│       (sources: attribution + population + timezone mapping)
│   └─ vizio_campaign_farm_bureau_financial_services
│       (sources: attribution + population + timezone mapping)
│
└─ Demographic Pipeline
    └─ mk_akkio_experian_demo + production_r2081_ipage
        ↓
    └─ v_akkio_attributes_latest (table)
        ↓
        ├─ v_agg_akkio_hh (table)
        ├─ v_agg_akkio_ind (table)
        └─ v_agg_akkio_media (table, depends on vizio_daily_fact_content_detail)
```

---

## Model Technical Specifications

### 1. Content Viewing Models

#### Source Tables
- **Primary:** `production_r2079_content_with_null`
- **Enrichment:** `mk_akkio_genre_title_mapping`, `mk_akkio_tvtimezone_mapping`

#### vizio_daily_fact_content_detail

**Configuration:**
```yaml
materialized: incremental
clustering: (viewed_date, akkio_id)
```

**Incremental Logic:**
```sql
-- Batch mode with variables
WHERE date_partition BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'

-- Normal incremental mode
WHERE date_partition > (SELECT MAX(PARTITION_DATE) FROM {{ this }})
```

**Note:** Uses incremental materialization with merge strategy for efficient updates.

**Transformations:**
1. **Join Logic:**
   - LEFT JOIN on episode_id for genre enrichment
   - LEFT JOIN on hash for timezone enrichment

2. **Text Standardization:**
   - Network, callsign, title, genre: `lower(replace(column, ' ', '-'))`
   - Input device: `lower(replace(INPUT_DEVICE_NAME, ' ', '_'))`
   - Input category: `upper(replace(INPUT_CATEGORY, ' ', '-'))`
   - Commercial brand/title: Advanced regex cleaning: `lower(regexp_replace(replace(column, '''’', ''), '[\\s,_/:;|.-]+', '-'))`
   - Commercial category: Normalizes forward slashes: `lower(regexp_replace(replace(cc.commercial_category, ' / ', '/'), '[\\s,-]+', '-'))`

3. **Duration Calculation:**
   - `DATEDIFF(SECOND, ts_start, ts_end) AS TOTAL_SECONDS`
   - Filter: `TOTAL_SECONDS > 10` (minimum 10 second viewing)

4. **Genre Processing:**
   - Replace ', ' with ',' to standardize comma-separated genres

**Data Filters:**
- `WHERE show_title IS NOT NULL` - Excludes sessions without content identification
- `WHERE hash IS NOT NULL` - Excludes records without device identifiers
- `WHERE TOTAL_SECONDS > 10` - Minimum viewing duration threshold (filters out channel surfing)

**Key Mapping:**
- `hash` → `TV_ID` → `AKKIO_ID`
- `date_partition` → `PARTITION_DATE` → `VIEWED_DATE`

#### vizio_daily_fact_content_summary

**Configuration:**
```yaml
materialized: table
clustering: (viewed_date, akkio_id)
```

**Aggregation Logic:**
```sql
GROUP BY 
    PARTITION_DATE,
    VIEWED_DATE,
    AKKIO_ID,
    TIMEZONE,
    ZIP_CODE,
    DMA
```

**Aggregation Patterns:**
1. **String Lists (CSV):**
   - `string_agg(DISTINCT column, ',')` for text values
   - Used for: networks, callsigns, titles, genres, input categories, etc.

2. **Numeric Aggregation:**
   - `SUM(TOTAL_SECONDS) AS TOTAL_VIEWING_SECONDS`

**Special Handling:**
- Genre deduplication: Uses `array_join(ARRAY_DISTINCT(flatten(ARRAY_AGG(SPLIT(GENRE, ',')))), ',')` to flatten comma-separated genres, remove duplicates, and re-aggregate
- Genre normalization: Replaces ', ' with ',' in source data before processing

---

### 2. Commercial/Advertisement Models

#### Source Tables
- **Primary:** `production_r2080_commercialfeedmodular`
- **Enrichment:** `mk_commercialcategory_mapping`, `mk_akkio_tvtimezone_mapping`

#### vizio_daily_fact_commercial_detail

**Configuration:**
```yaml
materialized: incremental
incremental_strategy: merge
unique_key: [PARTITION_DATE, AKKIO_ID]
partition_by: PARTITION_DATE
```

**Incremental Logic:**
Same pattern as content detail (batch mode with vars or normal incremental). Uses merge strategy for efficient upserts.

**Transformations:**
1. **Context Enrichment:**
   - Captures previous content context (PREV_EPISODE_ID, PREV_TITLE, PREV_NETWORK, etc.)
   - Captures next content context (NEXT_EPISODE_ID, NEXT_TITLE, NEXT_NETWORK, etc.)
   - Enables analysis of what content surrounded each ad

2. **Commercial Categorization:**
   - LEFT JOIN on `mk_commercialcategory_mapping` using creative value
   - Maps creative IDs to commercial categories

3. **Text Standardization:**
   - Same pattern as content models
   - Brand, ad title, category: `lower(replace(column, ' ', '-'))`

**Key Fields:**
- `CREATIVE_ID`: Unique ad identifier
- `AD_MATCH_START_TIME_UTC`, `AD_MATCH_END_TIME_UTC`: Ad timing
- `AD_LENGTH`: Ad duration in seconds

#### vizio_daily_fact_commercial_summary

**Configuration:**
```yaml
materialized: table
clustering: (viewed_date, akkio_id)
```

**Aggregation Logic:**
```sql
GROUP BY 
    PARTITION_DATE,
    VIEWED_DATE,
    AKKIO_ID,
    TIMEZONE,
    ZIP_CODE,
    DMA
```

**Aggregation Metrics:**
- `COUNT(*) AS TOTAL_AD_VIEWS`
- `SUM(AD_LENGTH) AS TOTAL_AD_SECONDS`
- String lists for: creative IDs, brand names, ad titles, categories, input devices

---

### 3. Device Activity Model

#### Source Tables
- **Primary:** `production_r2081_ipage`
- **Enrichment:** `mk_akkio_tvtimezone_mapping`

#### vizio_daily_fact_standard_detail

**Configuration:**
```yaml
materialized: incremental
clustering: (activity_date, akkio_id)
```

**Transformations:**
1. **Deduplication Logic:**
   - Uses `ROW_NUMBER() OVER (PARTITION BY date_partition, hash ORDER BY ts_start)` to select first session per device per day
   - Aggregates total activity seconds across all sessions: `SUM(DATEDIFF(SECOND, ts_start, ts_end)) OVER (PARTITION BY date_partition, hash)`
   - Ensures one row per device per day

2. **Location Tracking:**
   - Captures: city, state (iso_state), DMA, zip code
   - Used for geographic analysis and location validation

3. **Activity Duration:**
   - `all_total_seconds` - Sum of all session durations for the day

4. **Timezone Enrichment:**
   - Same pattern as other models via `mk_akkio_tvtimezone_mapping`

**Use Cases:**
- Device presence verification
- Geographic distribution analysis
- Location data for other table enrichment

---

### 4. Campaign Attribution Models

#### vizio_campaign_nothing_bundt_cakes

**Source Tables:**
- **Attribution:** `nothing_bundt_cakes_attr_data_akkio_poc`
- **Population:** `nothing_bundt_cakes_pop_data_akkio_poc`
- **Enrichment:** `mk_akkio_tvtimezone_mapping`

**Configuration:**
```yaml
materialized: table
clustering: (impression_date, akkio_id)
```

**Join Logic:**
```sql
FROM attribution_data attr
LEFT JOIN population_data pop
    ON attr.market = pop.market
LEFT JOIN timezone_mapping tz
    ON attr.hashed_tvid = tz.hash
LEFT JOIN v_akkio_attributes_latest st
    ON st.akkio_id = attr.hashed_tvid
```

**Key Features:**
- Links impression to market TV population for reach calculations (joined by market name)
- Enriches with latest device location (city, state, DMA) from attributes table
- Captures show context during impression (title, station, channel)
- Tracks local vs national broadcast
- Includes session type and source

#### vizio_campaign_farm_bureau_financial_services

**Same structure as Nothing Bundt Cakes with additions:**
- `MARKET_UE_POPULATION`: Additional UE population metric from same population table
- Joins same population table which contains both `inscape_tv_population` and `ue_population` columns

---

### 5. Demographic Attributes Models

#### v_akkio_attributes_latest

**Source Tables:**
- **Primary:** `mk_akkio_experian_demo`
- **Location:** `production_r2081_ipage` (for city, state, DMA, zip)

**Configuration:**
```yaml
materialized: table
clustering: (PARTITION_DATE, TV_ID)
```

**Data Structure Notes:**
- Source contains HOUSEHOLD-LEVEL counts (not individual binary flags)
- Example: `demo_male_25_34 = 2` means 2 males aged 25-34 in household
- Each HASH represents one unique household
- Source is already deduplicated at HASH level

**Decoding Logic:**

1. **Gender (M/F):**
```sql
CASE
    WHEN SUM(all_male_age_columns) > 0 THEN 'M'
    WHEN SUM(all_female_age_columns) > 0 THEN 'F'
    ELSE NULL
END
```

2. **Age (Integer):**
```sql
-- Returns lower bound of age range (18, 25, 35, 45, 55, 65)
CASE
    WHEN demo_male_18_24 >= 1 OR demo_female_18_24 >= 1 THEN 18
    WHEN demo_male_25_34 >= 1 OR demo_female_25_34 >= 1 THEN 25
    ...
END
```

3. **Age Bucket (Integer code):**
```sql
-- 1: 18-24, 2: 25-34, 3: 35-44, 4: 45-54, 5: 55-64, 6: 65-74, 7: 75+
```

4. **Education Level (Categorical):**
```sql
CASE 
    WHEN edu_graduate >= 1 THEN 'Graduate'
    WHEN edu_college >= 1 THEN 'College'
    WHEN edu_some_college >= 1 THEN 'Some College'
    WHEN edu_high_school >= 1 THEN 'High School'
END
```

5. **Ethnicity (Categorical):**
```sql
CASE 
    WHEN ethnicity_african_american >= 1 THEN 'African American'
    WHEN ethnicity_asian >= 1 THEN 'Asian'
    WHEN ethnicity_white_non_hispanic >= 1 THEN 'White Non-Hispanic'
    ...
END
```

6. **Home Ownership (Numeric: 1/0/NULL):**
```sql
CASE
    WHEN home_owner_hh >= 1 THEN 1
    WHEN home_renter_hh >= 1 THEN 0
    ELSE NULL
END
```

7. **Household Income (Integer K):**
```sql
-- Returns lower bound in thousands (0, 35, 45, 55, 70, 85, 100, 125, 150, 200)
CASE
    WHEN income_0_35_hh >= 1 THEN 0
    WHEN income_35_45_hh >= 1 THEN 35
    ...
END
```

8. **Income Bucket (Integer code):**
```sql
-- 1: $0-35K, 2: $35-45K, ..., 10: $200K+, 11: Unknown
```

**Location Enrichment:**
```sql
-- Gets latest location from ipage
latest_location AS (
    SELECT hash, city, iso_state AS state, dma, zipcode AS zip_code
    FROM production_r2081_ipage
    WHERE hash IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hash ORDER BY date_partition DESC) = 1
)
-- Then LEFT JOIN to attributes_decoded
```

**Key Field Optimizations:**
- `AKKIO_ID`, `TV_ID`, `AKKIO_HH_ID` all contain same value
- Separate columns for downstream compatibility

**Output Fields:**
- Demographics: GENDER, AGE, AGE_BUCKET, EDUCATION_LEVEL, ETHNICITY, SPANISH_LANGUAGE
- Geographic: STATE, ZIP11 (11-digit padded), CITY, DMA_NAME, COUNTY_NAME (NULL - not available)
- Socioeconomic: MARITAL_STATUS, HOME_OWNERSHIP, HOMEOWNER (alias), HOUSEHOLD_INCOME_K, INCOME (alias), INCOME_BUCKET
- Household: HAS_BABIES_0_3, HAS_CHILDREN_0_18, ADULT_HOUSEHOLD_SIZE
- Mobility: MOVE_LIKELY, MOVE_RECENT
- Quality: LOW_QUALITY_FLAG, DEMO_INCOMPLETE_FLAG
- Financial: NET_WORTH (NULL - not available in Vizio data)
- Metadata: DBT_UPDATED_AT

#### v_agg_akkio_hh

**Source:** `v_akkio_attributes_latest`

**Configuration:**
```yaml
materialized: table
clustering: (PARTITION_DATE, AKKIO_HH_ID)
```

**Purpose:** Household-level grain for household-based analytics.

**Fields:**
- `AKKIO_HH_ID`: Household identifier (unique)
- `HH_WEIGHT`: Fixed at 1 per requirements (not 11)
- `HOMEOWNER`: Home ownership status (1/0/NULL)
- `INCOME`: Household income in dollars (HOUSEHOLD_INCOME_K * 1000)
- `INCOME_BUCKET`: Income range bucket code
- `PRESENCE_OF_CHILDREN`: Indicator (1/0) derived from HAS_CHILDREN_0_18
- `PARTITION_DATE`: Date partition

**Transformation:**
```sql
SELECT
    AKKIO_HH_ID,
    1 AS HH_WEIGHT,
    HOME_OWNERSHIP AS HOMEOWNER,
    HOUSEHOLD_INCOME_K * 1000 AS INCOME,
    CASE WHEN HAS_CHILDREN_0_18 = 'Y' THEN 1 ELSE 0 END AS PRESENCE_OF_CHILDREN,
    INCOME_BUCKET,
    PARTITION_DATE
FROM v_akkio_attributes_latest
```

#### v_agg_akkio_ind

**Source:** `v_akkio_attributes_latest` + IP aggregation from detail tables

**Configuration:**
```yaml
materialized: table
clustering: (PARTITION_DATE, AKKIO_ID)
```

**Purpose:** Individual-level grain for person-based analytics.

**IP Aggregation:**
```sql
-- Union IPs from all activity sources
content_ips UNION ALL commercial_ips UNION ALL standard_ips
```

**Fields:**
- `AKKIO_ID`: Individual identifier (unique)
- `AKKIO_HH_ID`: Household identifier
- `WEIGHT`: Fixed at 11 per requirements
- `GENDER`, `AGE`, `AGE_BUCKET`: Individual demographics
- `ETHNICITY`, `EDUCATION_LEVEL`, `MARITAL_STATUS`: Additional attributes
- `HOMEOWNER`, `INCOME`, `INCOME_BUCKET`: Household-level attributes
- `MAIDS`, `IPS`, `EMAILS`, `PHONES`: Contact identifier counts (placeholders, currently 0)
- `PARTITION_DATE`: Date partition

**Transformation Notes:**
- Gender: `COALESCE(GENDER, 'UNDETERMINED')` for Insights compatibility
- Ethnicity: `COALESCE(ETHNICITY, 'Unknown')` as ETHNICITY_PREDICTION
- Education: `COALESCE(EDUCATION_LEVEL, 'Unknown')` as EDUCATION
- Marital Status: `COALESCE(MARITAL_STATUS, 'Unknown')`
- State: `COALESCE(NULLIF(STATE, ''), 'Unknown')`
- Income: Multiplied by 1000 to convert from thousands to dollars
- ZIP Code: Right 5 digits extracted from ZIP11
- Contact fields: Currently set to 0 (placeholders for future enrichment)
- NET_WORTH_BUCKET: Cast to string, defaults to 'Unknown' (not available in Vizio data)

#### v_agg_akkio_media

**Source:** `vizio_daily_fact_content_detail`

**Configuration:**
```yaml
materialized: table
clustering: (PARTITION_DATE, AKKIO_ID)
```

**Purpose:** Individual-level aggregation of media viewing behavior for analytics. Provides maps of content categories (titles, genres, networks) to session counts.

**Transformation:**
- Uses `MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(k, v)))` to create category maps
- Flattens comma-separated genres using `LATERAL VIEW EXPLODE(SPLIT(GENRE, ','))`
- Counts sessions per category per individual per day
- Handles NULL values by converting to 'unknown' category

**Fields:**
- `TITLES_WATCHED` - Map of title → session count
- `GENRES_WATCHED` - Map of genre → session count  
- `NETWORKS_WATCHED` - Map of network → session count
- `INPUT_DEVICES_WATCHED` - Map of input device → session count
- `APP_SERVICES_WATCHED` - Map of app service → session count

---

## Common Data Transformations

### Text Standardization Patterns

**Standard Pattern:**
```sql
lower(replace(column, ' ', '-'))  -- Most text fields
upper(replace(column, ' ', '-'))  -- Input categories
lower(replace(column, ' ', '_'))  -- Input device names
```

**Applied to:**
- Networks, callsigns, titles, genres
- Brand names, ad titles
- Commercial categories
- Show titles, station callsigns

### Array and String List Patterns

**CSV String Lists:**
```sql
string_agg(DISTINCT column, ',') AS column_STR_LIST
```

**Array Collections:**
```sql
collect_set(column) AS column_ARRAY
```

**Genre Flattening:**
```sql
-- Explode semicolon-separated genres, then re-aggregate
explode(split(GENRE, ','))
```

### Clustering Strategy

**All Tables:**
```sql
-- Post-hook in dbt config
alter table {{this}} cluster by (partition_date, akkio_id)

-- Or for specific tables
cluster by (viewed_date, akkio_id)
cluster by (impression_date, akkio_id)
```

**Benefits:**
- Efficient partition pruning on date ranges
- Fast lookups by device ID
- Optimized for common join patterns

---

## Performance Optimization

### For 700GB+ Scale

#### 1. Table Selection Strategy
```
START: Summary Tables (daily aggregates)
  ↓
  If insufficient detail:
  ↓
USE: Detail Tables (session-level)
```

#### 2. Query Optimization Patterns

**Always Include Date Filter:**
```sql
WHERE PARTITION_DATE BETWEEN '2024-10-01' AND '2024-10-07'
```

**Use Clustered Columns in WHERE:**
```sql
WHERE PARTITION_DATE = '2024-10-01'
  AND AKKIO_ID IN (...)
```

**Leverage String Lists for Simple Matching:**
```sql
-- Efficient
WHERE GENRE_STR_LIST LIKE '%sports%'

-- Less efficient (requires detail table)
WHERE GENRE = 'sports'
```

**Join on Clustered Columns:**
```sql
ON t1.PARTITION_DATE = t2.PARTITION_DATE
AND t1.AKKIO_ID = t2.AKKIO_ID
```

#### 3. Incremental Processing

**Detail Tables Configuration:**
```yaml
materialized: incremental
```

**Batch Processing:**
```bash
# Process specific date range
dbt run --models vizio_daily_fact_content_detail \
  --vars '{"start_date": "2024-10-01", "end_date": "2024-10-31"}'
```

**Normal Incremental:**
```bash
# Process only new data
dbt run --models vizio_daily_fact_content_detail
```

#### 4. Databricks Optimizations

**Available (commented out in dbt_project.yml):**
```yaml
# file_format: delta
# on_schema_change: 'sync_all_columns'
# tblproperties:
#   delta.autoOptimize.optimizeWrite: 'true'
#   delta.autoOptimize.autoCompact: 'true'
#   delta.checkpoint.writeStatsAsJson: 'false'
#   delta.checkpoint.writeStatsAsStruct: 'true'
```

**To Enable:**
Uncomment in `dbt_project.yml` under `vizio:` section.

---

## Schema Documentation (schema.yml)

### Source Definitions

All sources reference `vizio-poc-share.akkio` via Delta Share:
```yaml
sources:
  - name: vizio_poc_share
    database: vizio-poc-share
    schema: akkio
    tables:
      - name: production_r2079_content_with_null
      - name: production_r2080_commercialfeedmodular
      - name: production_r2081_ipage
      - name: production_r2067_optout
      - name: mk_akkio_genre_title_mapping
      - name: mk_akkio_tvtimezone_mapping
      - name: mk_commercialcategory_mapping
      - name: nothing_bundt_cakes_attr_data_akkio_poc
      - name: nothing_bundt_cakes_pop_data_akkio_poc
      - name: farm_bureau_financial_services_attr_data_akkio_poc
      - name: farm_bureau_financial_services_pop_data_akkio_poc
      - name: mk_akkio_experian_demo
```

### Model Documentation

**Short-name Tags:**
- `:short-name:content_consumption:` - Content viewing tables
- `:short-name:commercial_ad_views:` - Commercial tables
- `:short-name:device_activity_detail:` - Device activity
- `:short-name:campaign_attribution:` - Campaign tables
- `:short-name:audience_attributes:` - Demographic attributes
- `:short-name:individual_demographics:` - Individual aggregation
- `:short-name:household_demographics:` - Household aggregation

**Column Annotations:**
- `:lower` - Lowercase transformation applied
- `:space-to-hyphen` - Space replaced with hyphen
- `:space-to-underscore` - Space replaced with underscore
- `:upper` - Uppercase transformation applied
- `:csv` - Comma-separated value list
- `:akkio-context-ignore` - Ignore for certain Akkio contexts
- `:all-unique-values` - All unique values should be indexed
- `:use_for_audience_gen:` - Use for audience generation
- `:ml-encoding-nominal:` - Use nominal encoding for ML
- `:akkio-ml-ignore:` - Ignore for ML models

### Data Tests

**Primary Key Tests:**
```yaml
data_tests:
  - dbt_utils.unique_combination_of_columns:
      combination_of_columns:
        - PARTITION_DATE
        - AKKIO_ID
```

**Not Null Tests:**
```yaml
- name: PARTITION_DATE
  data_tests:
    - not_null
- name: AKKIO_ID
  data_tests:
    - not_null
```

**Range Tests:**
```yaml
- name: AGE
  data_tests:
    - dbt_utils.accepted_range:
        min_value: 18
        max_value: 100
        severity: warn
```

**Expression Tests:**
```yaml
- name: TOTAL_VIEWING_SECONDS
  data_tests:
    - not_null
    - dbt_utils.expression_is_true:
        expression: ">= 0"
```

---

## Build Instructions

### Prerequisites

1. **Configure profiles.yml:**
```yaml
vizio_poc_databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <your-databricks-host>
      http_path: <your-http-path>
      schema: vizio_poc
      catalog: akkio
```

2. **Install Dependencies:**
```bash
dbt deps
```

### Build Commands

**Build All Models:**
```bash
dbt build --models vizio
```

**Build Specific Model:**
```bash
dbt run --models vizio_daily_fact_content_detail
```

**Build with Dependencies:**
```bash
dbt build --models vizio_daily_fact_content_detail+
```

**Build Downstream:**
```bash
dbt build --models +vizio_daily_fact_content_summary
```

**Build Date Range (Batch Mode):**
```bash
dbt run --models vizio_daily_fact_content_detail \
  --vars '{"start_date": "2024-10-01", "end_date": "2024-10-31"}'
```

### Test Commands

**Run All Tests:**
```bash
dbt test --models vizio
```

**Test Specific Model:**
```bash
dbt test --models vizio_daily_fact_content_summary
```

**Test with Selection:**
```bash
dbt test --models vizio --exclude vizio_daily_fact_standard_detail
```

### Validation Commands

**Check Source Connections:**
```bash
dbt debug
```

**Compile Without Running:**
```bash
dbt compile --models vizio
```

**Generate Documentation:**
```bash
dbt docs generate
dbt docs serve
```

---

## Data Quality & Validation

### Built-in Filters

**All Models:**
- Filter NULL TV_IDs/hashes
- Filter invalid partition dates

**Content Models:**
- Filter NULL show titles
- Minimum viewing duration: 10 seconds

**Commercial Models:**
- Include all valid creative IDs
- No minimum duration filter

**Standard Models:**
- Include all device activity
- No duration filter

### Data Tests Coverage

**Primary Keys:**
- Summary tables: unique combination of (PARTITION_DATE, AKKIO_ID)
- Attributes table: unique TV_ID

**Not Null:**
- PARTITION_DATE (all models)
- AKKIO_ID (all fact tables)
- Timestamp fields (start/end times)

**Range Validation:**
- AGE: 18-100 (warn on violations)
- HOUSEHOLD_INCOME_K: 0-500 (warn on violations)
- ADULT_HOUSEHOLD_SIZE: 0-20 (warn on violations)
- All duration fields: >= 0 (error on violations)

**Business Logic:**
- MARKET_TV_POPULATION > 0 (warn)
- WEIGHT = 11 (error on violations)

---

## Development Patterns

### Adding New Models

1. **Create SQL file** in `models/vizio/`
2. **Add to schema.yml** with description and tests
3. **Follow naming convention:**
   - Fact tables: `vizio_daily_fact_<type>_<grain>`
   - Campaign tables: `vizio_campaign_<campaign_name>`
   - Attributes: `v_<type>_<description>`

4. **Apply standard patterns:**
   - Text standardization
   - Timezone enrichment
   - Clustering configuration
   - Incremental logic (for detail tables)

### Modifying Existing Models

1. **Test in development:**
```bash
dbt run --models <model_name> --target dev
```

2. **Validate results:**
```bash
dbt test --models <model_name>
```

3. **Check row counts:**
```sql
SELECT COUNT(*) FROM akkio.vizio_poc.<model_name>
```

4. **Compare before/after:**
```sql
-- Save counts before changes
-- Run model
-- Compare counts and sample data
```

### Debugging

**Check Compiled SQL:**
```bash
dbt compile --models <model_name>
# View in target/compiled/vizio_poc_databricks/models/vizio/
```

**Run with Debug Logging:**
```bash
dbt --debug run --models <model_name>
```

**Check for Incremental Issues:**
```bash
# Force full refresh
dbt run --models <model_name> --full-refresh
```

---

## Campaign Attribution Use Cases

### Cross-Table Analysis Patterns

**Campaign + Content Viewing:**
```sql
SELECT 
  ca.AKKIO_ID,
  ca.IMPRESSION_TIMESTAMP,
  ca.SHOW_TITLE as impression_context,
  cs.GENRE_STR_LIST as daily_genres_watched,
  cs.TOTAL_VIEWING_SECONDS
FROM vizio_campaign_nothing_bundt_cakes ca
LEFT JOIN vizio_daily_fact_content_summary cs
  ON ca.AKKIO_ID = cs.AKKIO_ID 
  AND ca.PARTITION_DATE = cs.PARTITION_DATE
WHERE ca.PARTITION_DATE = '2024-10-01'
```

**Campaign + Demographics:**
```sql
SELECT 
  ca.MARKET,
  attr.AGE_BUCKET,
  attr.HOUSEHOLD_INCOME_K,
  COUNT(DISTINCT ca.AKKIO_ID) as unique_reach,
  COUNT(*) as total_impressions
FROM vizio_campaign_farm_bureau_financial_services ca
LEFT JOIN v_akkio_attributes_latest attr
  ON ca.AKKIO_ID = attr.AKKIO_ID
WHERE ca.PARTITION_DATE = '2024-10-01'
GROUP BY ca.MARKET, attr.AGE_BUCKET, attr.HOUSEHOLD_INCOME_K
```

**Campaign + Commercial Co-viewing:**
```sql
SELECT 
  ca.AKKIO_ID,
  ca.IMPRESSION_TIMESTAMP,
  com.BRAND_NAME_STR_LIST as other_brands_seen,
  com.TOTAL_AD_VIEWS as daily_ad_exposure
FROM vizio_campaign_nothing_bundt_cakes ca
LEFT JOIN vizio_daily_fact_commercial_summary com
  ON ca.AKKIO_ID = com.AKKIO_ID
  AND ca.PARTITION_DATE = com.PARTITION_DATE
WHERE ca.PARTITION_DATE = '2024-10-01'
```

---

## Demographic Enrichment Use Cases

### Audience Segmentation

**High-Value Sports Viewers:**
```sql
SELECT 
  attr.AKKIO_ID,
  attr.AGE,
  attr.GENDER,
  attr.HOUSEHOLD_INCOME_K,
  attr.EDUCATION_LEVEL,
  cont.TOTAL_VIEWING_SECONDS / 3600 as total_hours
FROM v_akkio_attributes_latest attr
JOIN vizio_daily_fact_content_summary cont
  ON attr.AKKIO_ID = cont.AKKIO_ID
WHERE cont.GENRE_STR_LIST LIKE '%sports%'
  AND attr.HOUSEHOLD_INCOME_K >= 100
  AND cont.PARTITION_DATE = '2024-10-01'
```

**Household Analysis:**
```sql
SELECT 
  attr.STATE,
  attr.DMA_NAME,
  attr.INCOME_BUCKET,
  attr.HOME_OWNERSHIP,
  COUNT(DISTINCT attr.AKKIO_HH_ID) as household_count,
  AVG(cont.TOTAL_VIEWING_SECONDS) / 3600 as avg_viewing_hours
FROM v_akkio_attributes_latest attr
JOIN vizio_daily_fact_content_summary cont
  ON attr.AKKIO_ID = cont.AKKIO_ID
WHERE cont.PARTITION_DATE = '2024-10-01'
GROUP BY attr.STATE, attr.DMA_NAME, attr.INCOME_BUCKET, attr.HOME_OWNERSHIP
```

**Individual-Level Audience Building:**
```sql
SELECT 
  ind.AKKIO_ID,
  ind.GENDER,
  ind.AGE,
  ind.ETHNICITY,
  ind.INCOME,
  ind.WEIGHT
FROM v_agg_akkio_ind ind
WHERE ind.AGE BETWEEN 25 AND 54
  AND ind.INCOME >= 75
  AND ind.GENDER IN ('M', 'F')
```

---

## Future Enhancements

### Planned Improvements

1. **Standard Summary Table:**
   - Create `vizio_daily_fact_standard_summary` for device activity aggregation
   - Pattern: same as content/commercial summaries

2. **IP Collection:**
   - Currently placeholder in `v_agg_akkio_ind`
   - Add actual IP collection logic from source tables

3. **Additional Contact Identifiers:**
   - MAIDS (Mobile Advertising IDs)
   - Email addresses
   - Phone numbers
   - Requires additional source data

4. **Optimization:**
   - Enable Databricks Auto Optimize
   - Implement Z-ordering on hot columns
   - Add materialized views for common queries

5. **Data Quality:**
   - Add more comprehensive tests
   - Implement data freshness checks
   - Add anomaly detection for row counts

---

## Troubleshooting

### Common Issues

**Issue: Incremental model not picking up new data**
```bash
# Solution: Force full refresh
dbt run --models <model_name> --full-refresh
```

**Issue: Delta Share connection errors**
```bash
# Solution: Verify source tables are accessible
dbt run-operation stage_external_sources --args '{sources: vizio_poc_share}'
```

**Issue: Clustering not applied**
```bash
# Solution: Check post-hooks are executing
# View logs for "alter table" commands
dbt --debug run --models <model_name>
```

**Issue: Test failures on range checks**
```bash
# Solution: Check data quality at source
# Range tests have severity: warn, so they won't fail builds
dbt test --models <model_name> --warn-error
```

### Performance Issues

**Slow Summary Table Builds:**
1. Check detail table is properly clustered
2. Verify partition pruning is working
3. Consider breaking into smaller date ranges

**Slow Detail Table Builds:**
1. Use batch mode with date range variables
2. Check incremental logic is working
3. Verify source table filters are applied

**Memory Issues:**
1. Process smaller date ranges
2. Increase Databricks cluster size
3. Enable Auto Optimize and Auto Compaction

---

## Maintenance

### Regular Tasks

**Daily:**
- Run incremental models: `dbt run --models vizio`
- Check for test failures: `dbt test --models vizio`

**Weekly:**
- Review data quality metrics
- Check row count trends
- Monitor build times

**Monthly:**
- Optimize tables: `OPTIMIZE table_name ZORDER BY (partition_date, akkio_id)`
- Vacuum old files: `VACUUM table_name RETAIN 168 HOURS`
- Review and update documentation

---

## Configuration Files

### dbt_project.yml

**Key Settings:**
```yaml
name: 'vizio_poc_databricks'
version: '1.0.0'
profile: 'vizio_poc_databricks'

models:
  vizio_poc_databricks:
    +materialized: table
    +persist_docs:
      relation: true
      columns: true
    vizio:
      +tags: 
        - vizio
      +database: akkio
```

### packages.yml

**Dependencies:**
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=0.8.0", "<2.0.0"]
```

---

## Technical Metadata

**DBT Version:** 1.0+  
**Databricks Runtime:** Compatible with Delta Lake  
**Delta Share:** vizio-poc-share.akkio  
**Output Schema:** akkio.vizio_poc  
**Data Scale:** 700GB+ daily  
**Model Count:** 11 production models  
**Test Count:** 50+ data quality tests  

**Last Updated:** October 2025  
**Maintained By:** Akkio Data Engineering Team  
**Documentation Version:** 1.0

