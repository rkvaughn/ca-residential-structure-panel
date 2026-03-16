---
title: Data & Downloads
---

# Data & Downloads

Pre-built panel files are distributed via **GitHub Releases**. All panels cover California Census tracts (2010 vintage) for years 2010–2024.

**Repository:** [github.com/rkvaughn/ca-residential-structure-panel](https://github.com/rkvaughn/ca-residential-structure-panel)
**Paper:** [structure_count_writeup.pdf](https://github.com/rkvaughn/ca-residential-structure-panel/blob/main/paper/structure_count_writeup.pdf)
**GitHub Releases:** [v1.0](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.0) · [v1.1](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.1)

---

## Panel Files

<div class="grid gap-3">

<div class="card">

### Arruda Hybrid Panel (Best Estimate) · v1.1
**File:** `tract_structure_panel_arruda_hybrid.parquet` · ~1.7 MB · 120,855 rows

The primary output. Bootstrap P5/P50/P95 with Arruda calibration replacing ACS-clipped absorption fractions for 15 dense-urban counties.

| Column | Type | Description |
|---|---|---|
| `geoid` | string | 11-digit 2010 Census tract FIPS |
| `county_fips` | string | 5-digit county FIPS |
| `year` | int | 2010–2024 |
| `p5_residential_count` | float | 5th percentile structure count |
| `p50_residential_count` | float | Median structure count |
| `p95_residential_count` | float | 95th percentile structure count |
| `iqr_residential_count` | float | Interquartile range |
| `alpha_c` | float | Beta distribution α parameter (county) |
| `beta_c` | float | Beta distribution β parameter (county) |

[Download from v1.1 →](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.1)

</div>

<div class="card">

### ACS B25001 Challenger Panel · v1.0
**File:** `tract_structure_panel_acs.parquet` · ~453 KB · 120,855 rows

ACS 5-year rolling housing unit estimates used as the external validation benchmark. Note: ACS measures *housing units*, not structures; the expected bootstrap/ACS ratio is ~0.82.

| Column | Type | Description |
|---|---|---|
| `geoid` | string | 11-digit 2010 Census tract FIPS |
| `county_fips` | string | 5-digit county FIPS |
| `year` | int | 2010–2024 |
| `acs_housing_units` | float | ACS B25001 housing unit estimate |
| `acs_vintage_year` | int | ACS survey vintage year used |
| `acs_extrapolated` | bool | True if extrapolated beyond ACS coverage |
| `acs_crosswalk_translated` | bool | True if 2020→2010 crosswalk applied |
| `acs_imputed` | bool | True if value was imputed |

[Download from v1.0 →](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.0)

</div>

<div class="card">

### Point Estimate Panel · v1.0
**File:** `tract_structure_panel.parquet` · ~550 KB · 100,350 rows

Deterministic hindcast without bootstrap uncertainty. Useful as a fast baseline that requires no sampling. Excludes tracts with zero Overture anchor counts.

| Column | Type | Description |
|---|---|---|
| `geoid` | string | 11-digit 2010 Census tract FIPS |
| `county_fips` | string | 5-digit county FIPS |
| `year` | int | 2010–2024 |
| `residential_count_hindcast` | float | Point estimate structure count |
| `overture_residential_count_2024` | float | Overture 2024 labeled residential count |
| `tract_share` | float | Tract's share of county-level anchor |
| `structures_permitted` | float | BPS-derived permitted structures |
| `structures_destroyed` | float | DINS-derived demolished structures |
| `net_structures_change` | float | Net annual change |

[Download from v1.0 →](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.0)

</div>

<div class="card">

### Arruda Hindcast Panel · v1.1
**File:** `tract_structure_panel_arruda.parquet` · ~556 KB · 120,855 rows

Pre-hybrid Arruda-anchored hindcast. Uses Arruda et al. (2024) OSM-derived 2024 county residential counts as the anchor instead of labeled Overture counts. Useful for methodological comparison.

| Column | Type | Description |
|---|---|---|
| `geoid` | string | 11-digit 2010 Census tract FIPS |
| `county_fips` | string | 5-digit county FIPS |
| `year` | int | 2010–2024 |
| `residential_count_hindcast` | float | Arruda-anchored hindcast count |
| `tract_share` | float | Tract's share of county Arruda anchor |
| `county_anchor` | float | County-level Arruda RES count |
| `county_count_hindcast` | float | County-level hindcast count |

[Download from v1.1 →](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.1)

</div>

</div>

---

## Reading Parquet Files

**Python (pandas + pyarrow):**
```python
import pandas as pd
df = pd.read_parquet("tract_structure_panel_arruda_hybrid.parquet")
```

**R (arrow):**
```r
library(arrow)
df <- read_parquet("tract_structure_panel_arruda_hybrid.parquet")
```

**DuckDB (SQL):**
```sql
SELECT * FROM read_parquet('tract_structure_panel_arruda_hybrid.parquet') LIMIT 10;
```

---

## Tract Geometry

The 2010 California Census tract geometries (500k cartographic simplification) used for this dashboard are available in the repository:

**File:** `dashboard/src/data/ca-tracts.json` — GeoJSON with `geoid` property matching all panel files.

**Source:** Census TIGER/Line 2010 cartographic boundary (`GENZ2010`, state FIPS 06), simplified with geopandas (tolerance = 0.001°).
