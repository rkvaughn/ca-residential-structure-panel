# WUI Buffer Structure Count Analysis — Project Plan

**Goal:** Estimate the number of residential structures within buffer zones of the
Wildland-Urban Interface (WUI) as defined by the Banerjee et al. classification, by
year (2010–2024), using the CA Residential Structure Panel as the structure count source.

**Status:** Planning stage — data acquisition not yet begun.

---

## 1. Background and Motivation

The CA Residential Structure Panel provides annual residential structure counts at the
Census tract × year level (2010–2024) with bootstrap uncertainty intervals. These counts
can be aggregated to any geographic unit that is expressible as a weighted sum of Census
tracts. The WUI — where housing and wildland vegetation meet — is the primary exposure zone
for wildfire risk to residential structures.

Linking the panel to the WUI classification produces a time series of residential exposure
within the WUI, enabling:

- Annual estimates of structures at risk in WUI zones
- Decomposition of growth into WUI vs. non-WUI development
- Denominator for wildfire damage rates (structures destroyed / structures at risk)
- Input to insurance exposure analysis (complement to the Prop 13 paper M1 panel)

---

## 2. Data Sources Required

### 2.1 Banerjee WUI Classification

**First step before any code is written:** Locate and verify the exact citation, dataset
URL, schema, and geographic unit of the Banerjee WUI classification.

Key questions to resolve:
- Full citation (authors, year, journal/preprint, DOI)
- Data hosting location (OSF, Zenodo, agency website, GitHub)
- Geographic unit: is the classification at the parcel, grid cell, Census tract, or
  polygon level?
- Temporal coverage: is the classification a single snapshot or multi-year?
- Variable schema: what column identifies WUI membership/type (Intermix vs. Interface)?
- CRS and geometry format (Shapefile, GeoPackage, GeoJSON?)
- License: can it be freely downloaded and redistributed as derived outputs?

Until these are confirmed, no processing code should be written for this dataset.

**Known WUI classification alternatives (if Banerjee is unavailable):**
- Radeloff et al. (2018, PNAS) SILVIS Lab WUI — 2010 snapshot, Census block level,
  at https://silvis.forest.wisc.edu/data/wui-change/
- CalFire FHSZ (Fire Hazard Severity Zones) — polygon, available from CalFire GIS,
  used as a proxy for WUI exposure in the Prop 13 paper

### 2.2 CA Residential Structure Panel (already available)

- `data/clean/tract_structure_panel.parquet` — point estimate, 100,350 rows
- `data/clean/tract_structure_panel_bootstrap.parquet` — P5/P50/P95/IQR, 120,855 rows
- `data/clean/tract_structure_panel_arruda.parquet` — Arruda-anchored, 120,855 rows
- `data/clean/tract_structure_panel_arruda_hybrid.parquet` — hybrid calibration, 120,855 rows

All panels: geoid (11-digit 2010 tract FIPS), county_FIPS, year, residential count columns.

### 2.3 Census Tract Geometries (to be acquired)

TIGER 2010 Census tract polygons for California are needed to spatially join WUI
boundaries to tracts.

- URL: `https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/tl_2010_06_tract10.zip`
- Already used internally by script 05 for the spatial join (path:
  `data/raw/shapefiles/tl_2010_06_tract10/`)
- Need to acquire if not on disk.

---

## 3. Methodology

### 3.1 Approach: WUI Intersection Weights

The structure panel is at Census tract resolution. The WUI classification may be at
a finer or coarser resolution. The approach depends on the WUI unit:

**If WUI is at Census block or parcel level (finer than tract):**
1. Aggregate WUI membership up to tract: compute `wui_share_i` = fraction of
   tract i's land area (or housing units) classified as WUI.
2. Apply: `wui_structures_it = panel_structures_it × wui_share_i`
3. Limitation: assumes within-tract uniform distribution of structures.

**If WUI is polygon (e.g., SILVIS, FHSZ):**
1. Intersect WUI polygons with Census tract polygons using EPSG:3310 (CA Albers).
2. Compute `wui_area_share_i` = WUI polygon area ∩ tract i / tract i area.
3. Apply: `wui_structures_it = panel_structures_it × wui_area_share_i`
4. Limitation: area-proportional downscaling assumes uniform structure density
   within tract — less valid for large rural tracts.

**If WUI is at Census tract level (same as panel):**
1. Direct merge: no area weighting needed.
2. Tag each tract as WUI / Intermix / Interface / non-WUI.
3. Group structures by WUI type × year.

### 3.2 Buffer Analysis

"Within a buffer of the WUI" means residential structures located within distance d
of a WUI boundary (or within a WUI polygon expanded by distance d). Two interpretations:

**Interpretation A — Interior WUI:**
Structures within WUI-classified areas. This is the strict exposure estimate.

**Interpretation B — WUI + buffer:**
Structures within WUI plus structures within d km of WUI boundary. Buffer accounts for
structures just outside the WUI boundary that face similar fire exposure.
Candidate buffer distances: 0.5 km, 1 km, 5 km (to be confirmed with PI).

For both interpretations, the methodology is:
1. Create WUI polygon (or WUI + buffer polygon using `gpd.buffer(d, cap_style=1)`)
2. Intersect with Census tract polygons
3. Compute intersection area shares
4. Apply to structure panel

### 3.3 Temporal Handling

The WUI classification is typically a snapshot (one year). The structure panel runs
2010–2024. Two options:

**Option A — Static WUI boundary:** Apply the Banerjee WUI (single year) to all panel
years. This attributes all temporal variation to structure count changes, not WUI
reclassification. Cleanest for trend analysis if the WUI boundary is stable.

**Option B — Multi-year WUI (if Banerjee provides it):** Match WUI vintage to panel
year. If Banerjee only provides one year, use Option A.

---

## 4. Planned Scripts

### Script 09: `09_acquire_wui.py`

Acquire and preprocess the Banerjee WUI classification.

**Steps:**
1. Discover data URL from publication supplementary materials or OSF/Zenodo.
2. Download to `data/raw/wui/` (gitignored).
3. Load, filter to California, reproject to EPSG:3310.
4. Verify schema: confirm WUI type column, geometry validity.
5. Save clean CA WUI layer to `data/clean/wui_ca.parquet` or `wui_ca.gpkg`.
6. Print summary: WUI area by type, county coverage.

**Dependencies:** `requests`, `geopandas`, `download_utils.py`

### Script 10: `10_build_wui_structure_panel.py`

Compute structure counts within WUI + buffer zones by year.

**Steps:**
1. Load WUI polygons (`data/clean/wui_ca.gpkg`).
2. Load Census tract polygons (TIGER 2010). Acquire if not on disk.
3. Create buffer variants: interior WUI only; WUI + {buffer distances} (to be
   confirmed by PI before inserting specific values).
4. Intersect buffered WUI with tract polygons → tract-level WUI area share.
5. Load structure panel (bootstrap p50 recommended as primary; point estimate
   and Arruda hybrid as comparison columns).
6. Apply area shares: `wui_structures_it = panel_structures_it × wui_share_i`
7. Aggregate: by WUI type × year → total WUI structures statewide.
8. Optionally: by county × WUI type × year.
9. Save outputs (see Section 5).

**Note on buffer distances:** Candidate distances (0.5 km, 1 km, 5 km) are not
confirmed. Before inserting any specific buffer distance into code, propose values
to PI and wait for confirmation. Document confirmed values in script with source comment.

**Dependencies:** `geopandas`, `pandas`, `numpy`, `pyarrow`, `download_utils.py`

### Script 11 (optional): `11_wui_structure_figures.py`

Figures for WUI structure count analysis.

- Time series: total CA WUI structures by year, by WUI type (Intermix vs. Interface)
- County-level bar chart: top counties by WUI structure count
- Map: structure count density within WUI (tract-level choropleth, 2024 snapshot)

---

## 5. Outputs

| File | Description |
|------|-------------|
| `data/raw/wui/` | Raw Banerjee WUI files (gitignored) |
| `data/clean/wui_ca.gpkg` | CA WUI polygons, EPSG:3310, WUI type column |
| `data/clean/tract_wui_shares.parquet` | geoid, wui_type, area_share (one row per tract × WUI type) |
| `data/clean/wui_structure_panel.parquet` | geoid, county_FIPS, year, wui_type, wui_structures_p50, wui_structures_p5, wui_structures_p95 |
| `output/tables/wui_county_summary.csv` | county_FIPS, county_name, year, wui_type, structures (for top counties) |
| `output/figures/fig_wui_timeseries.png` | Total WUI structures by year, by type |
| `output/figures/fig_wui_county_bar.png` | Top 20 counties by WUI structure count |
| `output/figures/fig_wui_tract_choropleth.png` | 2024 tract-level WUI structure density map |

---

## 6. Methodological Caveats and Limitations

**Area-proportional assumption:** Downscaling panel counts from tract to WUI area
assumes structures are uniformly distributed within tracts. This is least valid in
large rural tracts where structures cluster near roads and at tract boundaries. The
Overture-based `tract_share` in the point estimate panel captures within-county
heterogeneity but not within-tract heterogeneity.

**WUI boundary vintage:** If the Banerjee classification is a single-year snapshot,
the WUI boundary is held fixed while structure counts evolve. This is appropriate for
"how many structures were at risk in WUI areas over time" but understates actual WUI
exposure if development pushes into WUI areas between the WUI vintage and the panel year.

**Unit of analysis:** The structure panel counts structures (buildings), not housing
units. In multi-family WUI areas, structures undercount residential units.

**Bootstrap uncertainty:** The bootstrap P5/P50/P95 intervals propagate uncertainty
from the Overture null-subtype calibration. They do not propagate uncertainty from the
area-proportional downscaling or WUI boundary delineation.

---

## 7. Execution Order

1. Locate and verify Banerjee WUI citation, data URL, schema — **before any code**
2. Confirm buffer distances with PI — **before inserting any buffer values in code**
3. Write and run `09_acquire_wui.py`
4. Write and run `10_build_wui_structure_panel.py`
5. Write and run `11_wui_structure_figures.py` (optional)
6. Update `CLAUDE.md` status table
7. Add WUI outputs to GitHub Release (if panel outputs are public-ready)

---

## 8. Open Questions (resolve before coding)

| Question | Needed for | How to resolve |
|---|---|---|
| Full Banerjee et al. citation and DOI | Step 1 (data acquisition) | Check paper supplementary; OSF; Google Scholar |
| Dataset URL and format | Script 09 | Follow citation to hosting repo |
| WUI type schema (column names) | Scripts 09, 10 | Read dataset documentation |
| Temporal coverage of WUI | Step 3.3 methodology choice | Read dataset documentation |
| Buffer distance(s) to use | Script 10 | PI confirmation required |
| WUI classification vs. FHSZ overlap | Comparison analysis | After both datasets acquired |
| Which panel to use as primary (bootstrap vs. hybrid) | Script 10 | PI preference; recommend hybrid for dense-urban WUI counties |
