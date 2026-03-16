# CA Residential Structure Panel

**Annual residential housing stock estimates for California Census tracts (2010–2024)**

Built from Overture Maps building footprints, Census Building Permits Survey (BPS) data, and CAL FIRE Damage Inspection (DINS) demolition records, with bootstrap uncertainty quantification over Overture's 69% null-subtype rate.

**[Interactive Dashboard →](https://rkvaughn.github.io/ca-residential-structure-panel/)** — Browse tract-level estimates by year, compare panels, view figures.

---

## Data

Pre-built panel outputs are distributed via **[GitHub Releases v1.0](https://github.com/rkvaughn/ca-residential-structure-panel/releases/tag/v1.0)**:

| File | Description | Size |
|------|-------------|------|
| `tract_structure_panel.parquet` | Point-estimate hind-cast panel (100,350 tract × year rows) | ~563 KB |
| `tract_structure_panel_bootstrap.parquet` | Bootstrap P5/P50/P95/IQR panel | ~1.8 MB |
| `tract_structure_panel_acs.parquet` | ACS B25001 challenger panel (2010–2024) | ~464 KB |
| `county_permits_ca_2010_2024.parquet` | County-level BPS permit data | ~24 KB |
| `tract_residential_counts_2024.parquet` | 2024 Overture anchor counts per tract | ~75 KB |

Raw data (Overture GeoParquet, BPS files, DINS CSVs, Census shapefiles) are NOT committed; see scripts 01–03 to re-acquire.

---

## Method Summary

Three publicly available sources are combined:

1. **Overture Maps (2024)** — 15.6M California building footprints; filtered to residential subtype and spatially joined to 2010 Census tracts. Provides the 2024 anchor count (3.94M labeled residential buildings statewide).

2. **Census BPS (2010–2024)** — Annual county-level authorized residential units, converted to structure counts using PI-confirmed unit-to-structure ratios (1-unit: 1.0, 2-unit: 2.0, 3–4 unit: 3.5, 5+ unit: 15.0).

3. **CAL FIRE DINS (2013–2022)** — 50,483 destroyed residential structures across 194 county-year fire events; used to correct the backward hind-cast in wildfire-affected counties.

The hind-cast runs a DINS-corrected backward cumulative sum from the 2024 Overture anchor; each county's annual total is then downscaled to tracts using each tract's proportional share of the county's Overture residential count.

A 500-iteration bootstrap over the null-subtype absorption fraction (Beta prior calibrated against ACS B25001 external validation) produces P5/P50/P95 uncertainty intervals. The bootstrap P50 tracks ACS housing unit counts at ~82% (reflecting the genuine unit/structure distinction) and substantially outperforms the labeled-only point estimate in wildfire-affected counties (e.g., Butte County: bootstrap 87,850 vs. ACS 98,743 pre-Camp Fire, vs. point estimate 2,016).

See **[paper/structure_count_writeup.pdf](paper/structure_count_writeup.pdf)** for full methodology, diagnostics, and validation results.

---

## Scripts

Run in order:

| Script | What it does |
|--------|-------------|
| `scripts/01_acquire_overture.py` | Download CA buildings from Overture Maps, filter to residential, spatial join to 2010 tracts |
| `scripts/02_acquire_bps.py` | Download Census BPS county permit files 2010–2024; convert units to structures |
| `scripts/03_acquire_dins.py` | Download CAL FIRE DINS destroyed residential structures 2013–present |
| `scripts/04_build_structure_panel.py` | DINS-corrected hind-cast + downscale → tract × year point estimate panel |
| `scripts/05_bootstrap_structure_panel.py` | ACS-external Beta calibration + B=500 bootstrap → tract × year P5/P50/P95/IQR panel |
| `scripts/06_build_acs_challenger.py` | ACS B25001 challenger panel 2010–2024; comparison vs. BPS and bootstrap |

Scripts 01–03 require internet access and take 15–60 minutes (Overture download is ~2.75 GB). Scripts 04–06 run in under 5 minutes total given cached inputs.

### Dependencies

```bash
pip install geopandas overturemaps pandas numpy scipy pyarrow requests
```

A Census API key is required for scripts 05 and 06. Set `CENSUS_API_KEY` in your environment or pass via the `api_key` argument.

---

## Outputs

- `output/figures/` — 6 PNG figures (maps, time series, fire county comparisons, calibration diagnostics, county scatter)
- `output/tables/` — bootstrap calibration log, ACS vs. BPS comparison CSV

---

## Citation

If you use this dataset or methodology, please cite:

```
Vaughn, Ryan K. (2026). CA Residential Structure Panel: Annual residential housing
stock estimates for California Census tracts, 2010–2024.
https://github.com/rkvaughn/ca-residential-structure-panel
```

---

## Related Projects

- **[Prop 13 / Insurance Wedge](https://github.com/rkvaughn/prop13_paper)** — The empirical context for which this panel was originally developed. Consumes the panel outputs via `acquire_structure_panel.py`.
- **[python-geo-utils](https://github.com/rkvaughn/python-geo-utils)** — Shared download and Census API utilities used by the acquisition scripts.
