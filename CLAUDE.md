# CLAUDE.md — CA Residential Structure Panel

This file is automatically loaded by Claude Code when working in this repository.

---

## Project in One Paragraph

This repository produces an annual panel of residential housing stock estimates for California
Census tracts (2010–2024). It combines Overture Maps building footprints (2024 anchor),
Census Building Permits Survey backward hind-cast, and CAL FIRE DINS demolition correction.
A 500-iteration bootstrap over Overture's 69% null-subtype rate produces uncertainty intervals
(P5/P50/P95/IQR). An ACS B25001 challenger panel validates the hind-cast. The panel is
general-purpose: it can serve as a housing stock denominator for any California tract-level
analysis requiring annual structure counts with wildfire demolition corrections.

---

## Data Integrity — No Fabricated Quantitative Values

**Full rule is in `~/Projects/CLAUDE.md`. This section adds project-specific enforcement.**

Never generate, hardcode, or invent any quantitative value unless it is read from an existing
data file, derived from one by documented computation, a universally-known constant, or a
value pre-specified here and explicitly confirmed by the PI.

**Pre-specified values confirmed by PI (exempt from confirmation protocol):**
- Unit-to-structure ratios: 1-unit → 1.0, 2-unit → 2.0, 3–4 unit → 3.5, 5+ unit → 15.0
  (confirmed 2026-03-01, documented in `04_build_structure_panel.py`)
- Bootstrap iterations: B = 500 (confirmed 2026-03-01)
- Beta prior: Beta(2, 5) initial prior; calibrated against ACS B25001 external validation
  (confirmed 2026-03-01)
- Annual noise CV: σ = 0.5% of annual count per county (confirmed 2026-03-01)
- Null-absorption upper clip: 0.99 (confirmed 2026-03-01)

---

## Project Status (as of 2026-03-06)

| Component | Status |
|---|---|
| Directory structure | **Complete** — created 2026-03-06 |
| GitHub repo | **Complete** — https://github.com/rkvaughn/ca-residential-structure-panel (public) |
| Script 01: acquire_overture | **Validated** — prior successful run in prop13/ 2026-03-01 |
| Script 02: acquire_bps | **Validated** — prior successful run in prop13/ 2026-03-01 |
| Script 03: acquire_dins | **Validated** — prior successful run in prop13/ 2026-03-01 |
| Script 04: build_structure_panel | **Validated** — run in new repo 2026-03-06; 100,350 rows, 2024 hind-cast = anchor |
| Script 05: bootstrap_structure_panel | **Validated** — prior successful run in prop13/ 2026-03-03 |
| Script 06: build_acs_challenger | **Validated** — run in new repo 2026-03-06; Boot/ACS ratio ≈ 0.82–0.84 |
| Figures (6) | **Complete** — in output/figures/ |
| Tables (2) | **Complete** — in output/tables/ |
| Paper (markdown) | **Complete** — standalone framing, prop13 DDD refs removed |
| Paper (PDF) | **Complete** — rebuilt 2026-03-06 |
| GitHub Release v1.0 | **Complete** — 5 parquet assets uploaded |
| utils/download_utils.py | Copied from ~/Projects/utilities/ 2026-03-06 |
| utils/census_api.py | Copied from ~/Projects/utilities/ 2026-03-06 |

---

## Script Path Conventions

All scripts use:
```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # ca-residential-structure-panel/
sys.path.insert(0, str(Path(__file__).parent / "utils"))  # scripts/utils/
```

Scripts are in `scripts/` (not `scripts/01_build/` as in prop13). Utility modules are in
`scripts/utils/`.

---

## Directory Structure

```
ca-residential-structure-panel/
├── scripts/
│   ├── 01_acquire_overture.py
│   ├── 02_acquire_bps.py
│   ├── 03_acquire_dins.py
│   ├── 04_build_structure_panel.py
│   ├── 05_bootstrap_structure_panel.py
│   ├── 06_build_acs_challenger.py
│   └── utils/
│       ├── download_utils.py
│       └── census_api.py
├── data/
│   ├── raw/          # gitignored — re-acquire via scripts 01–03
│   └── clean/        # gitignored — distributed via GitHub Releases v1.0
├── output/
│   ├── figures/      # 6 PNG figures — committed
│   └── tables/       # 2 CSV tables — committed
├── paper/
│   ├── structure_count_writeup.md
│   └── structure_count_writeup.pdf
├── .gitignore
├── CLAUDE.md
└── README.md
```

---

## PDF Rendering Toolchain

Same as prop13_paper:
- **Engine:** `tectonic` via `pandoc --pdf-engine=tectonic`
- **Font:** `--variable mainfont="Georgia" --variable monofont="Menlo"`
- **Unicode:** Preprocess μ, →, ≈, ✓ → LaTeX math equivalents before piping to pandoc
- **Working dir:** Run pandoc from `paper/` so `../output/figures/` paths resolve

---

## Git Workflow

- Default branch: `main`
- Raw data: never committed (gitignored); distributed via GitHub Releases
- Commit output figures and tables (they are pipeline outputs, not raw data)
- Commit paper PDF
