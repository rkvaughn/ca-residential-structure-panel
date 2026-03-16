---
title: CA Residential Structure Panel — Overview
---

# CA Residential Structure Panel

An annual panel of estimated residential structure counts for all California Census tracts, 2010–2024. Combines [Overture Maps](https://overturemaps.org) building footprints, Census Bureau Permits Survey (BPS), and CAL FIRE damage inspections (DINS). A 500-iteration bootstrap over Overture's 69% null-subtype rate produces P5/P50/P95 uncertainty intervals calibrated against ACS B25001.

<div class="grid grid-cols-3 gap-3 mt-4 mb-6">
  <a class="card" href="./map" style="text-decoration:none;">
    <h2>Map Explorer →</h2>
    <p>Browse tract-level structure counts by year and panel. Compare estimation methods.</p>
  </a>
  <a class="card" href="./figures" style="text-decoration:none;">
    <h2>Figures →</h2>
    <p>Bootstrap calibration, validation plots, spaghetti charts, and animated county animation.</p>
  </a>
  <a class="card" href="./downloads" style="text-decoration:none;">
    <h2>Data & Downloads →</h2>
    <p>Download parquet panel files from GitHub Releases. Column schemas and methodology notes.</p>
  </a>
</div>

---

## Statewide Residential Structures, 2010–2024

```js
import {fetchStatewideByYear} from "./components/supabase-client.js";

// Query the panel_hybrid_annual aggregated view (15 rows — fast)
const annual = await fetchStatewideByYear();
```

```js
// Statewide time-series with P5/P95 uncertainty band
Plot.plot({
  title: "Total CA Residential Structures (P5 / Median / P95)",
  subtitle: "Bootstrap uncertainty over Overture Maps null-subtype absorption fraction",
  width,
  height: 320,
  x: {label: "Year", tickFormat: "d"},
  y: {label: "Structures (millions)", tickFormat: d => (d / 1e6).toFixed(1) + "M"},
  marks: [
    Plot.areaY(annual, {
      x: "year", y1: "p5_total", y2: "p95_total",
      fill: "#e07b39", fillOpacity: 0.2,
    }),
    Plot.lineY(annual, {x: "year", y: "p50_total", stroke: "#e07b39", strokeWidth: 2}),
    Plot.dot(annual, {x: "year", y: "p50_total", fill: "#e07b39", r: 3}),
    Plot.tip(annual, Plot.pointerX({
      x: "year",
      title: d => `${d.year}\nP50: ${(d.p50_total / 1e6).toFixed(3)}M\nP5–P95: ${(d.p5_total / 1e6).toFixed(3)}M – ${(d.p95_total / 1e6).toFixed(3)}M`,
    })),
  ],
})
```

---

## 2024 Snapshot

```js
const latest = annual?.at(-1);
```

<div class="grid grid-cols-3 gap-3 my-4">
  <div class="card">
    <h3>Median Structures</h3>
    <p class="big-number">${latest ? (latest.p50_total / 1e6).toFixed(2) + "M" : "—"}</p>
    <small>P5–P95: ${latest ? [(latest.p5_total/1e6).toFixed(2), (latest.p95_total/1e6).toFixed(2)].join("M – ") + "M" : "—"}</small>
  </div>
  <div class="card">
    <h3>Census Tracts</h3>
    <p class="big-number">${latest ? latest.n_tracts.toLocaleString() : "—"}</p>
    <small>2010 vintage</small>
  </div>
  <div class="card">
    <h3>Counties</h3>
    <p class="big-number">${latest ? latest.n_counties : "—"}</p>
    <small>All 58 CA counties</small>
  </div>
</div>

---

## Methodology

| Source | Coverage | Role |
|---|---|---|
| **Overture Maps (2024)** | 15.6M CA building footprints; 4.35M labeled residential | 2024 anchor count |
| **Census BPS (2010–2024)** | 1.41M authorized residential units | Annual permit-based hindcast |
| **CAL FIRE DINS (2013–2022)** | 50,483 destroyed residential structures | Wildfire demolition correction |
| **ACS B25001 (2010–2024)** | 5-year rolling estimates by tract | External validation |

The bootstrap samples the Overture null-subtype absorption fraction from a Beta(2, 5) prior calibrated against ACS counts, propagating labeling uncertainty into P5/P50/P95 intervals. For 15 dense-urban counties, the Arruda et al. (2024) OSM-derived counts replace the ACS-calibrated absorption fraction (Spearman ρ = 0.916 vs. bootstrap).

**Paper:** [structure_count_writeup.pdf](https://github.com/rkvaughn/ca-residential-structure-panel/blob/main/paper/structure_count_writeup.pdf) · **Code:** [github.com/rkvaughn/ca-residential-structure-panel](https://github.com/rkvaughn/ca-residential-structure-panel)

<style>
.big-number { font-size: 2rem; font-weight: bold; margin: 0.25rem 0; }
.warning { padding: 0.75rem 1rem; background: #fff3cd; border-left: 4px solid #ffc107; border-radius: 4px; }
</style>
