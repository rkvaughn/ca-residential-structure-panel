---
title: Map Explorer
---

# Map Explorer

Browse tract-level residential structure **density** (structures per sq mile) across California. Use the controls to select a year and estimation method. **Click a tract on the map** to see its 2010–2024 time-series below.

```js
import {PANELS, fetchYearSlice, fetchTractSeries} from "./components/supabase-client.js";

// Tract geometry — pre-computed GeoJSON, committed to repo (~4.7 MB)
const tracts = await FileAttachment("data/ca-tracts.json").json();

// Net residential land area per tract (sq miles), excluding water bodies and public lands.
// Derived from 2010 Census block-level ALAND10 (water-free) aggregated over blocks
// with at least one housing unit (HU10 > 0). Blocks in national parks, national forests,
// BLM land, state parks, and military reservations are excluded automatically.
// See scripts/generate_tract_net_area.py for methodology.
const netAreaRaw = await FileAttachment("data/tract_net_area.json").json();
const tractAreaSqMi = new Map(Object.entries(netAreaRaw));

// Hybrid panel — all years loaded statically to avoid runtime CORS issues (~15 MB, gzipped ~2 MB)
const hybridAllYears = await FileAttachment("data/panel-hybrid.json").json();

// Fixed global density bounds computed once across all years of hybrid data.
// Using a stable domain means the color scale doesn't auto-rescale per year,
// so year-over-year changes (e.g. wildfire demolitions, new development) are
// visible as actual color shifts rather than being masked by rescaling.
const _allDensities = hybridAllYears
  .filter(d => d.p50_residential_count != null)
  .map(d => { const a = tractAreaSqMi.get(d.geoid) ?? 0; return a > 0 ? d.p50_residential_count / a : null; })
  .filter(v => v != null && v > 0);
const globalDMin = d3.min(_allDensities);
const globalDMax = d3.max(_allDensities);

// County FIPS (first 5 chars of tract GEOID) → nearest major metro area.
// Source: Census Bureau 2020 CBSA county delineations for all 58 CA counties.
const COUNTY_METRO = new Map([
  ["06001","Bay Area"],["06003","Rural CA"],["06005","Rural CA"],
  ["06007","Chico"],["06009","Rural CA"],["06011","Rural CA"],
  ["06013","Bay Area"],["06015","Crescent City"],["06017","Sacramento"],
  ["06019","Fresno"],["06021","Rural CA"],["06023","Eureka-Arcata"],
  ["06025","El Centro"],["06027","Rural CA"],["06029","Bakersfield"],
  ["06031","Hanford"],["06033","Rural CA"],["06035","Rural CA"],
  ["06037","Los Angeles"],["06039","Madera"],["06041","Bay Area"],
  ["06043","Rural CA"],["06045","Ukiah"],["06047","Merced"],
  ["06049","Rural CA"],["06051","Rural CA"],["06053","Salinas"],
  ["06055","Napa"],["06057","Truckee-Grass Valley"],["06059","Los Angeles"],
  ["06061","Sacramento"],["06063","Rural CA"],["06065","Inland Empire"],
  ["06067","Sacramento"],["06069","Hollister"],["06071","Inland Empire"],
  ["06073","San Diego"],["06075","Bay Area"],["06077","Stockton"],
  ["06079","San Luis Obispo"],["06081","Bay Area"],["06083","Santa Barbara"],
  ["06085","San Jose"],["06087","Santa Cruz"],["06089","Redding"],
  ["06091","Rural CA"],["06093","Rural CA"],["06095","Vallejo-Fairfield"],
  ["06097","Santa Rosa"],["06099","Modesto"],["06101","Yuba City"],
  ["06103","Red Bluff"],["06105","Rural CA"],["06107","Visalia"],
  ["06109","Sonora"],["06111","Ventura"],["06113","Sacramento"],
  ["06115","Yuba City"],
]);
```

<div class="grid grid-cols-4 gap-3 my-3">

```js
const year = view(Inputs.range([2010, 2024], {
  step: 1, value: 2024, label: "Year", width: 200,
}));
```

```js
const pKey = view(Inputs.select(
  Object.keys(PANELS),
  {label: "Panel", format: k => PANELS[k].label, value: "hybrid"}
));
```

</div>

<small style="color:#666">${PANELS[pKey]?.description ?? ""}</small>

---

## ${year} · ${PANELS[pKey]?.label} — Structures per sq mile

```js
// For the hybrid panel, filter the pre-loaded static data; other panels fetch from Supabase.
const yearData = pKey === "hybrid"
  ? hybridAllYears.filter(d => d.year === year)
  : await fetchYearSlice(pKey, year);

const valueCol = PANELS[pKey].col;

// Density: structures (or units) per square mile of tract land area.
const densityByGeoid = new Map(
  yearData
    .filter(d => d[valueCol] != null)
    .map(d => {
      const area = tractAreaSqMi.get(d.geoid) ?? 0;
      return [d.geoid, area > 0 ? d[valueCol] / area : null];
    })
);

// Log scale requires positive values; filter out zeros and nulls.
const densities = [...densityByGeoid.values()].filter(v => v != null && v > 0);
const [dMin, dMax] = [d3.min(densities), d3.max(densities)];
```

```js
// Build the tract selector BEFORE the map so the click handler below can reference it.
const geoidOptions = yearData
  .filter(d => d[valueCol] != null)
  .sort((a, b) => d3.descending(a[valueCol], b[valueCol]))
  .map(d => d.geoid);

const geoidInput = Inputs.select(
  [null, ...geoidOptions],
  {
    label: "Tract GEOID (or click a tract on the map)",
    format: g => g ?? "— click a tract, or select here —",
  }
);
```

```js
// Choropleth — no tract boundaries for cleaner density display.
// Click any tract to update the time-series below.
const mapEl = Plot.plot({
  width,
  height: 600,
  projection: {type: "mercator", domain: tracts},
  color: {
    type: "log",
    scheme: "YlOrRd",
    // Fixed domain across all years so color shifts are visible as the year changes.
    // For non-hybrid panels, fall back to per-year bounds if global bounds are unavailable.
    domain: [Math.max(0.01, globalDMin ?? dMin ?? 0.01), globalDMax ?? dMax ?? 1000],
    label: `${PANELS[pKey]?.label} — structures per sq mile (log scale)`,
    legend: true,
  },
  marks: [
    Plot.geo(tracts, {
      fill: feature => densityByGeoid.get(feature.properties.geoid) ?? NaN,
      stroke: "none",
      cursor: "pointer",
      title: feature => {
        const g = feature.properties.geoid;
        const density = densityByGeoid.get(g);
        const area = tractAreaSqMi.get(g);
        const metro = COUNTY_METRO.get(g.slice(0, 5));
        return [
          `Tract: ${g}`,
          metro ? `Metro: ${metro}` : "",
          density != null ? `Density: ${density.toLocaleString(undefined, {maximumFractionDigits: 1})} / sq mi` : "no data",
          area != null ? `Net residential area: ${area.toFixed(2)} sq mi` : "",
        ].filter(Boolean).join("\n");
      },
    }),
  ],
});

// Click a tract → update the selector and trigger the time-series.
// Observable Plot renders the `title` channel as a SVG <title> child on each
// <path>. Parsing the geoid from that element is reliable without needing
// D3 data binding (which Plot does not expose on geo paths).
mapEl.addEventListener("click", event => {
  const path = event.target.closest("path");
  const titleText = path?.querySelector("title")?.textContent ?? "";
  const match = titleText.match(/^Tract:\s*(\d{11})/);
  if (match) {
    geoidInput.value = match[1];
    geoidInput.dispatchEvent(new Event("input", {bubbles: true}));
  }
});

display(mapEl);
```

---

## Tract Detail: Time-Series

```js
// Render the tract selector and create the reactive selectedGeoid value.
const selectedGeoid = view(geoidInput);
```

```js
if (selectedGeoid) {
  // For hybrid, filter the already-loaded static data; other panels fetch from Supabase.
  const series = pKey === "hybrid"
    ? hybridAllYears.filter(d => d.geoid === selectedGeoid).sort((a, b) => a.year - b.year)
    : await fetchTractSeries(pKey, selectedGeoid);

  const yCol = PANELS[pKey].col;
  const hasUncertainty = PANELS[pKey].hasUncertainty;
  const area = tractAreaSqMi.get(selectedGeoid);
  const areaLabel = area != null ? ` (${area.toFixed(2)} sq mi net residential)` : "";

  display(Plot.plot({
    title: `Tract ${selectedGeoid}${areaLabel} · ${PANELS[pKey].label}`,
    subtitle: "Raw structure counts (not density-normalized)",
    width,
    height: 280,
    x: {label: "Year", tickFormat: "d"},
    y: {label: "Estimated Structures", grid: true},
    marks: [
      ...(hasUncertainty ? [
        Plot.areaY(series, {
          x: "year", y1: PANELS[pKey].p5, y2: PANELS[pKey].p95,
          fill: "#e07b39", fillOpacity: 0.2,
        }),
      ] : []),
      Plot.lineY(series, {x: "year", y: yCol, stroke: "#e07b39", strokeWidth: 2}),
      Plot.dot(series, {x: "year", y: yCol, fill: "#e07b39", r: 3}),
      Plot.ruleY([0]),
    ],
  }));

  display(Inputs.table(series.map(d => ({
    Year: d.year,
    [PANELS[pKey].label]: d[yCol]?.toLocaleString(undefined, {maximumFractionDigits: 0}) ?? "—",
    ...(hasUncertainty ? {
      P5: d[PANELS[pKey].p5]?.toLocaleString(undefined, {maximumFractionDigits: 0}) ?? "—",
      P95: d[PANELS[pKey].p95]?.toLocaleString(undefined, {maximumFractionDigits: 0}) ?? "—",
    } : {}),
  }))));
} else {
  display(html`<div class="tip">Click a tract on the map above, or select one from the dropdown.</div>`);
}
```

<style>
.tip { padding: 0.75rem 1rem; background: #f0f4ff; border-left: 4px solid #4c6ef5; border-radius: 4px; color: #444; }
</style>
