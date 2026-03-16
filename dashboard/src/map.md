---
title: Map Explorer
---

# Map Explorer

Browse tract-level residential structure counts across California. Use the controls to select a year and estimation method. **Click a tract on the map** to see its 2010–2024 time-series below.

```js
import {PANELS, fetchYearSlice, fetchTractSeries} from "./components/supabase-client.js";

// Tract geometry — pre-computed GeoJSON, committed to repo (~4.7 MB)
const tracts = await FileAttachment("data/ca-tracts.json").json();

// Hybrid panel — all years loaded statically to avoid runtime CORS issues (~15 MB, gzipped ~2 MB)
const hybridAllYears = await FileAttachment("data/panel-hybrid.json").json();
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

## ${year} · ${PANELS[pKey]?.label}

```js
// For the hybrid panel, filter the pre-loaded static data; other panels fetch from Supabase.
const yearData = pKey === "hybrid"
  ? hybridAllYears.filter(d => d.year === year)
  : await fetchYearSlice(pKey, year);

const valueCol = PANELS[pKey].col;
const valueByGeoid = new Map(yearData.map(d => [d.geoid, d[valueCol]]));

// Log scale requires positive values; filter out zeros and nulls.
const values = yearData.map(d => d[valueCol]).filter(v => v > 0);
const [vMin, vMax] = [d3.min(values), d3.max(values)];
```

```js
// Build the tract selector BEFORE the map so the click handler below can reference it.
// All non-null tracts are included; sort descending by value so the most populated appear first.
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
// Choropleth map. Click any tract to update the time-series below.
const mapEl = Plot.plot({
  width,
  height: 600,
  projection: {type: "mercator", domain: tracts},
  color: {
    type: "log",
    scheme: "YlOrRd",
    domain: [Math.max(1, vMin ?? 1), vMax ?? 100],
    label: `${PANELS[pKey]?.label} (log scale)`,
    legend: true,
  },
  marks: [
    Plot.geo(tracts, {
      fill: feature => valueByGeoid.get(feature.properties.geoid) ?? NaN,
      stroke: "white",
      strokeWidth: 0.15,
      cursor: "pointer",
      title: feature => {
        const g = feature.properties.geoid;
        const v = valueByGeoid.get(g);
        return `Tract: ${g}\n${PANELS[pKey]?.label}: ${v != null ? v.toLocaleString(undefined, {maximumFractionDigits: 0}) : "no data"}`;
      },
    }),
  ],
});

// Click a tract on the map → update the selector and trigger the time-series.
// Observable Plot attaches the GeoJSON feature to __data__ on each <path> element.
mapEl.addEventListener("click", event => {
  const path = event.target.closest("path");
  const geoid = path?.__data__?.properties?.geoid;
  if (geoid) {
    geoidInput.value = geoid;
    geoidInput.dispatchEvent(new Event("input", {bubbles: true}));
  }
});

display(mapEl);
```

---

## Tract Detail: Time-Series

```js
// Render the tract selector and create the reactive selectedGeoid value.
// geoidInput was already built in the cell above so the map click handler could reference it.
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

  display(Plot.plot({
    title: `Tract ${selectedGeoid} · ${PANELS[pKey].label}`,
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
