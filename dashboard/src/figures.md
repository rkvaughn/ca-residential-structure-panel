---
title: Figures
---

# Figures

All figures are outputs of the estimation pipeline. See the [paper](https://github.com/rkvaughn/ca-residential-structure-panel/blob/main/paper/structure_count_writeup.pdf) for discussion.

```js
const BASE = "https://raw.githubusercontent.com/rkvaughn/ca-residential-structure-panel/main/output/figures";

const figures = [
  {
    file: "fig_structure_count_maps.png",
    title: "Tract-Level Choropleth Maps",
    caption: "P5 / P50 / P95 residential structure counts for 2024 anchor year across all CA tracts.",
  },
  {
    file: "fig_structure_timeseries.png",
    title: "Statewide Time-Series",
    caption: "Statewide total residential structures (P50) with P5–P95 uncertainty band, 2010–2024.",
  },
  {
    file: "fig_bootstrap_uncertainty_map.png",
    title: "Bootstrap Uncertainty Map",
    caption: "Spatial pattern of IQR (interquartile range) of bootstrap draws, highlighting tracts with high estimation uncertainty.",
  },
  {
    file: "fig_calibration.png",
    title: "Beta Calibration Diagnostics",
    caption: "County-level Beta(α, β) parameter estimates. Points show calibrated absorption fractions vs. ACS benchmark.",
  },
  {
    file: "fig_county_scatter.png",
    title: "County-Level Scatter",
    caption: "Bootstrap P50 vs. ACS housing units by county. Systematic offset (~82%) reflects unit/structure distinction.",
  },
  {
    file: "fig_fire_county_comparison.png",
    title: "Wildfire County Comparison",
    caption: "Bootstrap vs. ACS counts for wildfire-affected counties (e.g., Butte). Bootstrap substantially outperforms the labeled-only point estimate.",
  },
  {
    file: "fig_arruda_comparison.png",
    title: "Arruda Comparison",
    caption: "County-level comparison: Bootstrap P50 vs. Arruda et al. (2024) OSM-derived counts. Spearman ρ = 0.916.",
  },
  {
    file: "fig_spaghetti_all_counties.png",
    title: "All Counties: Indexed Counts (2010–2024)",
    caption: "Indexed structure counts (2010 = 1.0) for all 58 CA counties showing growth trajectories.",
  },
  {
    file: "fig_spaghetti_dense_urban.png",
    title: "Dense-Urban Counties: Original vs. Arruda Hybrid",
    caption: "Comparison of original bootstrap and Arruda hybrid calibration for the 15 dense-urban counties.",
  },
  {
    file: "fig_spaghetti_rural.png",
    title: "Rural/Suburban Counties: Indexed (2010–2024)",
    caption: "Indexed structure counts for rural and suburban California counties.",
  },
  {
    file: "fig_structure_count_maps.png",
    title: "Structure Count Maps",
    caption: "Tract-level choropleth maps of P5, P50, and P95 structure counts.",
  },
];
```

```js
// Animated GIF
display(html`
  <div class="figure-card">
    <h3>County Animation: Top-30 Counties by P50 Count</h3>
    <img src="${BASE}/fig_structure_count_animation.gif" alt="Animated bar chart of top-30 counties by P50 residential structure count, 2010–2024" style="max-width:100%;border-radius:6px;">
    <p class="caption">Animated bar chart cycling through 2010–2024. Top 30 California counties ranked by P50 residential structure count.</p>
  </div>
`);
```

```js
// Static figure grid
display(html`
  <div class="figure-grid">
    ${figures.map(f => html`
      <div class="figure-card">
        <h3>${f.title}</h3>
        <a href="${BASE}/${f.file}" target="_blank">
          <img src="${BASE}/${f.file}" alt="${f.title}" loading="lazy">
        </a>
        <p class="caption">${f.caption}</p>
      </div>
    `)}
  </div>
`);
```

<style>
.figure-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 1.5rem;
  margin: 1.5rem 0;
}
.figure-card {
  background: var(--theme-background-alt);
  border-radius: 8px;
  padding: 1rem;
  border: 1px solid var(--theme-foreground-faintest);
}
.figure-card h3 {
  font-size: 0.95rem;
  margin: 0 0 0.5rem;
}
.figure-card img {
  width: 100%;
  border-radius: 4px;
  cursor: zoom-in;
}
.caption {
  font-size: 0.82rem;
  color: var(--theme-foreground-muted);
  margin: 0.5rem 0 0;
}
</style>
