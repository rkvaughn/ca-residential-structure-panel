/**
 * supabase-client.js
 * ------------------
 * Supabase REST API helpers for the CA Residential Structure Panel dashboard.
 *
 * Authentication uses direct fetch() calls with apikey + Authorization headers
 * because the sb_publishable_* key format is not handled by @supabase/supabase-js
 * when loaded via npm: in Observable Framework.
 *
 * The hybrid panel (default) is loaded from a committed static JSON file
 * (FileAttachment("data/panel-hybrid.json")) to avoid CORS preflight issues
 * for the most-used panel. These Supabase helpers are used for the remaining
 * three panels (acs, point, arruda) and the overview statewide aggregate.
 */

const SUPABASE_URL = "https://pgoeeknfanpjwwqnwuyx.supabase.co";
// Anon/publishable key — safe to commit; read-only access enforced by RLS policies.
const SUPABASE_ANON_KEY = "sb_publishable_eP8T2qJx3aRTMR2fSjDpuQ_Z7zmRF5a";

/**
 * Panel configuration: maps panel key → Supabase table name and value columns.
 * `col` is the primary display column. `p5`/`p95` are uncertainty bounds (bootstrap panels only).
 */
export const PANELS = {
  hybrid: {
    label: "Arruda Hybrid (P50)",
    table: "panel_hybrid",
    col: "p50_residential_count",
    p5: "p5_residential_count",
    p95: "p95_residential_count",
    hasUncertainty: true,
    description: "Bootstrap median with Arruda calibration for 15 dense-urban counties (best estimate)",
  },
  acs: {
    label: "ACS B25001",
    table: "panel_acs",
    col: "acs_housing_units",
    hasUncertainty: false,
    description: "ACS housing unit counts (validation benchmark; units, not structures)",
  },
  point: {
    label: "Point Estimate",
    table: "panel_point",
    col: "residential_count_hindcast",
    hasUncertainty: false,
    description: "Deterministic hindcast without bootstrap uncertainty",
  },
  arruda: {
    label: "Arruda Hindcast",
    table: "panel_arruda",
    col: "residential_count_hindcast",
    hasUncertainty: false,
    description: "Pre-hybrid Arruda-anchored hindcast",
  },
};

/**
 * Returns the required HTTP headers for Supabase REST API requests.
 * @returns {Object} Headers object with apikey, Authorization, and Accept.
 */
function restHeaders() {
  return {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": `Bearer ${SUPABASE_ANON_KEY}`,
    "Accept": "application/json",
  };
}

/**
 * Performs a GET request to the Supabase REST API.
 * @param {string} path - PostgREST path + query string (e.g., "panel_acs?select=geoid&year=eq.2024")
 * @returns {Promise<Array>} Parsed JSON response array.
 * @throws {Error} If the HTTP response is not OK.
 */
async function restGet(path) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {headers: restHeaders()});
  if (!res.ok) throw new Error(`Supabase ${res.status}: ${await res.text()}`);
  return res.json();
}

/**
 * Fetches all tracts for a given panel and year. Used for non-hybrid choropleth panels.
 * Note: The hybrid panel uses a static FileAttachment instead of this function.
 *
 * @param {string} panelKey - Key from PANELS (e.g., "acs", "point", "arruda")
 * @param {number} year - Year to fetch (2010–2024)
 * @returns {Promise<Array>} Array of {geoid, county_fips, <value_col>} objects.
 */
export async function fetchYearSlice(panelKey, year) {
  const {table, col, p5, p95, hasUncertainty} = PANELS[panelKey];
  const selectCols = hasUncertainty
    ? `geoid,county_fips,${col},${p5},${p95}`
    : `geoid,county_fips,${col}`;
  return restGet(`${table}?select=${selectCols}&year=eq.${year}&limit=10000`);
}

/**
 * Fetches all years for a single tract. Used for the time-series detail panel
 * for non-hybrid panels. The hybrid panel filters its pre-loaded static data client-side.
 *
 * @param {string} panelKey - Key from PANELS (e.g., "acs", "point", "arruda")
 * @param {string} geoid - 11-digit Census tract FIPS string (e.g., "06037207400")
 * @returns {Promise<Array>} Array of {year, <value_col>} objects ordered by year.
 */
export async function fetchTractSeries(panelKey, geoid) {
  const {table, col, p5, p95, hasUncertainty} = PANELS[panelKey];
  const selectCols = hasUncertainty
    ? `year,${col},${p5},${p95}`
    : `year,${col}`;
  return restGet(`${table}?select=${selectCols}&geoid=eq.${geoid}&order=year`);
}

/**
 * Fetches the statewide aggregate time-series (15 rows) from the pre-computed view.
 * Used by the Overview page. This is a small query (15 rows) and does not
 * require the static data loader workaround used for the map.
 *
 * @returns {Promise<Array>} Array of {year, p50_total, p5_total, p95_total, n_tracts, n_counties}.
 */
export async function fetchStatewideByYear() {
  return restGet("panel_hybrid_annual?select=year,p50_total,p5_total,p95_total,n_tracts,n_counties&order=year");
}
