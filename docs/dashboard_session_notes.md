# Dashboard Development Session Notes

_Last updated: 2026-03-15. Read this before resuming dashboard work._

---

## Current State

- **Dev server**: `cd dashboard && npm run dev -- --port 3456` → http://localhost:3456
- **Supabase**: Fully provisioned and populated (507,507 rows across 4 tables + `panel_hybrid_annual` view)
- **Map page**: Choropleth renders from static `panel-hybrid.json` FileAttachment (CORS issue bypassed). Click-to-select implemented. Dropdown includes all non-null tracts.
- **Overview page**: Uses `fetchStatewideByYear()` → Supabase `panel_hybrid_annual` view (15 rows). Not yet tested end-to-end.
- **Figures page**: Static images from GitHub raw URLs. No runtime data needed.
- **Downloads page**: Static markdown. No runtime data needed.

---

## Architecture Decisions

### 1. Static FileAttachment for hybrid panel (replaces runtime Supabase fetch for map)

**Why:** The `sb_publishable_*` key format is not supported by `@supabase/supabase-js` in Observable Framework's `npm:` loader. Direct `fetch()` with `apikey` + `Authorization: Bearer` headers works in curl but is blocked by CORS preflight in the browser (non-simple headers trigger OPTIONS request).

**Solution:** Pre-generate `dashboard/src/data/panel-hybrid.json` locally using `scripts/generate_panel_hybrid_json.py` and commit it. Observable Framework serves this as a static asset via `FileAttachment("data/panel-hybrid.json")`. No runtime network call needed for the default (hybrid) panel.

**Re-generating:** Run `scripts/generate_panel_hybrid_json.py` whenever the hybrid panel parquet changes, then commit the updated JSON.

**File:** `dashboard/src/data/panel-hybrid.json` — 14.8 MB uncompressed, ~2 MB gzipped.

### 2. Supabase REST API for non-hybrid panels and overview

`supabase-client.js` still provides `fetchYearSlice()` and `fetchTractSeries()` for the ACS, Point, and Arruda panels, and `fetchStatewideByYear()` for the overview statewide aggregate. If CORS continues to block these in-browser, the same static FileAttachment pattern can be extended.

**Files to create if needed:**
- `scripts/generate_panel_acs_json.py` → `dashboard/src/data/panel-acs.json`
- `scripts/generate_panel_point_json.py` → `dashboard/src/data/panel-point.json`
- `scripts/generate_panel_arruda_json.py` → `dashboard/src/data/panel-arruda.json`
- `scripts/generate_panel_hybrid_annual_json.py` → `dashboard/src/data/panel-hybrid-annual.json`

### 3. Map click-to-select interaction

The map page now supports clicking any tract to update the tract selector and trigger the time-series. Implementation:
- `geoidInput` is defined (but not displayed) BEFORE the map cell
- Map click handler fires `geoidInput.dispatchEvent(new Event("input", {bubbles: true}))` to trigger Observable Framework's reactive update of `selectedGeoid`
- `view(geoidInput)` is called AFTER the map in the "Tract Detail" section — this displays the input and creates the reactive `selectedGeoid` value
- Dropdown includes all non-null tracts (removed the 500-item limit from prior version)

### 4. `@supabase/supabase-js` → direct fetch

Replaced mid-session. `sb_publishable_*` key format not handled correctly. See session 4 history above.

---

## Files Modified (not yet committed)

| File | Change |
|---|---|
| `dashboard/src/components/supabase-client.js` | Replaced supabase-js with direct fetch; added JSDoc |
| `dashboard/src/index.md` | Updated to use `fetchStatewideByYear()` |
| `dashboard/src/map.md` | FileAttachment for hybrid, click-to-select, removed DEBUG lines |
| `dashboard/package.json` | Added `"type": "module"` to suppress Node.js warning |
| `dashboard/src/data/panel-hybrid.json` | NEW — 14.8 MB committed static JSON |
| `scripts/generate_panel_hybrid_json.py` | NEW — local generator for panel-hybrid.json |
| `scripts/generate_ca_tracts.py` | NEW — already-run; generated ca-tracts.json |
| `scripts/import_to_supabase.py` | NEW — already-run; imported 507,507 rows |
| `.gitignore` | Added `.env` |
| `.env` | Created (gitignored) with Supabase credentials |
| `dashboard/` | Entire new directory (Observable Framework app) |
| `.github/workflows/deploy-dashboard.yml` | NEW — GitHub Pages deploy |
| `CLAUDE.md` | Status table updated; directory structure updated |

**Do not commit `.env`.**

---

## GitHub Actions CI

The workflow (`deploy-dashboard.yml`) runs `npm run build` in `dashboard/`. Since `panel-hybrid.json` is committed (not a data loader `.json.py`), no Python step is needed in CI. Observable Framework just copies the JSON file to `dist/`.

---

## Supabase Credentials

Stored in `.env` at project root (gitignored). All 4 keys are present. Service role key was used once for the import and is not needed again unless re-importing.

```
SUPABASE_URL=https://pgoeeknfanpjwwqnwuyx.supabase.co
SUPABASE_ANON_KEY=sb_publishable_eP8T2qJx3aRTMR2fSjDpuQ_Z7zmRF5a
```

---

## Next Steps

1. **Test all 4 pages** locally (dev server running on port 3456)
   - http://localhost:3456 → Overview (check if statewide chart loads from Supabase)
   - http://localhost:3456/map → Map (choropleth, click-to-select, time-series)
   - http://localhost:3456/figures → Figures gallery
   - http://localhost:3456/downloads → Downloads
2. **If overview Supabase call fails** (CORS) → create `panel-hybrid-annual.json` static fallback
3. **Commit** everything except `.env`
4. **Enable GitHub Pages**: Repo Settings → Pages → Source → GitHub Actions
5. **Push to `main`** → CI deploys automatically
6. Update `CLAUDE.md` dashboard row to `Complete`
7. **Figure caption standard** — user requested; not yet implemented
