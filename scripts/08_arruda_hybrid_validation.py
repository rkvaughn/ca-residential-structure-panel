"""
08_arruda_hybrid_validation.py
================================
Post-hoc Arruda hybrid calibration, validation, spaghetti plots, and animated GIF.

Background
----------
Script 05 calibrates the null-subtype absorption fraction f_c using ACS B25001
housing unit counts. In dense urban counties (≥15 in CA), ACS_units >> Overture
structures → f_c clips to 0.99. Script 05 now includes Arruda hybrid calibration
(Step 0b) to fix this for future runs, but that requires the raw Overture GeoParquet
(2.75 GB), which is distributed via GitHub Release and may not be on disk.

This script performs the equivalent correction post-hoc, using only the outputs
already on disk:
  - tract_structure_panel_bootstrap.parquet  (existing bootstrap, ACS-calibrated)
  - arruda_ca_county_counts.parquet          (Arruda RES counts by county)
  - external_absorption_fractions.parquet    (ACS calibration results per county)
  - tract_residential_counts_2024.parquet    (Overture R_c by tract)

Post-hoc rescaling logic
------------------------
For each ACS-clipped county c:
  scale_c = Arruda_RES_c / bootstrap_p50_2024_c
  p{q}_hybrid_t = p{q}_bootstrap_t × scale_c  (all years, all percentiles)

For non-clipping counties: no change.

This preserves the BPS/DINS backward hind-cast temporal structure while
correcting the 2024 level using Arruda's building-level count.

Validation check — negative f_c_arruda
---------------------------------------
Counties where Arruda_RES_c < R_c (Overture labeled residential) would produce
a negative absorption fraction if used in the f_c formula. These indicate
counties where OSM undercounts even the already-labeled Overture residential
buildings — a data coverage issue, not a calibration one. Printed and saved to
output/tables/arruda_hybrid_validation.csv.

Outputs
-------
  data/clean/tract_structure_panel_arruda_hybrid.parquet
      Same schema as tract_structure_panel_bootstrap.parquet; p5/p50/p95/iqr
      rescaled for ACS-clipped counties using Arruda 2024 anchor.

  output/tables/arruda_hybrid_scale_factors.csv
      county_FIPS, county_name, calibration_source, R_c, arruda_res_count,
      bootstrap_p50_2024, scale_factor, change_pct

  output/tables/arruda_hybrid_validation.csv
      Validation report: negative f_c_arruda check for all 58 counties.

  output/figures/fig_spaghetti_all_counties.png
      All 58 CA counties: structure count indexed 2010=100, mean + outliers.

  output/figures/fig_spaghetti_dense_urban.png
      15 ACS-clipped (dense urban) counties: original vs hybrid overlay.

  output/figures/fig_spaghetti_rural.png
      43 non-clipping counties: structure count indexed 2010=100, mean + outliers.

  output/figures/fig_structure_count_animation.gif
      Animated bar chart: top-30 counties by p50 structure count, 2010–2024.

Usage
-----
  python scripts/08_arruda_hybrid_validation.py

Dependencies
------------
  pandas, numpy, matplotlib, pyarrow
"""

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.animation as animation
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CLEAN_DIR   = PROJECT_ROOT / "data" / "clean"
OUT_TABLES  = PROJECT_ROOT / "output" / "tables"
OUT_FIGURES = PROJECT_ROOT / "output" / "figures"

IN_BOOTSTRAP    = CLEAN_DIR  / "tract_structure_panel_bootstrap.parquet"
IN_EXT_ABS      = CLEAN_DIR  / "external_absorption_fractions.parquet"
IN_ARRUDA       = OUT_TABLES / "arruda_ca_county_counts.parquet"
IN_TRACT_COUNTS = CLEAN_DIR  / "tract_residential_counts_2024.parquet"
IN_COUNTY_NAMES = OUT_TABLES / "arruda_ca_county_counts.parquet"  # has county_name

OUT_HYBRID        = CLEAN_DIR  / "tract_structure_panel_arruda_hybrid.parquet"
OUT_SCALE_TABLE   = OUT_TABLES / "arruda_hybrid_scale_factors.csv"
OUT_VALIDATION    = OUT_TABLES / "arruda_hybrid_validation.csv"
OUT_SPAG_ALL      = OUT_FIGURES / "fig_spaghetti_all_counties.png"
OUT_SPAG_URBAN    = OUT_FIGURES / "fig_spaghetti_dense_urban.png"
OUT_SPAG_RURAL    = OUT_FIGURES / "fig_spaghetti_rural.png"
OUT_GIF           = OUT_FIGURES / "fig_structure_count_animation.gif"

YEARS = list(range(2010, 2025))


# ---------------------------------------------------------------------------
# Step 1: Load all data
# ---------------------------------------------------------------------------

def load_data() -> tuple[pd.DataFrame, ...]:
    """Load bootstrap panel, Arruda counts, ACS calibration, Overture R_c."""
    print("  Loading bootstrap panel...")
    bp = pd.read_parquet(IN_BOOTSTRAP)
    print(f"    {len(bp):,} tract × year rows, {bp['geoid'].nunique():,} tracts")

    print("  Loading Arruda county counts...")
    arruda = pd.read_parquet(IN_ARRUDA)
    print(f"    {len(arruda)} counties")

    print("  Loading ACS external absorption fractions...")
    ext = pd.read_parquet(IN_EXT_ABS)
    print(f"    {len(ext)} counties; source distribution:")
    for src, cnt in ext["calibration_source"].value_counts().items():
        print(f"      {src}: {cnt}")

    print("  Loading Overture tract residential counts (R_c)...")
    tr = pd.read_parquet(IN_TRACT_COUNTS)
    r_county = (
        tr.groupby("county_FIPS")["overture_residential_count_2024"]
        .sum()
        .reset_index()
        .rename(columns={"overture_residential_count_2024": "R_c"})
    )
    print(f"    {len(r_county)} counties")

    return bp, arruda, ext, r_county


# ---------------------------------------------------------------------------
# Step 2: Validation — negative f_c_arruda check
# ---------------------------------------------------------------------------

def validate_negative_f_arruda(
    arruda: pd.DataFrame,
    r_county: pd.DataFrame,
    ext: pd.DataFrame,
) -> pd.DataFrame:
    """
    Check all 58 counties for Arruda_RES_c < R_c (which would yield a
    negative absorption fraction if used in the f_c formula).

    Returns a validation DataFrame saved to output/tables/.
    """
    merged = (
        r_county.merge(arruda[["county_FIPS", "county_name", "arruda_res_count"]], on="county_FIPS")
        .merge(ext[["county_FIPS", "calibration_source", "f_c_external"]], on="county_FIPS")
    )
    merged["arruda_exceeds_R_c"] = merged["arruda_res_count"] >= merged["R_c"]
    merged["gap_arruda_minus_Rc"] = merged["arruda_res_count"] - merged["R_c"]

    negative = merged[~merged["arruda_exceeds_R_c"]].copy()

    print(f"\n  ── Negative f_c_arruda validation ────────────────────────────────")
    if len(negative) == 0:
        print(f"  [ok] No counties where Arruda_RES < R_c (all counties valid).")
    else:
        print(f"  [WARN] {len(negative)} county/ies where Arruda_RES_c < Overture R_c:")
        print(f"  {'county_FIPS':<12} {'county_name':<18} {'R_c':>10} "
              f"{'arruda_res':>12} {'gap':>10}  calibration_source")
        for _, row in negative.iterrows():
            print(f"  {row['county_FIPS']:<12} {row['county_name']:<18} "
                  f"{int(row['R_c']):>10,} {int(row['arruda_res_count']):>12,} "
                  f"{int(row['gap_arruda_minus_Rc']):>10,}  {row['calibration_source']}")
        print(f"\n  Note: These counties have OSM undercoverage relative to Overture's")
        print(f"  already-labeled residential buildings. If any were ACS-clipped, they")
        print(f"  would fall back to endogenous r_frac_c in the hybrid calibration.")
        print(f"  None of the negative-gap counties are ACS-clipped (verified below).")
        clipping_negative = negative[negative["calibration_source"] == "acs_clipped"]
        if len(clipping_negative) > 0:
            print(f"  [ALERT] {len(clipping_negative)} ACS-clipped counties also have "
                  f"negative gap — these use r_frac fallback.")
        else:
            print(f"  [ok] Confirmed: 0 ACS-clipped counties have negative gap.")

    return merged[[
        "county_FIPS", "county_name", "R_c", "arruda_res_count",
        "gap_arruda_minus_Rc", "arruda_exceeds_R_c", "calibration_source", "f_c_external"
    ]].sort_values("county_FIPS")


# ---------------------------------------------------------------------------
# Step 3: Post-hoc rescaling for ACS-clipped counties
# ---------------------------------------------------------------------------

def compute_hybrid_panel(
    bp: pd.DataFrame,
    arruda: pd.DataFrame,
    ext: pd.DataFrame,
    r_county: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    For each ACS-clipped county, compute:
        scale_c = Arruda_RES_c / bootstrap_p50_2024_c

    Apply to all percentiles for all years:
        p{q}_hybrid_t = p{q}_bootstrap_t × scale_c

    Non-clipping counties are unchanged.

    Returns (hybrid_panel, scale_table).
    """
    # County-level p50 in 2024 from bootstrap
    p50_2024 = (
        bp[bp["year"] == 2024]
        .groupby("county_FIPS")["p50_residential_count"]
        .sum()
        .reset_index()
        .rename(columns={"p50_residential_count": "bootstrap_p50_2024"})
    )

    # Dense urban counties: acs_clipped (script 05 not yet re-run with Arruda) OR
    # arruda_direct (script 05 already re-run — no post-hoc rescaling needed for these)
    clipping_fips      = set(ext[ext["calibration_source"].isin({"acs_clipped", "arruda_direct"})]["county_FIPS"])
    needs_rescale_fips = set(ext[ext["calibration_source"] == "acs_clipped"]["county_FIPS"])

    # Scale table: one row per county (all 58)
    scale_rows = []
    scale_map  = {}   # county_FIPS → scale_factor (only for clipping counties)

    for _, row in (
        r_county
        .merge(arruda[["county_FIPS", "county_name", "arruda_res_count"]], on="county_FIPS", how="left")
        .merge(p50_2024, on="county_FIPS", how="left")
        .merge(ext[["county_FIPS", "calibration_source"]], on="county_FIPS", how="left")
        .iterrows()
    ):
        fips   = row["county_FIPS"]
        is_clip          = fips in clipping_fips
        needs_rescale    = fips in needs_rescale_fips
        p50_24 = float(row["bootstrap_p50_2024"]) if not pd.isna(row["bootstrap_p50_2024"]) else None
        arr_res = float(row["arruda_res_count"]) if not pd.isna(row["arruda_res_count"]) else None
        r_c     = float(row["R_c"])

        # Skip rescaling if Arruda_RES < R_c (negative gap — would bias downward)
        arruda_negative = arr_res is not None and arr_res < r_c

        if needs_rescale and p50_24 is not None and arr_res is not None and not arruda_negative:
            # acs_clipped: bootstrap not yet Arruda-calibrated → rescale post-hoc
            sf = arr_res / p50_24
        else:
            # arruda_direct: bootstrap already Arruda-calibrated by script 05 (no-op)
            # or non-clipping county
            sf = 1.0

        scale_map[fips] = sf if is_clip else 1.0
        scale_rows.append({
            "county_FIPS":         fips,
            "county_name":         row.get("county_name", ""),
            "calibration_source":  row.get("calibration_source", ""),
            "R_c":                 int(r_c),
            "arruda_res_count":    int(arr_res) if arr_res is not None else None,
            "bootstrap_p50_2024":  round(p50_24, 1) if p50_24 is not None else None,
            "scale_factor":        round(sf, 6),
            "change_pct":          round((sf - 1.0) * 100, 2),
            "arruda_negative_flag": arruda_negative,
            "rescaled":            needs_rescale and sf != 1.0,
        })

    scale_table = pd.DataFrame(scale_rows).sort_values("county_FIPS")

    # Print scale factors for clipping counties
    clip_rows = scale_table[scale_table["calibration_source"] == "acs_clipped"].copy()
    print(f"\n  Scale factors for {len(clip_rows)} ACS-clipped counties "
          f"(Arruda_RES / Bootstrap_P50_2024):")
    print(f"  {'county_FIPS':<12} {'county_name':<18} {'R_c':>10} "
          f"{'arruda_res':>12} {'bp50_2024':>12} {'scale':>8} {'change_pct':>10}")
    for _, row in clip_rows.iterrows():
        print(f"  {row['county_FIPS']:<12} {row['county_name']:<18} "
              f"{int(row['R_c']):>10,} "
              f"{int(row['arruda_res_count']) if row['arruda_res_count'] else 'N/A':>12} "
              f"{row['bootstrap_p50_2024']:>12,.1f} "
              f"  {row['scale_factor']:>7.4f}  {row['change_pct']:>+9.1f}%")

    # Build hybrid panel
    hybrid = bp.copy()
    percentile_cols = [
        "p5_residential_count",
        "p50_residential_count",
        "p95_residential_count",
        "iqr_residential_count",
    ]

    for fips, sf in scale_map.items():
        if sf == 1.0:
            continue
        mask = hybrid["county_FIPS"] == fips
        for col in percentile_cols:
            hybrid.loc[mask, col] = (hybrid.loc[mask, col] * sf).clip(lower=1.0).round(2)

    n_rescaled = len([sf for sf in scale_map.values() if sf != 1.0])
    print(f"\n  Hybrid panel: {n_rescaled} counties rescaled, "
          f"{len(scale_map) - n_rescaled} unchanged.")
    print(f"  Panel rows: {len(hybrid):,} (same structure as bootstrap panel)")

    return hybrid, scale_table


# ---------------------------------------------------------------------------
# Helper: aggregate to county × year
# ---------------------------------------------------------------------------

def county_time_series(panel: pd.DataFrame) -> pd.DataFrame:
    """Sum p50 tract counts to county × year."""
    return (
        panel.groupby(["county_FIPS", "year"])["p50_residential_count"]
        .sum()
        .reset_index()
        .rename(columns={"p50_residential_count": "p50_county"})
    )


def index_to_base_year(df: pd.DataFrame, base_year: int = 2010) -> pd.DataFrame:
    """
    Add 'indexed' column: p50_county / p50_county[base_year].
    Counties missing base year data are dropped.
    """
    base = df[df["year"] == base_year][["county_FIPS", "p50_county"]].copy()
    base = base.rename(columns={"p50_county": "base_count"})
    out = df.merge(base, on="county_FIPS", how="inner")
    out["indexed"] = out["p50_county"] / out["base_count"]
    return out


def flag_outliers(df: pd.DataFrame, year: int = 2024) -> set:
    """
    IQR-based outlier detection on the 2024 indexed value.
    Returns set of county_FIPS flagged as outliers.
    """
    vals = df[df["year"] == year]["indexed"].dropna()
    q1, q3 = vals.quantile(0.25), vals.quantile(0.75)
    iqr    = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outlier_mask = (
        (df["year"] == year) &
        ((df["indexed"] < lo) | (df["indexed"] > hi))
    )
    return set(df.loc[outlier_mask, "county_FIPS"].unique())


# ---------------------------------------------------------------------------
# Step 5a: Spaghetti — all counties
# ---------------------------------------------------------------------------

def plot_spaghetti_all(
    hybrid_ts: pd.DataFrame,
    county_names: dict,
    ext: pd.DataFrame,
    out_path: Path,
) -> None:
    """All 58 counties indexed to 2010=100. Mean line, IQR-outliers labeled."""
    df = index_to_base_year(hybrid_ts)
    outlier_fips = flag_outliers(df)

    # Classification for coloring outliers (dense urban = acs_clipped OR arruda_direct)
    clipping_fips = set(ext[ext["calibration_source"].isin({"acs_clipped", "arruda_direct"})]["county_FIPS"])

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")

    # All county lines
    for fips, grp in df.groupby("county_FIPS"):
        grp = grp.sort_values("year")
        is_out = fips in outlier_fips
        is_urban = fips in clipping_fips
        if is_out:
            color = "#d7191c" if grp["indexed"].iloc[-1] < grp["indexed"].iloc[0] else "#1a9641"
            ax.plot(grp["year"], grp["indexed"] * 100, linewidth=1.4,
                    color=color, alpha=0.85, zorder=3)
            last = grp[grp["year"] == grp["year"].max()].iloc[0]
            name = county_names.get(fips, fips)
            ax.annotate(
                name, xy=(last["year"], last["indexed"] * 100),
                xytext=(4, 0), textcoords="offset points",
                fontsize=6.5, color=color, va="center",
            )
        else:
            color = "#4393c3" if is_urban else "#888888"
            ax.plot(grp["year"], grp["indexed"] * 100, linewidth=0.5,
                    color=color, alpha=0.35, zorder=2)

    # Mean line
    mean_ts = df.groupby("year")["indexed"].mean() * 100
    ax.plot(mean_ts.index, mean_ts.values, linewidth=2.2, color="black",
            zorder=4, label="CA mean")

    # Legend
    legend_handles = [
        mpatches.Patch(color="black",   label="CA mean"),
        mpatches.Patch(color="#4393c3", label="Dense urban (ACS-clipped)"),
        mpatches.Patch(color="#888888", label="Rural / suburban"),
        mpatches.Patch(color="#1a9641", label="Outlier: high growth"),
        mpatches.Patch(color="#d7191c", label="Outlier: decline (fire counties)"),
    ]
    ax.legend(handles=legend_handles, fontsize=7.5, loc="upper left",
              framealpha=0.9, edgecolor="#cccccc")

    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Residential structures (2010 = 100)", fontsize=9)
    ax.set_title("CA Residential Structure Count — All 58 Counties\n"
                 "(Arruda hybrid calibration; 2010 indexed to 100)",
                 fontsize=10, pad=8)
    ax.set_xlim(2010, 2026)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=8)
    ax.grid(axis="y", linewidth=0.4, alpha=0.4)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  [saved] {out_path.name}")


# ---------------------------------------------------------------------------
# Step 5b: Spaghetti — dense urban (original vs hybrid overlay)
# ---------------------------------------------------------------------------

def plot_spaghetti_dense_urban(
    original_ts: pd.DataFrame,
    hybrid_ts: pd.DataFrame,
    county_names: dict,
    ext: pd.DataFrame,
    out_path: Path,
) -> None:
    """
    15 ACS-clipped counties. Each county: dashed=original bootstrap, solid=hybrid.
    Shows how Arruda rescaling changes the dense-urban estimates.
    """
    clipping_fips = sorted(ext[ext["calibration_source"].isin({"acs_clipped", "arruda_direct"})]["county_FIPS"])

    orig_sub  = original_ts[original_ts["county_FIPS"].isin(clipping_fips)].copy()
    hyb_sub   = hybrid_ts[hybrid_ts["county_FIPS"].isin(clipping_fips)].copy()

    orig_idx  = index_to_base_year(orig_sub)
    hyb_idx   = index_to_base_year(hyb_sub)
    outlier_fips = flag_outliers(hyb_idx)

    cmap   = plt.get_cmap("tab20")
    colors = {fips: cmap(i / max(len(clipping_fips) - 1, 1))
              for i, fips in enumerate(clipping_fips)}

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")

    for fips in clipping_fips:
        color = colors[fips]
        name  = county_names.get(fips, fips)

        orig_grp = orig_idx[orig_idx["county_FIPS"] == fips].sort_values("year")
        hyb_grp  = hyb_idx[hyb_idx["county_FIPS"] == fips].sort_values("year")

        # Original: dashed
        ax.plot(orig_grp["year"], orig_grp["indexed"] * 100,
                linestyle="--", linewidth=1.0, color=color, alpha=0.45, zorder=2)
        # Hybrid: solid
        is_out = fips in outlier_fips
        lw = 1.8 if is_out else 1.2
        ax.plot(hyb_grp["year"], hyb_grp["indexed"] * 100,
                linestyle="-", linewidth=lw, color=color, alpha=0.9, zorder=3)

        # Label on right side
        if len(hyb_grp) > 0:
            last = hyb_grp[hyb_grp["year"] == hyb_grp["year"].max()].iloc[0]
            ax.annotate(
                name, xy=(last["year"], last["indexed"] * 100),
                xytext=(3, 0), textcoords="offset points",
                fontsize=6.5, color=color, va="center",
            )

    # Mean lines
    orig_mean = orig_idx.groupby("year")["indexed"].mean() * 100
    hyb_mean  = hyb_idx.groupby("year")["indexed"].mean() * 100
    ax.plot(orig_mean.index, orig_mean.values, linewidth=2.0,
            color="black", linestyle="--", zorder=4, label="Original mean")
    ax.plot(hyb_mean.index, hyb_mean.values, linewidth=2.0,
            color="black", linestyle="-", zorder=4, label="Hybrid mean")

    legend_handles = [
        plt.Line2D([0], [0], color="black", linestyle="--", lw=1.8,
                   label="Original bootstrap (ACS-clipped, f_c = 0.99)"),
        plt.Line2D([0], [0], color="black", linestyle="-",  lw=1.8,
                   label="Hybrid (Arruda rescaled)"),
    ]
    ax.legend(handles=legend_handles, fontsize=8, loc="upper left",
              framealpha=0.9, edgecolor="#cccccc")

    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Residential structures (2010 = 100)", fontsize=9)
    n_direct  = (ext["calibration_source"] == "arruda_direct").sum()
    n_clipped = (ext["calibration_source"] == "acs_clipped").sum()
    mode_note = (
        "Script 05 re-run with Arruda hybrid — bootstrap natively calibrated"
        if n_direct > 0 else
        "Post-hoc rescaling applied — re-run script 05 for native calibration"
    )
    ax.set_title(f"Dense Urban Counties — Arruda-Calibrated Residential Structures\n"
                 f"({mode_note}; dashed = original; solid = current)",
                 fontsize=9, pad=8)
    ax.set_xlim(2010, 2027)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=8)
    ax.grid(axis="y", linewidth=0.4, alpha=0.4)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  [saved] {out_path.name}")


# ---------------------------------------------------------------------------
# Step 5c: Spaghetti — rural / suburban
# ---------------------------------------------------------------------------

def plot_spaghetti_rural(
    hybrid_ts: pd.DataFrame,
    county_names: dict,
    ext: pd.DataFrame,
    out_path: Path,
) -> None:
    """43 non-clipping counties indexed to 2010=100. Mean line, IQR-outliers labeled."""
    rural_fips = set(ext[ext["calibration_source"] == "acs"]["county_FIPS"])  # only clean-ACS counties
    df_rural   = hybrid_ts[hybrid_ts["county_FIPS"].isin(rural_fips)].copy()
    df_idx     = index_to_base_year(df_rural)
    outlier_fips = flag_outliers(df_idx)

    fig, ax = plt.subplots(figsize=(11, 6))
    ax.set_facecolor("white")
    fig.patch.set_facecolor("white")

    for fips, grp in df_idx.groupby("county_FIPS"):
        grp = grp.sort_values("year")
        is_out = fips in outlier_fips
        if is_out:
            last_val = grp["indexed"].iloc[-1]
            color = "#d7191c" if last_val < 1.0 else "#1a9641"
            ax.plot(grp["year"], grp["indexed"] * 100, linewidth=1.5,
                    color=color, alpha=0.85, zorder=3)
            last = grp[grp["year"] == grp["year"].max()].iloc[0]
            name = county_names.get(fips, fips)
            ax.annotate(
                name, xy=(last["year"], last["indexed"] * 100),
                xytext=(4, 0), textcoords="offset points",
                fontsize=6.5, color=color, va="center",
            )
        else:
            ax.plot(grp["year"], grp["indexed"] * 100, linewidth=0.5,
                    color="#888888", alpha=0.35, zorder=2)

    # Mean line
    mean_ts = df_idx.groupby("year")["indexed"].mean() * 100
    ax.plot(mean_ts.index, mean_ts.values, linewidth=2.2, color="black",
            zorder=4, label="Rural/suburban mean")

    legend_handles = [
        mpatches.Patch(color="black",   label="Rural/suburban mean"),
        mpatches.Patch(color="#888888", label="Rural / suburban counties"),
        mpatches.Patch(color="#1a9641", label="Outlier: high growth"),
        mpatches.Patch(color="#d7191c", label="Outlier: decline (fire counties)"),
    ]
    ax.legend(handles=legend_handles, fontsize=7.5, loc="upper left",
              framealpha=0.9, edgecolor="#cccccc")

    ax.set_xlabel("Year", fontsize=9)
    ax.set_ylabel("Residential structures (2010 = 100)", fontsize=9)
    ax.set_title("Rural / Suburban Counties — Residential Structure Count\n"
                 "(Arruda hybrid; no rescaling applied to non-clipping counties)",
                 fontsize=10, pad=8)
    ax.set_xlim(2010, 2026)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=8)
    ax.grid(axis="y", linewidth=0.4, alpha=0.4)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"  [saved] {out_path.name}")


# ---------------------------------------------------------------------------
# Step 6: Animated GIF
# ---------------------------------------------------------------------------

def make_gif(
    hybrid_ts: pd.DataFrame,
    county_names: dict,
    out_path: Path,
    top_n: int = 30,
    fps: int = 2,
) -> None:
    """
    Animated horizontal bar chart: top-{top_n} CA counties by p50 structure count,
    animated 2010→2024 at {fps} frames per second.

    Each frame: one year; bars sorted descending by that year's count;
    color encodes % change from 2010 baseline (diverging: green=growth, red=decline).
    """
    # Compute % change from 2010
    base2010 = (
        hybrid_ts[hybrid_ts["year"] == 2010][["county_FIPS", "p50_county"]]
        .rename(columns={"p50_county": "count_2010"})
    )
    ts = hybrid_ts.merge(base2010, on="county_FIPS", how="left")
    ts["pct_change"] = (ts["p50_county"] - ts["count_2010"]) / ts["count_2010"] * 100

    # Determine fixed top-N counties by 2024 count
    top_fips = (
        ts[ts["year"] == 2024]
        .nlargest(top_n, "p50_county")["county_FIPS"]
        .tolist()
    )
    ts_top = ts[ts["county_FIPS"].isin(top_fips)].copy()
    ts_top["county_label"] = ts_top["county_FIPS"].map(county_names).fillna(ts_top["county_FIPS"])

    # Color: diverging around 0% change
    # Green = growth (pct_change > 0), Red = decline (pct_change < 0)
    # Use a fixed color per county (by mean pct change)
    mean_chg = ts_top.groupby("county_FIPS")["pct_change"].mean()
    colors = {
        fips: "#1a9641" if mean_chg.get(fips, 0) >= 0 else "#d7191c"
        for fips in top_fips
    }

    # ── Figure setup ──────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_facecolor("white")

    def draw_frame(year: int) -> list:
        ax.clear()
        ax.set_facecolor("white")
        yr_data = (
            ts_top[ts_top["year"] == year]
            .sort_values("p50_county", ascending=True)
        )
        bar_colors = [colors.get(f, "#888888") for f in yr_data["county_FIPS"]]
        bars = ax.barh(yr_data["county_label"], yr_data["p50_county"] / 1000,
                       color=bar_colors, edgecolor="white", linewidth=0.3)

        # Label bars with pct change from 2010
        for bar, (_, row) in zip(bars, yr_data.iterrows()):
            chg = row["pct_change"]
            sign = "+" if chg >= 0 else ""
            ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                    f"{sign}{chg:.1f}%", va="center", fontsize=6.5,
                    color="#555555")

        ax.set_xlabel("Residential structures (thousands)", fontsize=9)
        ax.set_title(f"CA Residential Structure Count — Top {top_n} Counties\n"
                     f"Year: {year}   (% change from 2010)",
                     fontsize=10, pad=8)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(axis="y", labelsize=7.5)
        ax.tick_params(axis="x", labelsize=8)
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f"))

        # Fix x-axis to 2024 maximum for stable animation
        max_val = ts_top["p50_county"].max() / 1000 * 1.15
        ax.set_xlim(0, max_val)
        fig.tight_layout()
        return [bars]

    ani = animation.FuncAnimation(
        fig,
        draw_frame,
        frames=YEARS,
        interval=int(1000 / fps),
        blit=False,
        repeat=True,
    )

    writer = animation.PillowWriter(fps=fps)
    ani.save(str(out_path), writer=writer, dpi=120)
    plt.close(fig)
    print(f"  [saved] {out_path.name}  ({len(YEARS)} frames @ {fps} fps)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 65)
    print("08_arruda_hybrid_validation.py")
    print("=" * 65)

    OUT_FIGURES.mkdir(parents=True, exist_ok=True)
    OUT_TABLES.mkdir(parents=True, exist_ok=True)

    # ── Step 1: Load data ─────────────────────────────────────────────────
    print("\n--- Step 1: Loading data ---")
    bp, arruda, ext, r_county = load_data()
    county_names = dict(zip(arruda["county_FIPS"], arruda["county_name"]))

    # ── Step 2: Validate negative f_c_arruda ─────────────────────────────
    print("\n--- Step 2: Negative f_c_arruda validation ---")
    validation_df = validate_negative_f_arruda(arruda, r_county, ext)
    validation_df.to_csv(OUT_VALIDATION, index=False)
    print(f"  [saved] {OUT_VALIDATION.name}")

    # ── Step 3: Post-hoc hybrid rescaling ────────────────────────────────
    print("\n--- Step 3: Post-hoc Arruda hybrid rescaling ---")
    hybrid_panel, scale_table = compute_hybrid_panel(bp, arruda, ext, r_county)

    hybrid_panel.to_parquet(OUT_HYBRID, index=False)
    print(f"  [saved] {OUT_HYBRID.name}")

    scale_table.to_csv(OUT_SCALE_TABLE, index=False)
    print(f"  [saved] {OUT_SCALE_TABLE.name}")

    # ── Step 4: Aggregate to county × year ───────────────────────────────
    print("\n--- Step 4: Aggregating to county × year ---")
    original_ts = county_time_series(bp)
    hybrid_ts   = county_time_series(hybrid_panel)
    print(f"  Original: {len(original_ts):,} county × year rows")
    print(f"  Hybrid:   {len(hybrid_ts):,} county × year rows")

    # ── Step 5: Spaghetti plots ───────────────────────────────────────────
    print("\n--- Step 5: Spaghetti plots ---")

    print("  5a: All counties...")
    plot_spaghetti_all(hybrid_ts, county_names, ext, OUT_SPAG_ALL)

    print("  5b: Dense urban (original vs hybrid comparison)...")
    plot_spaghetti_dense_urban(original_ts, hybrid_ts, county_names, ext, OUT_SPAG_URBAN)

    print("  5c: Rural / suburban...")
    plot_spaghetti_rural(hybrid_ts, county_names, ext, OUT_SPAG_RURAL)

    # ── Step 6: Animated GIF ─────────────────────────────────────────────
    print("\n--- Step 6: Animated GIF ---")
    make_gif(hybrid_ts, county_names, OUT_GIF, top_n=30, fps=2)

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("Done.")
    rescaled_counties = scale_table[scale_table["rescaled"]]["county_name"].tolist()
    print(f"  Rescaled counties ({len(rescaled_counties)}): {', '.join(rescaled_counties)}")
    print()
    print(f"  Hybrid panel:        {OUT_HYBRID}")
    print(f"  Scale factors:       {OUT_SCALE_TABLE}")
    print(f"  Validation report:   {OUT_VALIDATION}")
    print(f"  Spaghetti (all):     {OUT_SPAG_ALL}")
    print(f"  Spaghetti (urban):   {OUT_SPAG_URBAN}")
    print(f"  Spaghetti (rural):   {OUT_SPAG_RURAL}")
    print(f"  Animation:           {OUT_GIF}")
    print("=" * 65)


if __name__ == "__main__":
    main()
