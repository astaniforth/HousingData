"""
End-to-end workflow runner for the housing data pipeline.

This script wires together the individual steps:
1) optional refresh of HPD data from NYC Open Data,
2) classification of projects by financing type (LL44 funding),
3) DOB NB/New Building filing searches (with optional BBL fallback),
4) Certificate of Occupancy searches,
5) timeline joins, and
6) optional chart generation.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from fetch_affordable_housing_data import update_local_data, verify_and_fetch_hpd_data
from query_ll44_funding import query_and_add_financing
from query_dob_filings import query_dob_filings
from query_co_filings import query_co_filings
from HPD_DOB_Join_On_BIN import create_separate_timelines
from create_timeline_chart import create_timeline_chart, create_financing_charts
from data_quality import quality_tracker


def _default_hpd_csv() -> Path:
    """Pick the best-available HPD dataset on disk."""
    candidates = [
        Path("data/processed/Affordable_Housing_Production_by_Building_with_financing.csv"),
        Path("data/raw/Affordable_Housing_Production_by_Building_with_financing.csv"),
        Path("data/raw/Affordable_Housing_Production_by_Building.csv"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[-1]


def _normalize_bin(bin_value) -> Optional[str]:
    """Normalize BIN to a clean string."""
    if pd.isna(bin_value):
        return None
    try:
        return str(int(float(bin_value)))
    except (TypeError, ValueError):
        value = str(bin_value).strip()
        return value or None


def _write_bin_file(source_csv: Path, output_txt: Path) -> Path:
    """Extract BINs from a CSV and write them to a text file for CO searches."""
    df = pd.read_csv(source_csv)
    candidate_cols = [col for col in df.columns if col.lower() in ("bin", "bin_normalized")]
    if not candidate_cols:
        raise SystemExit(f"Could not find a BIN column in {source_csv}")

    bins = [_normalize_bin(val) for val in df[candidate_cols[0]].dropna()]
    bins = sorted({b for b in bins if b})

    output_txt.parent.mkdir(parents=True, exist_ok=True)
    output_txt.write_text("\n".join(bins))
    print(f"Wrote {len(bins)} BINs to {output_txt}")
    return output_txt


def _require_file(path: Path, description: str) -> None:
    """Exit with a helpful message if a required file is missing."""
    if not path.exists():
        raise SystemExit(f"{description} not found at {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the housing data workflow end-to-end.")
    parser.add_argument("--hpd-csv", help="HPD CSV to start from (defaults to best available on disk).")
    parser.add_argument("--refresh-hpd", action="store_true", help="Fetch fresh HPD data from NYC Open Data.")
    parser.add_argument("--skip-ll44", action="store_true", help="Skip LL44 funding lookup / financing classification.")
    parser.add_argument("--financing-output", help="Where to write HPD data with financing type.")

    parser.add_argument("--dob-search-source", help="Path for DOB search (CSV with BIN/BBL or BIN text). Defaults to HPD CSV.")
    parser.add_argument("--dob-output", help="DOB filings output CSV path.")
    parser.add_argument("--disable-bbl-fallback", action="store_true", help="Disable BBL fallback when searching DOB APIs.")
    parser.add_argument("--skip-dob", action="store_true", help="Do not query DOB APIs (reuse an existing output).")

    parser.add_argument("--bin-file", help="Text file with BINs for CO search. If absent, one is generated from the HPD CSV.")
    parser.add_argument("--bin-output", help="Where to write the generated BIN list (defaults to data/processed/workflow_bins.txt).")
    parser.add_argument("--co-output", help="CO filings output CSV path.")
    parser.add_argument("--skip-co", action="store_true", help="Do not query CO APIs (reuse an existing output).")

    parser.add_argument("--skip-join", action="store_true", help="Skip timeline joins (reuse an existing timeline).")
    parser.add_argument("--skip-charts", action="store_true", help="Skip PDF chart generation.")
    return parser.parse_args()


def step_1_verify_and_fetch_hpd_data(args) -> Path:
    """
    Step 1: Verify if local HPD data matches API, fetch if needed.

    Returns:
        Path: Path to the verified/fetched HPD CSV file
    """
    print("=" * 70)
    print("STEP 1: VERIFY AND FETCH HPD DATA")
    print("=" * 70)

    # Start quality tracking
    quality_tracker.start_processing()

    if args.refresh_hpd:
        print("Force refresh requested - fetching fresh HPD data...")
        hpd_df, hpd_csv = update_local_data()
    else:
        print("Verifying local HPD data against API...")
        hpd_df, hpd_csv = verify_and_fetch_hpd_data()

    _require_file(hpd_csv, "HPD dataset")

    # Record initial dataset size
    quality_tracker.analyze_hpd_data(hpd_df, "Full_HPD_Dataset")
    quality_tracker.record_pipeline_stage("raw_hpd_data", len(hpd_df), "Raw HPD affordable housing dataset")

    print(f"✅ Step 1 complete: {len(hpd_df):,} records loaded")
    return hpd_csv


def step_2_add_financing_classification(args, hpd_csv: Path) -> Path:
    """
    Step 2: Add LL44 financing classification to HPD data.

    Returns:
        Path: Path to the HPD CSV with financing classification
    """
    print("\n" + "=" * 70)
    print("STEP 2: ADD FINANCING CLASSIFICATION")
    print("=" * 70)

    if args.skip_ll44:
        print("Skipping LL44 financing classification as requested.")
        # Still record the stage for tracking
        hpd_df = pd.read_csv(hpd_csv)
        quality_tracker.record_pipeline_stage("after_financing_skip", len(hpd_df), "Financing classification skipped")
        return hpd_csv

    financing_output = Path(args.financing_output) if args.financing_output else Path(
        "data/processed/Affordable_Housing_Production_by_Building_with_financing.csv"
    )
    financing_output.parent.mkdir(parents=True, exist_ok=True)

    print(f"Classifying financing types -> {financing_output}")
    financing_df = query_and_add_financing(str(hpd_csv), output_path=str(financing_output))

    # Record dataset after financing classification
    quality_tracker.analyze_hpd_data(financing_df, "Filtered_HPD")
    quality_tracker.record_pipeline_stage("after_financing", len(financing_df), "Added LL44 financing classification")

    print(f"✅ Step 2 complete: {len(financing_df):,} records with financing classification")
    return financing_output


def step_3_enrich_with_dob_co_data(args, building_csv: Path) -> tuple[Path, Path]:
    """
    Step 3: Enrich dataset with DOB and CO filings data.

    Returns:
        tuple: (dob_output_path, co_output_path) - paths to enriched data files
    """
    print("\n" + "=" * 70)
    print("STEP 3: ENRICH WITH DOB AND CO DATA")
    print("=" * 70)

    # Prepare inputs for DOB/CO queries
    dob_search_source = Path(args.dob_search_source) if args.dob_search_source else Path(building_csv)
    _require_file(dob_search_source, "DOB search input")

    if args.bin_file:
        bin_file = Path(args.bin_file)
        _require_file(bin_file, "BIN file")
    else:
        bin_output = Path(args.bin_output) if args.bin_output else Path("data/processed/workflow_bins.txt")
        bin_file = _write_bin_file(building_csv, bin_output)

    # DOB filings
    dob_output = Path(args.dob_output) if args.dob_output else Path(
        f"data/processed/{dob_search_source.stem}_dob_filings.csv"
    )
    dob_output.parent.mkdir(parents=True, exist_ok=True)

    # Check for existing DOB files in multiple locations when skipping
    if args.skip_dob:
        print("⏭️  Skipping DOB queries as requested")
        # Look for existing files in processed folder or external folder
        alt_dob_path = Path(f"data/external/{dob_search_source.stem}_dob_filings.csv")
        if dob_output.exists():
            print(f"📁 Using existing DOB data at {dob_output}")
        elif alt_dob_path.exists():
            print(f"📁 Using existing DOB data from external folder: {alt_dob_path}")
            dob_output = alt_dob_path
        else:
            print("⚠️  No existing DOB data found; timeline will omit DOB entries.")
            dob_output = None
    else:
        print(f"🔍 Querying DOB APIs using {dob_search_source} -> {dob_output}")
        print("   This may take several minutes...")
        query_dob_filings(
            str(dob_search_source),
            output_path=str(dob_output),
            use_bbl_fallback=not args.disable_bbl_fallback,
        )
        print(f"✅ DOB query completed: {dob_output}")

    # Certificate of Occupancy filings
    co_output = Path(args.co_output) if args.co_output else Path(
        f"data/processed/{bin_file.stem}_co_filings.csv"
    )
    co_output.parent.mkdir(parents=True, exist_ok=True)

    if args.skip_co:
        # Look for existing CO files in multiple locations
        alt_co_path = Path(f"data/external/{bin_file.stem}_co_filings.csv")
        if co_output.exists():
            print(f"Using existing CO data at {co_output}")
        elif alt_co_path.exists():
            print(f"Using existing CO data from external folder: {alt_co_path}")
            co_output = alt_co_path
        else:
            print("No CO data supplied; timeline will omit CO entries.")
            co_output = None
    else:
        print(f"Querying CO APIs using {bin_file} -> {co_output}")
        query_co_filings(str(bin_file), output_path=str(co_output))

    # Record final enriched dataset
    enriched_df = pd.read_csv(building_csv)
    if dob_output is not None and dob_output.exists():
        quality_tracker.record_pipeline_stage("after_dob_enrichment", len(enriched_df), "Enriched with DOB and CO data")
    else:
        quality_tracker.record_pipeline_stage("after_dob_enrichment", len(enriched_df), "DOB/CO enrichment skipped - no data available")

    print("✅ Step 3 complete: Dataset enriched with DOB/CO data")
    return dob_output, co_output


def step_4_generate_charts(args, building_csv: Path, dob_output: Path, co_output: Path) -> None:
    """
    Step 4: Generate timeline charts from enriched data.
    """
    print("\n" + "=" * 70)
    print("STEP 4: GENERATE TIMELINE CHARTS")
    print("=" * 70)

    if args.skip_charts:
        print("Skipping chart generation as requested.")
        return

    # Timeline join
    hpd_timeline = Path(str(building_csv).replace(".csv", "_hpd_financed_timeline.csv"))
    private_timeline = Path(str(building_csv).replace(".csv", "_privately_financed_timeline.csv"))

    if args.skip_join:
        print("Skipping timeline join step.")
    else:
        if dob_output is None:
            print("No DOB data available; skipping timeline creation.")
            return
        _require_file(dob_output, "DOB filings CSV")
        print("Building timelines...")
        create_separate_timelines(
            str(building_csv),
            str(dob_output),
            str(co_output) if co_output else None,
        )

    # Charts
    print("Generating charts...")
    default_timeline_stem = "Affordable_Housing_Production_by_Building_with_financing"
    if Path(building_csv).name == f"{default_timeline_stem}.csv":
        create_financing_charts()
    else:
        if hpd_timeline.exists():
            create_timeline_chart(str(hpd_timeline))
        else:
            print(f"No HPD financed timeline found at {hpd_timeline}; skipping.")

        if private_timeline.exists():
            create_timeline_chart(str(private_timeline))
        else:
            print(f"No privately financed timeline found at {private_timeline}; skipping.")

    print("✅ Step 4 complete: Charts generated")


def main() -> None:
    import time
    args = parse_args()

    print("=" * 80)
    print("🏗️  HOUSING DATA WORKFLOW STARTED")
    print("=" * 80)
    print(f"Command line arguments: {vars(args)}")
    print()

    workflow_start_time = time.time()
    step_times = {}

    # Execute the 4-step pipeline
    try:
        print("🚀 STARTING STEP 1: Verify and fetch HPD data")
        step_start = time.time()
        # Step 1: Verify and fetch HPD data
        hpd_csv = step_1_verify_and_fetch_hpd_data(args)
        step_times['step_1'] = time.time() - step_start
        print(f"✅ STEP 1 COMPLETED in {step_times['step_1']:.1f}s: HPD data at {hpd_csv}")

        print("🚀 STARTING STEP 2: Add financing classification")
        step_start = time.time()
        # Step 2: Add financing classification
        building_csv = step_2_add_financing_classification(args, hpd_csv)
        step_times['step_2'] = time.time() - step_start
        print(f"✅ STEP 2 COMPLETED in {step_times['step_2']:.1f}s: Financing data at {building_csv}")

        print("🚀 STARTING STEP 3: Enrich with DOB/CO data")
        step_start = time.time()
        # Step 3: Enrich with DOB/CO data
        dob_output, co_output = step_3_enrich_with_dob_co_data(args, building_csv)
        step_times['step_3'] = time.time() - step_start
        print(f"✅ STEP 3 COMPLETED in {step_times['step_3']:.1f}s: DOB={dob_output}, CO={co_output}")

        print("🚀 STARTING STEP 4: Generate charts")
        step_start = time.time()
        # Step 4: Generate charts
        step_4_generate_charts(args, building_csv, dob_output, co_output)
        step_times['step_4'] = time.time() - step_start
        print(f"✅ STEP 4 COMPLETED in {step_times['step_4']:.1f}s: Charts generated")

        # Generate final data quality report and Sankey diagram
        print("\n" + "=" * 70)
        print("📊 GENERATING FINAL DATA QUALITY REPORT")
        print("=" * 70)

        report_start = time.time()
        quality_tracker.end_processing()
        report_filename = quality_tracker.save_report_to_file("final_workflow")
        sankey_filename = quality_tracker.generate_sankey_diagram()
        quality_tracker.print_report()
        report_time = time.time() - report_start

        total_time = time.time() - workflow_start_time

        print("\n" + "=" * 80)
        print("⏱️  WORKFLOW TIMING SUMMARY")
        print("=" * 80)
        print(f"Step 1 (HPD Data): {step_times['step_1']:.1f}s")
        print(f"Step 2 (Financing): {step_times['step_2']:.1f}s")
        print(f"Step 3 (DOB/CO): {step_times['step_3']:.1f}s")
        print(f"Step 4 (Charts): {step_times['step_4']:.1f}s")
        print(f"Reports: {report_time:.1f}s")
        print(f"TOTAL: {total_time:.1f}s")
        print()
        print("🎉 WORKFLOW COMPLETED SUCCESSFULLY!")
        print(f"📊 Data quality report: {report_filename}")
        if sankey_filename:
            print(f"📊 Sankey diagram: {sankey_filename}")

    except Exception as e:
        import traceback
        print(f"\n❌ WORKFLOW FAILED WITH ERROR: {e}")
        print("Full traceback:")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    sys.exit(main())
