#!/usr/bin/env python3
"""
Test script to find optimal batch size for DOB API queries.
Tests different batch sizes with real API calls.
"""

import time
import pandas as pd
import requests

def query_dob_api_batch(url, bin_list, job_type="NB", batch_size=50):
    """Query DOB API with specific batch size."""
    print(f"  Testing batch_size={batch_size} on {url.split('/')[-1]}")

    all_results = []
    start_time = time.time()

    # Query in batches
    for i in range(0, len(bin_list), batch_size):
        batch = bin_list[i:i+batch_size]

        # Build query
        bin_column = "bin__" if "ic3t-wcy2" in url else "bin"
        bin_filter = " OR ".join([f"{bin_column}='{bin_num}'" for bin_num in batch])
        query = f"job_type='{job_type}' AND ({bin_filter})"

        params = {
            '$where': query,
            '$limit': 50000
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if data:
                all_results.extend(data)

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            print(f"    Error: {str(e)[:50]}")
            continue

    duration = time.time() - start_time
    return len(all_results), duration

def test_batch_sizes():
    """Test different batch sizes with real API calls."""
    print("🚀 DOB API Batch Size Optimization Test")
    print("=" * 60)

    # Use a small subset of BINs for testing
    test_bins = ["1054682", "2129098", "3413815", "3426941", "3000000"]
    print(f"Testing with {len(test_bins)} BINs")

    # APIs to test
    apis = [
        ("DOB Job Apps", "https://data.cityofnewyork.us/resource/ic3t-wcy2.json", "NB"),
        ("DOB NOW", "https://data.cityofnewyork.us/resource/w9ak-ipjd.json", "New Building")
    ]

    # Test different batch sizes including larger ones
    batch_sizes = [200, 300, 400, 500, 600, 700, 800]

    results = {}

    for batch_size in batch_sizes:
        print(f"\n🧪 Testing batch_size={batch_size}")
        total_records = 0
        total_time = 0

        for api_name, url, job_type in apis:
            records, duration = query_dob_api_batch(url, test_bins, job_type, batch_size)
            total_records += records
            total_time += duration
            print(f"    {api_name}: {records} records in {duration:.1f}s")

        records_per_sec = total_records / total_time if total_time > 0 else 0
        results[batch_size] = (total_time, records_per_sec, total_records)
        print(".1f")
    print("\n📊 PERFORMANCE RESULTS")
    print("=" * 60)
    print("Batch Size | Time (s) | Records/s | Records | Efficiency")
    print("-" * 65)

    # Find best performer
    best_batch = min(results.keys(), key=lambda x: results[x][0])
    best_time = results[best_batch][0]

    for batch_size in sorted(results.keys()):
        total_time, records_per_sec, total_records = results[batch_size]
        efficiency = (best_time / total_time) * 100 if total_time > 0 else 0
        marker = " ⭐ BEST" if batch_size == best_batch else ""

        print(f"{batch_size:>10} | {total_time:>8.1f} | {records_per_sec:>9.1f} | {total_records:>7} | {efficiency:>9.1f}%{marker}")

    print(f"\n🏆 OPTIMAL BATCH SIZE: {best_batch}")
    print(".1f")
    print("   This maximizes API efficiency while respecting rate limits")
    return best_batch

if __name__ == "__main__":
    optimal_batch = test_batch_sizes()
    print(f"\n💡 Recommendation: Update query_dob_filings.py to use batch_size = {optimal_batch}")
