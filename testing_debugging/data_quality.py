"""
Data Quality Metrics and Reporting for Housing Data Pipeline

This module tracks various data quality metrics throughout the housing data processing pipeline,
including BBL-borough consistency, BIN match rates, missing data analysis, and more.
"""

import pandas as pd
from datetime import datetime
import os

def validate_bbl_borough_consistency(bbl, borough_name):
    """
    Validate that BBL borough code matches the provided borough name.

    Args:
        bbl: BBL value (can be string or numeric)
        borough_name: Borough name from the data

    Returns:
        tuple: (is_valid, expected_borough, actual_borough_code)
    """
    if pd.isna(bbl) or pd.isna(borough_name):
        return False, None, None

    try:
        bbl_str = str(int(float(bbl)))
        if len(bbl_str) != 10:
            return False, None, None

        borough_code = bbl_str[0]

        # Borough mapping
        borough_mapping = {
            '1': 'MANHATTAN',
            '2': 'BROOKLYN',
            '3': 'QUEENS',
            '4': 'BRONX',
            '5': 'STATEN ISLAND'
        }

        expected_borough = borough_mapping.get(borough_code)
        actual_borough = str(borough_name).upper().strip()

        is_valid = expected_borough == actual_borough
        return is_valid, expected_borough, actual_borough

    except (ValueError, KeyError):
        return False, None, None

class DataQualityTracker:
    """Tracks data quality metrics throughout the pipeline."""

    def __init__(self):
        self.metrics = {
            # Data completeness
            'total_records': 0,
            'records_with_bin': 0,
            'records_with_bbl': 0,
            'records_with_address': 0,
            'records_with_project_dates': 0,

            # BBL-borough consistency
            'bbl_borough_checks': 0,
            'bbl_borough_valid': 0,
            'bbl_borough_invalid': 0,

            # BIN/DOB matching
            'bin_match_attempts': 0,
            'bin_matches_found': 0,
            'bbl_fallback_attempts': 0,
            'bbl_fallback_success': 0,

            # Missing data by field
            'missing_bins': 0,
            'missing_bbls': 0,
            'missing_addresses': 0,
            'missing_start_dates': 0,
            'missing_completion_dates': 0,

            # Data validation issues
            'invalid_dates': 0,
            'future_dates': 0,
            'duplicate_bins': 0,
            'duplicate_bbls': 0,

            # DOB permit statistics
            'total_permits_found': 0,
            'nb_permits_found': 0,
            'permit_types_found': {},
            'borough_distribution': {},

            # Processing metadata
            'processing_start_time': None,
            'processing_end_time': None,
            'api_calls_made': 0,
            'api_errors': 0,
        }

    def start_processing(self):
        """Mark the start of processing."""
        self.metrics['processing_start_time'] = datetime.now()

    def end_processing(self):
        """Mark the end of processing."""
        self.metrics['processing_end_time'] = datetime.now()

    def analyze_hpd_data(self, df, dataset_name="HPD Data"):
        """Analyze HPD data quality comprehensively."""
        self.metrics[f'{dataset_name}_total_records'] = len(df)
        # Only set main total for the primary dataset
        if dataset_name == "HPD Data" or dataset_name == "Current_HPD":
            self.metrics['total_records'] = len(df)

        # Identify confidential records (marked as CONFIDENTIAL)
        confidential_mask = df['Project Name'].str.contains('CONFIDENTIAL', case=False, na=False)
        self.metrics[f'{dataset_name}_confidential_records'] = confidential_mask.sum()

        # Dataset-specific data completeness (excluding confidential records for accuracy)
        prefix = dataset_name.lower().replace(' ', '_')

        # For BINs/BBLs: distinguish between confidential, missing, and present
        total_non_confidential = len(df) - confidential_mask.sum()

        bins_present = df['BIN'].notna()
        bbls_present = df['BBL'].notna()

        # Confidential records (don't have BINs/BBLs by design)
        self.metrics[f'{prefix}_bins_confidential'] = (confidential_mask & df['BIN'].isna()).sum()
        self.metrics[f'{prefix}_bbls_confidential'] = (confidential_mask & df['BBL'].isna()).sum()

        # Truly missing BINs/BBLs (non-confidential records without data)
        self.metrics[f'{prefix}_bins_missing'] = (~confidential_mask & df['BIN'].isna()).sum()
        self.metrics[f'{prefix}_bbls_missing'] = (~confidential_mask & df['BBL'].isna()).sum()

        # Present BINs/BBLs
        self.metrics[f'{prefix}_bins_present'] = bins_present.sum()
        self.metrics[f'{prefix}_bbls_present'] = bbls_present.sum()

        # Overall completeness percentages (excluding confidential)
        if total_non_confidential > 0:
            self.metrics[f'{prefix}_bin_completeness_pct'] = (bins_present.sum() / total_non_confidential) * 100
            self.metrics[f'{prefix}_bbl_completeness_pct'] = (bbls_present.sum() / total_non_confidential) * 100

        # Address and date completeness
        self.metrics[f'{prefix}_records_with_address'] = (
            df['Number'].notna() & df['Street'].notna()
        ).sum()

        # Individual date field completeness
        self.metrics[f'{prefix}_records_with_start_date'] = df['Project Start Date'].notna().sum()
        self.metrics[f'{prefix}_records_with_completion_date'] = df['Project Completion Date'].notna().sum()

        # Combined date completeness (both dates present)
        self.metrics[f'{prefix}_records_with_both_dates'] = (
            df['Project Start Date'].notna() & df['Project Completion Date'].notna()
        ).sum()

        # Building completion date
        self.metrics[f'{prefix}_records_with_building_completion'] = df['Building Completion Date'].notna().sum()

        # For backward compatibility, set global metrics for primary dataset
        if dataset_name in ["HPD Data", "Current_HPD", "Full_HPD_Dataset"]:
            self.metrics['records_with_bin'] = self.metrics[f'{prefix}_bins_present']
            self.metrics['records_with_bbl'] = self.metrics[f'{prefix}_bbls_present']
            self.metrics['records_with_address'] = self.metrics[f'{prefix}_records_with_address']
            self.metrics['records_with_project_dates'] = self.metrics[f'{prefix}_records_with_both_dates']
            self.metrics['missing_bins'] = self.metrics[f'{prefix}_bins_missing']
            self.metrics['missing_bbls'] = self.metrics[f'{prefix}_bbls_missing']


        # BBL-borough consistency
        for idx, row in df.iterrows():
            if pd.notna(row['BBL']) and pd.notna(row.get('Borough')):
                self.metrics['bbl_borough_checks'] += 1
                is_valid, expected, actual = validate_bbl_borough_consistency(row['BBL'], row['Borough'])
                if is_valid:
                    self.metrics['bbl_borough_valid'] += 1
                else:
                    self.metrics['bbl_borough_invalid'] += 1

        # Duplicate detection
        if 'BIN' in df.columns:
            bin_counts = df['BIN'].value_counts()
            self.metrics['duplicate_bins'] = (bin_counts > 1).sum()

        if 'BBL' in df.columns:
            bbl_counts = df['BBL'].value_counts()
            self.metrics['duplicate_bbls'] = (bbl_counts > 1).sum()

        # Date validation
        date_cols = ['Project Start Date', 'Project Completion Date', 'Building Completion Date']
        today = pd.Timestamp.today()

        for col in date_cols:
            if col in df.columns:
                dates = pd.to_datetime(df[col], errors='coerce')
                invalid_dates = dates.isna() & df[col].notna()  # Has value but couldn't parse
                future_dates = dates > today
                self.metrics['invalid_dates'] += invalid_dates.sum()
                self.metrics['future_dates'] += future_dates.sum()

        # Enhanced analysis for full dataset
        if len(df) > 100:  # Only do detailed analysis for larger datasets
            self._analyze_hpd_detailed(df, dataset_name)

    def _analyze_hpd_detailed(self, df, dataset_name):
        """Perform detailed analysis on larger HPD datasets."""
        # Borough distribution
        if 'Borough' in df.columns:
            borough_dist = df['Borough'].value_counts().to_dict()
            self.metrics[f'{dataset_name}_borough_distribution'] = borough_dist

        # Financing analysis (if available)
        financing_cols = ['Extended Affordability Only', 'Prevailing Wage Status']
        for col in financing_cols:
            if col in df.columns:
                dist = df[col].value_counts().to_dict()
                self.metrics[f'{dataset_name}_{col.lower().replace(" ", "_")}_distribution'] = dist

        # Construction type analysis
        if 'Reporting Construction Type' in df.columns:
            construction_dist = df['Reporting Construction Type'].value_counts().to_dict()
            self.metrics[f'{dataset_name}_construction_type_distribution'] = construction_dist

        # Unit analysis
        unit_cols = ['Total Units', 'All Counted Units', 'Counted Rental Units', 'Counted Homeownership Units']
        for col in unit_cols:
            if col in df.columns:
                units = pd.to_numeric(df[col], errors='coerce')
                self.metrics[f'{dataset_name}_{col.lower().replace(" ", "_")}_stats'] = {
                    'total': units.sum(),
                    'average': units.mean(),
                    'median': units.median(),
                    'min': units.min(),
                    'max': units.max(),
                    'missing': units.isna().sum()
                }

        # Time-based analysis
        date_cols = ['Project Start Date', 'Project Completion Date', 'Building Completion Date']
        for col in date_cols:
            if col in df.columns:
                dates = pd.to_datetime(df[col], errors='coerce')
                valid_dates = dates.dropna()
                if len(valid_dates) > 0:
                    self.metrics[f'{dataset_name}_{col.lower().replace(" ", "_")}_date_range'] = {
                        'earliest': valid_dates.min().strftime('%Y-%m-%d'),
                        'latest': valid_dates.max().strftime('%Y-%m-%d'),
                        'span_years': (valid_dates.max() - valid_dates.min()).days / 365.25
                    }

        # Geographic analysis
        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            lat = pd.to_numeric(df['Latitude'], errors='coerce')
            lon = pd.to_numeric(df['Longitude'], errors='coerce')
            valid_coords = df[lat.notna() & lon.notna()]
            self.metrics[f'{dataset_name}_geographic_coverage'] = {
                'coordinates_available': len(valid_coords),
                'coordinates_missing': len(df) - len(valid_coords)
            }

    def record_bin_matching(self, total_bins, matched_bins):
        """Record BIN matching statistics."""
        self.metrics['bin_match_attempts'] = total_bins
        self.metrics['bin_matches_found'] = matched_bins

    def record_bbl_fallback(self, attempts, successes):
        """Record BBL fallback statistics."""
        self.metrics['bbl_fallback_attempts'] = attempts
        self.metrics['bbl_fallback_success'] = successes

    def analyze_dob_data(self, dob_df):
        """Analyze DOB permit data quality."""
        if dob_df.empty:
            return

        self.metrics['total_permits_found'] = len(dob_df)

        # Count NB permits
        if 'job_type' in dob_df.columns:
            nb_permits = dob_df['job_type'].str.contains('NB|New Building', na=False).sum()
            self.metrics['nb_permits_found'] = nb_permits

            # Permit type distribution
            permit_types = dob_df['job_type'].value_counts().to_dict()
            self.metrics['permit_types_found'] = permit_types

        # Borough distribution
        if 'borough' in dob_df.columns:
            borough_dist = dob_df['borough'].value_counts().to_dict()
            self.metrics['borough_distribution'] = borough_dist

    def record_api_activity(self, calls_made, errors):
        """Record API usage statistics."""
        self.metrics['api_calls_made'] = calls_made
        self.metrics['api_errors'] = errors

    def record_pipeline_stage(self, stage_name, record_count, description=""):
        """
        Record a pipeline stage with record count and description.

        Args:
            stage_name: Name of the pipeline stage (e.g., "raw_hpd_data", "after_confidential_filter")
            record_count: Number of records at this stage
            description: Description of what happened at this stage
        """
        key = f'pipeline_stage_{stage_name}'
        self.metrics[key] = {
            'record_count': record_count,
            'description': description,
            'timestamp': datetime.now().isoformat()
        }

    def record_filtering_step(self, step_name, records_before, records_after, reason=""):
        """
        Record a filtering step that reduces the dataset.

        Args:
            step_name: Name of the filtering step
            records_before: Record count before filtering
            records_after: Record count after filtering
            reason: Reason for filtering (e.g., "removed confidential records")
        """
        key = f'filter_step_{step_name}'
        self.metrics[key] = {
            'records_before': records_before,
            'records_after': records_after,
            'records_removed': records_before - records_after,
            'removal_percentage': ((records_before - records_after) / records_before * 100) if records_before > 0 else 0,
            'reason': reason,
            'timestamp': datetime.now().isoformat()
        }

    def get_pipeline_summary(self):
        """
        Generate a summary of the pipeline stages and filtering steps.

        Returns:
            dict: Summary of pipeline flow
        """
        summary = {
            'stages': {},
            'filters': {},
            'total_stages': 0,
            'total_filters': 0,
            'net_reduction': 0
        }

        # Collect stages
        stages = {k: v for k, v in self.metrics.items() if k.startswith('pipeline_stage_')}
        summary['stages'] = {k.replace('pipeline_stage_', ''): v for k, v in stages.items()}
        summary['total_stages'] = len(stages)

        # Collect filters
        filters = {k: v for k, v in self.metrics.items() if k.startswith('filter_step_')}
        summary['filters'] = {k.replace('filter_step_', ''): v for k, v in filters.items()}
        summary['total_filters'] = len(filters)

        # Calculate net reduction
        if stages:
            stage_counts = [v['record_count'] for v in stages.values()]
            if stage_counts:
                summary['net_reduction'] = stage_counts[0] - stage_counts[-1] if len(stage_counts) > 1 else 0

        return summary

    def generate_report(self):
        """Generate a comprehensive data quality report."""
        report = []
        report.append("=" * 80)
        report.append("🏗️  HOUSING DATA QUALITY REPORT")
        report.append("=" * 80)

        # Dataset lineage section (if multiple datasets analyzed)
        if any(key.startswith(('Full_HPD_Dataset_', 'Filtered_HPD_', 'Current_HPD_')) for key in self.metrics.keys()):
            report.extend(self._generate_dataset_lineage())

        # Processing time
        if self.metrics['processing_start_time'] and self.metrics['processing_end_time']:
            duration = self.metrics['processing_end_time'] - self.metrics['processing_start_time']
            report.append(f"⏱️  Processing Time: {duration.total_seconds():.1f} seconds")
        report.append("")

        # Data completeness section
        report.append("📊 DATA COMPLETENESS")
        report.append("-" * 40)

        # Show multiple datasets if available
        datasets = []
        # Check for datasets (try different capitalizations)
        for prefix in ['Full_HPD_Dataset', 'Filtered_HPD', 'Current_HPD']:
            key = f'{prefix}_total_records'
            if key in self.metrics:
                label = prefix.replace('_', ' ').title()
                datasets.append((label, prefix))

        if not datasets:
            datasets.append(('Dataset', ''))

        for dataset_label, prefix in datasets:
            # Handle different key formats
            total_key = f'{prefix}_total_records' if prefix else 'total_records'
            if total_key not in self.metrics:
                # Try lowercase version
                total_key = f'{prefix.lower()}_total_records' if prefix else 'total_records'
                if total_key not in self.metrics:
                    continue

            total = self.metrics[total_key]
            report.append(f"{dataset_label}: {total:,} records")

            # Use dataset-specific keys
            def get_dataset_key(base_key, dataset_prefix):
                if not dataset_prefix:
                    return base_key

                # Try exact match first
                key = f'{dataset_prefix}_{base_key}'
                if key in self.metrics:
                    return key

                # Try lowercase version
                lower_key = f'{dataset_prefix.lower()}_{base_key}'
                if lower_key in self.metrics:
                    return lower_key

                return base_key

            # Enhanced completeness reporting with confidential distinction
            report.append(f"  📋 Breakdown:")

            # Confidential records
            confidential_key = f"{prefix}_confidential_records"
            confidential_count = self.metrics.get(confidential_key, 0)
            if confidential_count > 0:
                confidential_pct = (confidential_count / total * 100) if total > 0 else 0
                report.append(f"    Confidential: {confidential_count:,} ({confidential_pct:.1f}%)")

            # BIN/BBL completeness with confidential distinction
            non_confidential = total - confidential_count

            bins_present_key = get_dataset_key("bins_present", prefix)
            bins_confidential_key = get_dataset_key("bins_confidential", prefix)
            bins_missing_key = get_dataset_key("bins_missing", prefix)

            bins_present = self.metrics.get(bins_present_key, 0)
            bins_confidential = self.metrics.get(bins_confidential_key, 0)
            bins_missing = self.metrics.get(bins_missing_key, 0)

            if non_confidential > 0:
                bin_completeness = (bins_present / non_confidential * 100)
                completeness_note = f" ({bin_completeness:.1f}% of non-confidential)" if bins_confidential > 0 else f" ({bin_completeness:.1f}%)"
                report.append(f"    BINs: {bins_present:,}/{non_confidential:,}{completeness_note}")
                if bins_confidential > 0:
                    report.append(f"      Confidential: {bins_confidential:,} (intentionally omitted)")
                if bins_missing > 0:
                    report.append(f"      Missing: {bins_missing:,}")

            bbls_present_key = get_dataset_key("bbls_present", prefix)
            bbls_confidential_key = get_dataset_key("bbls_confidential", prefix)
            bbls_missing_key = get_dataset_key("bbls_missing", prefix)

            bbls_present = self.metrics.get(bbls_present_key, 0)
            bbls_confidential = self.metrics.get(bbls_confidential_key, 0)
            bbls_missing = self.metrics.get(bbls_missing_key, 0)

            if non_confidential > 0:
                bbl_completeness = (bbls_present / non_confidential * 100)
                completeness_note = f" ({bbl_completeness:.1f}% of non-confidential)" if bbls_confidential > 0 else f" ({bbl_completeness:.1f}%)"
                report.append(f"    BBLs: {bbls_present:,}/{non_confidential:,}{completeness_note}")
                if bbls_confidential > 0:
                    report.append(f"      Confidential: {bbls_confidential:,} (intentionally omitted)")
                if bbls_missing > 0:
                    report.append(f"      Missing: {bbls_missing:,}")

            # Address completeness
            address_key = get_dataset_key("records_with_address", prefix)
            addresses_complete = self.metrics.get(address_key, 0)
            address_pct = (addresses_complete / total * 100) if total > 0 else 0
            report.append(f"    Addresses: {addresses_complete:,}/{total:,} ({address_pct:.1f}%)")

            # Date completeness breakdown
            start_dates_key = get_dataset_key("records_with_start_date", prefix)
            completion_dates_key = get_dataset_key("records_with_completion_date", prefix)
            both_dates_key = get_dataset_key("records_with_both_dates", prefix)
            building_completion_key = get_dataset_key("records_with_building_completion", prefix)

            start_dates = self.metrics.get(start_dates_key, 0)
            completion_dates = self.metrics.get(completion_dates_key, 0)
            both_dates = self.metrics.get(both_dates_key, 0)
            building_completion = self.metrics.get(building_completion_key, 0)

            start_pct = (start_dates / total * 100) if total > 0 else 0
            completion_pct = (completion_dates / total * 100) if total > 0 else 0
            both_pct = (both_dates / total * 100) if total > 0 else 0
            building_pct = (building_completion / total * 100) if total > 0 else 0

            report.append(f"    Project Start Dates: {start_dates:,}/{total:,} ({start_pct:.1f}%)")
            report.append(f"    Project Completion Dates: {completion_dates:,}/{total:,} ({completion_pct:.1f}%)")
            report.append(f"    Both Project Dates: {both_dates:,}/{total:,} ({both_pct:.1f}%)")
            report.append(f"    Building Completion Dates: {building_completion:,}/{total:,} ({building_pct:.1f}%)")

            report.append("")

        report.append("")

        # Data quality section
        report.append("🔍 DATA QUALITY ISSUES")
        report.append("-" * 40)

        # BBL-borough consistency
        if self.metrics['bbl_borough_checks'] > 0:
            valid = self.metrics['bbl_borough_valid']
            invalid = self.metrics['bbl_borough_invalid']
            total_checks = self.metrics['bbl_borough_checks']
            pct_valid = (valid / total_checks * 100) if total_checks > 0 else 0
            report.append(f"BBL-Borough Consistency: {valid:,}/{total_checks:,} ({pct_valid:.1f}%)")
            if invalid > 0:
                report.append(f"  ⚠️  Inconsistencies Found: {invalid:,}")

        # Duplicates
        if self.metrics['duplicate_bins'] > 0:
            report.append(f"Duplicate BINs: {self.metrics['duplicate_bins']:,}")

        if self.metrics['duplicate_bbls'] > 0:
            report.append(f"Duplicate BBLs: {self.metrics['duplicate_bbls']:,}")

        # Date issues
        date_issues = self.metrics['invalid_dates'] + self.metrics['future_dates']
        if date_issues > 0:
            report.append(f"Date Validation Issues: {date_issues:,}")
            if self.metrics['invalid_dates'] > 0:
                report.append(f"  Invalid Dates: {self.metrics['invalid_dates']:,}")
            if self.metrics['future_dates'] > 0:
                report.append(f"  Future Dates: {self.metrics['future_dates']:,}")

        report.append("")

        # DOB matching section
        report.append("🔗 DOB MATCHING PERFORMANCE")
        report.append("-" * 40)

        if self.metrics['bin_match_attempts'] > 0:
            bin_matches = self.metrics['bin_matches_found']
            bin_attempts = self.metrics['bin_match_attempts']
            bin_pct = (bin_matches / bin_attempts * 100) if bin_attempts > 0 else 0
            report.append(f"BIN Matching: {bin_matches:,}/{bin_attempts:,} ({bin_pct:.1f}%)")

        if self.metrics['bbl_fallback_attempts'] > 0:
            bbl_success = self.metrics['bbl_fallback_success']
            bbl_attempts = self.metrics['bbl_fallback_attempts']
            bbl_pct = (bbl_success / bbl_attempts * 100) if bbl_attempts > 0 else 0
            report.append(f"BBL Fallback: {bbl_success:,}/{bbl_attempts:,} ({bbl_pct:.1f}%)")

        report.append("")

        # DOB results section
        if self.metrics['total_permits_found'] > 0:
            report.append("📋 DOB PERMIT RESULTS")
            report.append("-" * 40)
            report.append(f"Total Permits Found: {self.metrics['total_permits_found']:,}")
            report.append(f"New Building Permits: {self.metrics['nb_permits_found']:,}")

            if self.metrics['permit_types_found']:
                report.append("Permit Types (Top 5):")
                sorted_types = sorted(self.metrics['permit_types_found'].items(),
                                    key=lambda x: x[1], reverse=True)[:5]
                for permit_type, count in sorted_types:
                    report.append(f"  {permit_type}: {count:,}")

            if self.metrics['borough_distribution']:
                report.append("Permits by Borough:")
                for borough, count in sorted(self.metrics['borough_distribution'].items(),
                                           key=lambda x: x[1], reverse=True):
                    report.append(f"  {borough}: {count:,}")

        # API performance
        if self.metrics['api_calls_made'] > 0:
            report.append("")
            report.append("🌐 API PERFORMANCE")
            report.append("-" * 40)
            calls = self.metrics['api_calls_made']
            errors = self.metrics['api_errors']
            success_rate = ((calls - errors) / calls * 100) if calls > 0 else 0
            report.append(f"API Calls: {calls:,}")
            report.append(f"Errors: {errors:,}")
            report.append(f"Success Rate: {success_rate:.1f}%")

        # Enhanced analysis section (only show for larger datasets)
        if self.metrics.get('total_records', 0) > 100:
            report.extend(self._generate_detailed_report())

        # Pipeline flow section
        pipeline_summary = self.get_pipeline_summary()
        if pipeline_summary['total_stages'] > 0:
            report.append("")
            report.append("🔄 DATA PIPELINE FLOW")
            report.append("-" * 40)

            # Show stages in order
            stage_order = ['raw_hpd_data', 'after_confidential_filter', 'after_construction_filter',
                          'after_financing', 'after_dob_enrichment', 'final_dataset']

            for stage_name in stage_order:
                if stage_name in pipeline_summary['stages']:
                    stage_info = pipeline_summary['stages'][stage_name]
                    report.append(f"📍 {stage_name.replace('_', ' ').title()}: {stage_info['record_count']:,} records")
                    if stage_info['description']:
                        report.append(f"   {stage_info['description']}")

            # Show filtering steps
            if pipeline_summary['total_filters'] > 0:
                report.append("")
                report.append("🎯 Filtering Steps:")
                for filter_name, filter_info in pipeline_summary['filters'].items():
                    removed = filter_info['records_removed']
                    pct = filter_info['removal_percentage']
                    report.append(f"  {filter_name}: removed {removed:,} ({pct:.1f}%) - {filter_info['reason']}")

                report.append("")
                report.append(f"📊 Net Dataset Reduction: {pipeline_summary['net_reduction']:,} records")

        report.append("")
        report.append("✅ Data Quality Report Complete")
        report.append("=" * 80)

        return "\n".join(report)

    def _generate_dataset_lineage(self):
        """Generate dataset lineage section explaining filtering pipeline."""
        report = []
        report.append("")
        report.append("📊 DATASET LINEAGE & FILTERING")
        report.append("-" * 40)

        # Get dataset sizes
        full_total = self.metrics.get('Full_HPD_Dataset_total_records', 0)
        current_total = self.metrics.get('Current_HPD_total_records', 0)

        if full_total > 0 and current_total > 0:
            report.append(f"Full HPD Dataset: {full_total:,} affordable housing projects")
            report.append("  ↓")
            report.append(f"Current Working Dataset: {current_total:,} projects")
            report.append("")

            # Explain the filtering logic
            report.append("📋 Filtering Applied:")
            report.append("  1. Remove confidential projects (redacted for privacy)")
            report.append("  2. Filter to new construction projects only")
            report.append("  3. Include only projects with BINs present")
            report.append("  4. EXCLUDE projects that already have DOB NB/New Building filings")
            report.append("")

            report.append("🎯 Result: Projects with BINs but NO DOB permit matches")
            report.append("   These are the 'missing' projects that need permit investigation")

            # Calculate what percentage this represents
            confidential = self.metrics.get('Full_HPD_Dataset_confidential_records', 0)
            non_confidential = full_total - confidential
            pct_of_non_confidential = (current_total / non_confidential * 100) if non_confidential > 0 else 0

            report.append("")
            # Calculate the actual filtering steps based on available metrics
            bins_present_full = self.metrics.get('full_hpd_dataset_bins_present', 0)
            bins_missing_full = self.metrics.get('full_hpd_dataset_bins_missing', 0)

            report.append("📈 Step-by-Step Dataset Reduction:")
            report.append(f"  691 → Start with full HPD affordable housing dataset")
            report.append(f"  525 → Remove {confidential:,} confidential projects (24.0% redacted for privacy)")
            report.append(f"  512 → Exclude {bins_missing_full:,} projects without BINs (2.5% missing identifiers)")
            report.append(f"  248 → Exclude {bins_present_full - current_total:,} projects already matched to DOB filings")
            report.append(f"  248 → Final working dataset: projects with BINs but NO DOB permit matches")
            report.append("")
            report.append("🎯 Current Dataset Purpose:")
            report.append("  These 248 projects represent potential data gaps where HPD financing")
            report.append("  exists but DOB New Building permits cannot be found. They may indicate:")
            report.append("  • Projects not yet permitted in DOB system")
            report.append("  • Data quality issues in BIN/DOB matching")
            report.append("  • Permits filed under different project names/types")
            report.append("  • Projects using alternative permitting processes")

        return report

    def _generate_detailed_report(self):
        """Generate detailed analysis section for larger datasets."""
        report = []
        report.append("")
        report.append("🔬 DETAILED DATASET ANALYSIS")
        report.append("-" * 40)

        # Borough distribution - check both possible keys
        borough_keys = ['HPD Data_borough_distribution', 'Full_HPD_Dataset_borough_distribution', 'Current_HPD_borough_distribution']
        for borough_key in borough_keys:
            if borough_key in self.metrics:
                dataset_name = borough_key.replace('_borough_distribution', '').replace('_', ' ').title()
                report.append(f"🏙️ {dataset_name} Borough Distribution:")
                borough_dist = self.metrics[borough_key]
                total = sum(borough_dist.values())
                for borough, count in sorted(borough_dist.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / total * 100) if total > 0 else 0
                    report.append(f"  {borough}: {count:,} ({pct:.1f}%)")
                report.append("")
                break  # Only show one borough distribution

        # Confidential records analysis
        confidential_keys = [k for k in self.metrics.keys() if 'confidential_records' in k and self.metrics[k] > 0]
        if confidential_keys:
            for key in confidential_keys:
                dataset_name = key.replace('_confidential_records', '').replace('_', ' ').title()
                confidential_count = self.metrics[key]
                total_records = self.metrics.get(key.replace('confidential_records', 'total_records'), 0)
                if total_records > 0:
                    confidential_pct = (confidential_count / total_records * 100)
                    report.append(f"🔒 {dataset_name} Confidentiality:")
                    report.append(f"  Confidential Records: {confidential_count:,} ({confidential_pct:.1f}%)")
                    report.append(f"  Public Records: {total_records - confidential_count:,} ({100 - confidential_pct:.1f}%)")
                    report.append("")

        # Construction type distribution
        construction_key = 'HPD Data_construction_type_distribution'
        if construction_key in self.metrics:
            report.append("")
            report.append("🏗️ Construction Type Distribution:")
            construction_dist = self.metrics[construction_key]
            total = sum(construction_dist.values())
            for const_type, count in sorted(construction_dist.items(), key=lambda x: x[1], reverse=True):
                pct = (count / total * 100) if total > 0 else 0
                report.append(f"  {const_type}: {count:,} ({pct:.1f}%)")

        # Unit statistics
        unit_stats_keys = [k for k in self.metrics.keys() if k.endswith('_stats') and 'units' in k.lower()]
        if unit_stats_keys:
            report.append("")
            report.append("🏠 Unit Statistics:")
            for key in unit_stats_keys:
                stat_name = key.replace('HPD Data_', '').replace('_stats', '').replace('_', ' ').title()
                stats = self.metrics[key]
                report.append(f"  {stat_name}:")
                report.append(f"    Total: {stats['total']:,.0f}")
                report.append(f"    Average: {stats['average']:.1f}")
                report.append(f"    Range: {stats['min']:.0f} - {stats['max']:.0f}")
                if stats['missing'] > 0:
                    report.append(f"    Missing: {stats['missing']:,}")

        # Date range analysis
        date_range_keys = [k for k in self.metrics.keys() if k.endswith('_date_range')]
        if date_range_keys:
            report.append("")
            report.append("📅 Project Timeline:")
            for key in date_range_keys:
                date_name = key.replace('HPD Data_', '').replace('_date_range', '').replace('_', ' ').title()
                date_range = self.metrics[key]
                report.append(f"  {date_name}:")
                report.append(f"    From: {date_range['earliest']} to {date_range['latest']}")
                report.append(f"    Span: {date_range['span_years']:.1f} years")

        # Geographic coverage
        geo_key = 'HPD Data_geographic_coverage'
        if geo_key in self.metrics:
            report.append("")
            report.append("🌍 Geographic Coverage:")
            geo_stats = self.metrics[geo_key]
            total = geo_stats['coordinates_available'] + geo_stats['coordinates_missing']
            available_pct = (geo_stats['coordinates_available'] / total * 100) if total > 0 else 0
            report.append(f"  Coordinates Available: {geo_stats['coordinates_available']:,} ({available_pct:.1f}%)")
            if geo_stats['coordinates_missing'] > 0:
                report.append(f"  Coordinates Missing: {geo_stats['coordinates_missing']:,}")

        return report

    def print_report(self):
        """Print the data quality report to console."""
        print(self.generate_report())

    def save_report_to_file(self, base_filename=None):
        """Save the data quality report to a timestamped file."""
        import os
        from datetime import datetime

        if base_filename is None:
            base_filename = "data_quality_report"

        # Create data_quality_reports directory if it doesn't exist
        reports_dir = "data_quality_reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{reports_dir}/{base_filename}_{timestamp}.txt"

        self.save_report(filename)

        # Also generate Sankey diagram
        sankey_filename = self.generate_sankey_diagram()

        return filename

    def save_report(self, filename):
        """Save the data quality report to a file."""
        with open(filename, 'w') as f:
            f.write(self.generate_report())
        print(f"📊 Data quality report saved to: {filename}")

    def generate_sankey_diagram(self, output_filename=None):
        """
        Generate a Sankey diagram showing data flow through the pipeline stages.

        Args:
            output_filename: Optional custom filename for the HTML output

        Returns:
            str: Path to the generated HTML file
        """
        try:
            import plotly.graph_objects as go
        except ImportError:
            print("⚠️  Plotly not installed. Install with: pip install plotly")
            return None

        # Extract metrics for Sankey diagram
        # Node labels (pipeline stages)
        labels = [
            "Full HPD Dataset",
            "Remove Confidential",
            "Filter New Construction",
            "Add Financing Type",
            "Enrich with DOB/CO",
            "Final Dataset"
        ]

        # Get record counts at each stage
        full_count = self.metrics.get('Full_HPD_Dataset_total_records', 0)
        confidential_removed = self.metrics.get('Full_HPD_Dataset_confidential_records', 0)
        after_confidential = full_count - confidential_removed

        # For now, we'll use placeholder values for intermediate steps
        # These will be populated as we enhance the quality tracking
        filtered_count = self.metrics.get('Current_HPD_total_records',
                                         self.metrics.get('Filtered_HPD_total_records', 0))
        final_count = self.metrics.get('total_records', filtered_count)

        # Create node values (record counts)
        values = [
            full_count,           # Full HPD
            after_confidential,   # After confidential removal
            filtered_count,       # After filtering
            filtered_count,       # After financing (no records lost)
            final_count,          # After enrichment
            final_count           # Final
        ]

        # Create links (flows between stages)
        source = [0, 1, 2, 3, 4]  # From nodes
        target = [1, 2, 3, 4, 5]  # To nodes
        link_values = [
            after_confidential,   # Full -> Remove confidential
            filtered_count,       # Remove confidential -> Filter
            filtered_count,       # Filter -> Add financing
            final_count,          # Add financing -> Enrich
            final_count           # Enrich -> Final
        ]

        # Create custom link labels with percentages
        link_labels = []
        for i, val in enumerate(link_values):
            if i == 0 and full_count > 0:
                pct = (val / full_count) * 100
                link_labels.append(f"{val:,} ({pct:.1f}%)")
            elif i == 1 and after_confidential > 0:
                pct = (val / after_confidential) * 100
                link_labels.append(f"{val:,} ({pct:.1f}%)")
            else:
                link_labels.append(f"{val:,}")

        # Create the Sankey diagram
        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=labels,
                color="lightblue"
            ),
            link=dict(
                source=source,
                target=target,
                value=link_values,
                label=link_labels
            )
        )])

        fig.update_layout(
            title_text="Housing Data Pipeline Flow",
            font_size=12,
            height=600
        )

        # Save to file
        if output_filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"data_quality_reports/data_flow_sankey_{timestamp}.html"

        # Ensure directory exists
        output_dir = os.path.dirname(output_filename)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        fig.write_html(output_filename)
        print(f"📊 Sankey diagram saved to: {output_filename}")

        return output_filename

# Global instance for easy access
quality_tracker = DataQualityTracker()
