"""
Data Quality Metrics and Reporting for Housing Data Pipeline

This module tracks various data quality metrics throughout the housing data processing pipeline,
including BBL-borough consistency, BIN match rates, missing data analysis, and more.
"""

import pandas as pd
from datetime import datetime

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

    def analyze_hpd_data(self, df):
        """Analyze HPD data quality."""
        self.metrics['total_records'] = len(df)

        # Data completeness
        self.metrics['records_with_bin'] = df['BIN'].notna().sum()
        self.metrics['records_with_bbl'] = df['BBL'].notna().sum()
        self.metrics['records_with_address'] = (
            df['Number'].notna() & df['Street'].notna()
        ).sum()
        self.metrics['records_with_project_dates'] = (
            df['Project Start Date'].notna() & df['Project Completion Date'].notna()
        ).sum()

        # Missing data counts
        self.metrics['missing_bins'] = df['BIN'].isna().sum()
        self.metrics['missing_bbls'] = df['BBL'].isna().sum()
        self.metrics['missing_addresses'] = (
            df['Number'].isna() | df['Street'].isna()
        ).sum()
        self.metrics['missing_start_dates'] = df['Project Start Date'].isna().sum()
        self.metrics['missing_completion_dates'] = df['Project Completion Date'].isna().sum()

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

    def generate_report(self):
        """Generate a comprehensive data quality report."""
        report = []
        report.append("=" * 80)
        report.append("🏗️  HOUSING DATA QUALITY REPORT")
        report.append("=" * 80)

        # Processing time
        if self.metrics['processing_start_time'] and self.metrics['processing_end_time']:
            duration = self.metrics['processing_end_time'] - self.metrics['processing_start_time']
            report.append(f"⏱️  Processing Time: {duration.total_seconds():.1f} seconds")
        report.append("")

        # Data completeness section
        report.append("📊 DATA COMPLETENESS")
        report.append("-" * 40)
        total = self.metrics['total_records']
        report.append(f"Total Records: {total:,}")

        completeness_metrics = [
            ('BINs Present', 'records_with_bin', 'missing_bins'),
            ('BBLs Present', 'records_with_bbl', 'missing_bbls'),
            ('Addresses Complete', 'records_with_address', 'missing_addresses'),
            ('Project Dates Complete', 'records_with_project_dates', None),
        ]

        for label, present_key, missing_key in completeness_metrics:
            present = self.metrics[present_key]
            pct = (present / total * 100) if total > 0 else 0
            report.append(f"  {label}: {present:,} ({pct:.1f}%)")
            if missing_key:
                missing = self.metrics[missing_key]
                report.append(f"    Missing: {missing:,}")

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

        report.append("")
        report.append("✅ Data Quality Report Complete")
        report.append("=" * 80)

        return "\n".join(report)

    def print_report(self):
        """Print the data quality report to console."""
        print(self.generate_report())

    def save_report_to_file(self, base_filename=None):
        """Save the data quality report to a timestamped file."""
        import os
        from datetime import datetime

        if base_filename is None:
            base_filename = "data_quality_report"

        # Create reports directory if it doesn't exist
        reports_dir = "reports"
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)

        # Generate timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{reports_dir}/{base_filename}_{timestamp}.txt"

        self.save_report(filename)
        return filename

    def save_report(self, filename):
        """Save the data quality report to a file."""
        with open(filename, 'w') as f:
            f.write(self.generate_report())
        print(f"📊 Data quality report saved to: {filename}")

# Global instance for easy access
quality_tracker = DataQualityTracker()
