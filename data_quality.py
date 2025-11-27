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

    def analyze_hpd_data(self, df, dataset_name="HPD Data"):
        """Analyze HPD data quality comprehensively."""
        self.metrics[f'{dataset_name}_total_records'] = len(df)
        # Only set main total for the primary dataset
        if dataset_name == "HPD Data" or dataset_name == "Current_HPD":
            self.metrics['total_records'] = len(df)

        # Dataset-specific data completeness
        prefix = dataset_name.lower().replace(' ', '_')
        self.metrics[f'{prefix}_records_with_bin'] = df['BIN'].notna().sum()
        self.metrics[f'{prefix}_records_with_bbl'] = df['BBL'].notna().sum()
        self.metrics[f'{prefix}_records_with_address'] = (
            df['Number'].notna() & df['Street'].notna()
        ).sum()
        self.metrics[f'{prefix}_records_with_project_dates'] = (
            df['Project Start Date'].notna() & df['Project Completion Date'].notna()
        ).sum()

        # Dataset-specific missing data counts
        self.metrics[f'{prefix}_missing_bins'] = df['BIN'].isna().sum()
        self.metrics[f'{prefix}_missing_bbls'] = df['BBL'].isna().sum()
        self.metrics[f'{prefix}_missing_addresses'] = (
            df['Number'].isna() | df['Street'].isna()
        ).sum()
        self.metrics[f'{prefix}_missing_start_dates'] = df['Project Start Date'].isna().sum()
        self.metrics[f'{prefix}_missing_completion_dates'] = df['Project Completion Date'].isna().sum()

        # For backward compatibility, also set global metrics for primary dataset
        if dataset_name in ["HPD Data", "Current_HPD"]:
            self.metrics['records_with_bin'] = self.metrics[f'{prefix}_records_with_bin']
            self.metrics['records_with_bbl'] = self.metrics[f'{prefix}_records_with_bbl']
            self.metrics['records_with_address'] = self.metrics[f'{prefix}_records_with_address']
            self.metrics['records_with_project_dates'] = self.metrics[f'{prefix}_records_with_project_dates']
            self.metrics['missing_bins'] = self.metrics[f'{prefix}_missing_bins']
            self.metrics['missing_bbls'] = self.metrics[f'{prefix}_missing_bbls']
            self.metrics['missing_addresses'] = self.metrics[f'{prefix}_missing_addresses']
            self.metrics['missing_start_dates'] = self.metrics[f'{prefix}_missing_start_dates']
            self.metrics['missing_completion_dates'] = self.metrics[f'{prefix}_missing_completion_dates']

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

        # Show multiple datasets if available
        datasets = []
        if 'Full_HPD_Dataset_total_records' in self.metrics:
            datasets.append(('Full HPD Dataset', 'Full_HPD_Dataset'))
        if 'Filtered_HPD_total_records' in self.metrics:
            datasets.append(('Filtered Dataset', 'Filtered_HPD'))
        elif 'Current_HPD_total_records' in self.metrics:
            datasets.append(('Current Dataset', 'Current_HPD'))

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

            # Use dataset-specific keys (try both title case and lowercase)
            def get_dataset_key(base_key, dataset_prefix):
                if not dataset_prefix:
                    return base_key

                # Try title case first
                title_key = f'{dataset_prefix}_{base_key}'
                if title_key in self.metrics:
                    return title_key

                # Try lowercase
                lower_key = f'{dataset_prefix.lower()}_{base_key}'
                if lower_key in self.metrics:
                    return lower_key

                return base_key

            completeness_metrics = [
                ('BINs Present', 'records_with_bin', 'missing_bins'),
                ('BBLs Present', 'records_with_bbl', 'missing_bbls'),
                ('Addresses Complete', 'records_with_address', 'missing_addresses'),
                ('Project Dates Complete', 'records_with_project_dates', None),
            ]

            for label, present_key, missing_key in completeness_metrics:
                present_key_full = get_dataset_key(present_key, prefix)
                present = self.metrics.get(present_key_full, 0)
                pct = (present / total * 100) if total > 0 else 0
                report.append(f"  {label}: {present:,} ({pct:.1f}%)")
                if missing_key:
                    missing_key_full = get_dataset_key(missing_key, prefix)
                    missing = self.metrics.get(missing_key_full, 0)
                    if missing > 0:
                        report.append(f"    Missing: {missing:,}")
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

        report.append("")
        report.append("✅ Data Quality Report Complete")
        report.append("=" * 80)

        return "\n".join(report)

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
