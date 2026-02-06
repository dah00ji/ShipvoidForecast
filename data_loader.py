#!/usr/bin/env python3
"""
Data Loader for Shipvoid Forecast Application

Handles loading and processing of Shipvoid Forecast and Legacy Unbilled Cartons data.
"""

import glob
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

import config


class DataLoadError(Exception):
    """Raised when data loading fails."""
    pass


def find_newest_file(pattern: str, directory: Optional[str] = None) -> Optional[str]:
    """
    Find the newest file matching the given pattern in the directory.
    
    For Shipvoid files, parses the date from the filename (e.g., 'Shipvoid Forecast 01-30-2025_0600.xlsm')
    to ensure we get the truly newest file by date, not just modification time.
    
    Args:
        pattern: Glob pattern to match (e.g., 'Shipvoid*.xlsm')
        directory: Directory to search in (defaults to configured source path)
    
    Returns:
        Path to the newest matching file, or None if not found
    """
    import re
    
    if directory is None:
        directory = config.SOURCE_PATH
    
    search_path = os.path.join(directory, pattern)
    files = glob.glob(search_path)
    
    if not files:
        return None
    
    # Try to parse date from filename for Shipvoid files
    # Pattern: "Shipvoid Forecast MM-DD-YYYY_HHMM.xlsm"
    date_pattern = re.compile(r'(\d{2})-(\d{2})-(\d{4})_(\d{4})')
    
    def get_file_date(filepath):
        """Extract date from filename, fall back to mtime."""
        filename = os.path.basename(filepath)
        match = date_pattern.search(filename)
        if match:
            month, day, year, time = match.groups()
            try:
                # Return as sortable string: YYYYMMDD_HHMM
                return f"{year}{month}{day}_{time}"
            except:
                pass
        # Fallback to modification time
        return datetime.fromtimestamp(os.path.getmtime(filepath)).strftime('%Y%m%d_%H%M')
    
    # Sort by parsed date, newest first
    files.sort(key=get_file_date, reverse=True)
    
    newest = files[0]
    print(f"  Found {len(files)} files matching '{pattern}', using newest: {os.path.basename(newest)}")
    return newest


def load_shipvoid_forecast(file_path: str) -> pd.DataFrame:
    """
    Load and clean the Shipvoid Forecast Excel file.
    
    Reads both Inhouse Data and Crossdock Data sheets, creates container_id
    from Store + Div + Carton Number, and filters out VF/BILLED OR INACTIVE.
    """
    print(f"Loading Shipvoid Forecast from: {file_path}")
    
    # Column mapping from Excel to internal names
    columns_needed = {
        'Item Number': 'item',
        'Item Description': 'item_description',
        'PO Number': 'po',
        'Status': 'shipvoid_status',
        'Label Date': 'label_date',
        'Store': 'store',
        'Div': 'div',
        'Carton Number': 'carton_number',
        'Department': 'whse_dept',
        'Area': 'area',
        'Slot': 'slot',
        'Whpk Cost': 'cost',
        'Whpk': 'whpk_qty'
    }
    
    container_id_source_cols = ['Store', 'Div', 'Carton Number']
    
    dfs = []
    sheet_configs = [
        ('Inhouse Data', 'In House'),
        ('Crossdock Data', 'CrossDock')
    ]
    
    for sheet_name, source_label in sheet_configs:
        try:
            dtype_spec = {'Store': str, 'Div': str, 'Carton Number': str}
            sheet_df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=dtype_spec)
            
            # Create container_id
            missing_cols = [c for c in container_id_source_cols if c not in sheet_df.columns]
            if missing_cols:
                if 'Container ID' in sheet_df.columns:
                    sheet_df['container_id'] = sheet_df['Container ID'].astype(str).str.strip()
                else:
                    raise ValueError(f"Cannot create container_id - missing: {missing_cols}")
            else:
                sheet_df['container_id'] = (
                    sheet_df['Store'].astype(str).str.strip() + 
                    sheet_df['Div'].astype(str).str.strip() + 
                    sheet_df['Carton Number'].astype(str).str.strip()
                )
            
            available_cols = [c for c in columns_needed.keys() if c in sheet_df.columns]
            sheet_df = sheet_df[available_cols + ['container_id']].rename(columns=columns_needed)
            sheet_df['source_type'] = source_label
            dfs.append(sheet_df)
            print(f"  Loaded {len(sheet_df):,} records from '{sheet_name}' sheet")
        except Exception as e:
            print(f"  Warning: Could not load sheet '{sheet_name}': {e}")
    
    if not dfs:
        raise DataLoadError("No data sheets found in Shipvoid Forecast file!")
    
    df = pd.concat(dfs, ignore_index=True)
    df['container_id'] = df['container_id'].astype(str).str.strip()
    df['label_date'] = pd.to_datetime(df['label_date'], errors='coerce').dt.date
    
    # Normalize status values (no longer filtering - user can filter in UI)
    df['shipvoid_status'] = df['shipvoid_status'].astype(str).str.strip().str.upper()
    print(f"  Total: {len(df):,} records (all statuses included)")
    
    return df


def load_legacy_unbilled(file_path: str) -> pd.DataFrame:
    """
    Load and process the Legacy Unbilled Cartons CSV file.
    
    Finds the latest event timestamp and corresponding status/location for each container.
    """
    print(f"Loading Legacy Unbilled Cartons from: {file_path}")
    df = pd.read_csv(file_path, low_memory=False, dtype={'container_id': str})
    df['container_id'] = df['container_id'].str.strip()
    
    event_ts_cols = ['event_ts_1', 'event_ts_2', 'event_ts_3', 'event_ts_4', 'event_ts_5']
    status_cols = ['status_1', 'status_2', 'status_3', 'status_4', 'status_5']
    event_type_cols = ['event_type_1', 'event_type_2', 'event_type_3', 'event_type_4', 'event_type_5']
    location_cols = ['location_id_1', 'location_id_2', 'location_id_3', 'location_id_4', 'location_id_5']
    
    for col in event_ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    def get_latest_event(row):
        latest_ts = None
        latest_status = None
        latest_event_name = None
        latest_location = None
        
        for ts_col, status_col, event_col, loc_col in zip(event_ts_cols, status_cols, event_type_cols, location_cols):
            if ts_col in row.index and status_col in row.index:
                ts_val = row[ts_col]
                if pd.notna(ts_val):
                    if latest_ts is None or ts_val > latest_ts:
                        latest_ts = ts_val
                        latest_status = row[status_col] if status_col in row.index else None
                        latest_event_name = row[event_col] if event_col in row.index else None
                        latest_location = row[loc_col] if loc_col in row.index else None
        
        return pd.Series({
            'latest_event_ts': latest_ts, 
            'latest_event_status': latest_status,
            'latest_event_name': latest_event_name,
            'atlas_location': latest_location
        })
    
    print("  Processing event timestamps...")
    latest_events = df.apply(get_latest_event, axis=1)
    df = pd.concat([df, latest_events], axis=1)
    
    result = df[['container_id', 'container_create_date', 'latest_event_ts', 
                 'latest_event_status', 'latest_event_name', 'atlas_location']].copy()
    
    print(f"  Loaded {len(result):,} records")
    return result


def merge_data(shipvoid_df: pd.DataFrame, legacy_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge datasets on container_id with timeline validation.
    
    Validates that container_create_date matches label_date to handle
    re-used/wrapped container IDs.
    """
    print("Merging datasets on container_id...")
    
    legacy_df = legacy_df.copy()
    legacy_df['container_create_date'] = pd.to_datetime(legacy_df['container_create_date'], errors='coerce')
    legacy_df['container_create_date_only'] = legacy_df['container_create_date'].dt.date
    
    legacy_grouped = legacy_df.sort_values('latest_event_ts', ascending=False)
    legacy_grouped = legacy_grouped.drop_duplicates(subset='container_id', keep='first')
    
    merged = pd.merge(
        shipvoid_df,
        legacy_grouped[['container_id', 'container_create_date', 'container_create_date_only', 
                        'latest_event_ts', 'latest_event_status', 'latest_event_name', 'atlas_location']],
        on='container_id',
        how='left'
    )
    
    # Timeline validation
    has_both_dates = merged['label_date'].notna() & merged['container_create_date_only'].notna()
    dates_match = merged['label_date'] == merged['container_create_date_only']
    timeline_mismatch = has_both_dates & ~dates_match
    mismatch_count = timeline_mismatch.sum()
    
    if mismatch_count > 0:
        print(f"  Timeline validation: {mismatch_count:,} mismatched records (clearing legacy data)")
        merged.loc[timeline_mismatch, ['container_create_date', 'container_create_date_only', 
                                        'latest_event_ts', 'latest_event_status', 
                                        'latest_event_name', 'atlas_location']] = None
    
    merged = merged.drop(columns=['container_create_date_only', 'container_create_date'], errors='ignore')
    
    # Group by container_id
    agg_cols = {
        'item': 'first', 'item_description': 'first', 'po': 'first',
        'shipvoid_status': 'first', 'label_date': 'first', 'store': 'first',
        'div': 'first', 'carton_number': 'first', 'whse_dept': 'first',
        'area': 'first', 'slot': 'first', 'source_type': 'first',
        'latest_event_ts': 'first', 'latest_event_status': 'first',
        'latest_event_name': 'first', 'atlas_location': 'first',
        'cost': 'first', 'whpk_qty': 'first'
    }
    agg_cols = {k: v for k, v in agg_cols.items() if k in merged.columns}
    
    merged_grouped = merged.groupby('container_id').agg(agg_cols).reset_index()
    print(f"  Merged: {len(merged_grouped):,} unique containers")
    
    return merged_grouped


def load_all_data(source_path: Optional[str] = None, legacy_path: Optional[str] = None) -> dict:
    """
    Load all data from the configured source paths.
    
    Args:
        source_path: Override the default Shipvoid source path
        legacy_path: Override the default Legacy Unbilled source path
    
    Returns:
        Dictionary with 'data', 'stats', 'files', and 'error' keys
    """
    if source_path:
        config.set_shipvoid_source_path(source_path)
    
    # Use custom legacy path or default
    legacy_source = legacy_path if legacy_path else config.LEGACY_SOURCE_PATH
    
    result = {
        'data': [],
        'stats': {},
        'files': {'shipvoid': None, 'legacy': None},
        'error': None,
        'load_time': datetime.now().isoformat()
    }
    
    try:
        # Find newest Shipvoid file from network share
        shipvoid_file = find_newest_file(config.SHIPVOID_PATTERN, config.SHIPVOID_SOURCE_PATH)
        if not shipvoid_file:
            for pattern in config.SHIPVOID_FALLBACK_PATTERNS:
                shipvoid_file = find_newest_file(pattern, config.SHIPVOID_SOURCE_PATH)
                if shipvoid_file:
                    break
        
        # Find Legacy file from specified path or local repo
        legacy_file = find_newest_file(config.LEGACY_PATTERN, legacy_source)
        
        if not shipvoid_file:
            raise DataLoadError(f"No Shipvoid file found matching '{config.SHIPVOID_PATTERN}' in {config.SHIPVOID_SOURCE_PATH}")
        
        result['files']['shipvoid'] = shipvoid_file
        result['files']['legacy'] = legacy_file  # May be None
        
        # Load Shipvoid data (required)
        shipvoid_df = load_shipvoid_forecast(shipvoid_file)
        
        # Load and merge Legacy data (optional)
        if legacy_file:
            legacy_df = load_legacy_unbilled(legacy_file)
            merged_df = merge_data(shipvoid_df, legacy_df)
        else:
            print("  [INFO] No Legacy file found - showing Shipvoid data only")
            # Use Shipvoid data without Legacy merge
            merged_df = shipvoid_df.copy()
            # Add empty columns for Legacy fields
            merged_df['latest_event_ts'] = None
            merged_df['latest_event_status'] = ''
            merged_df['latest_event_name'] = ''
            merged_df['atlas_location'] = ''
        
        # Convert to display format
        merged_df['label_date'] = merged_df['label_date'].astype(str)
        merged_df['latest_event_ts'] = merged_df['latest_event_ts'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
        )
        merged_df = merged_df.fillna('')
        
        result['data'] = merged_df.to_dict('records')
        
        # Calculate potential shipvoid cost (exclude already billed/VF)
        # Cost = Sum of Whpk Cost for at-risk containers (each row = 1 container)
        billed_statuses = ['VF', 'BILLED OR INACTIVE']
        at_risk_df = merged_df[~merged_df['shipvoid_status'].isin(billed_statuses)]
        
        # Sum the cost column, converting to numeric first
        if 'cost' in at_risk_df.columns:
            at_risk_df_copy = at_risk_df.copy()
            at_risk_df_copy['cost_numeric'] = pd.to_numeric(at_risk_df_copy['cost'], errors='coerce').fillna(0)
            total_potential_cost = at_risk_df_copy['cost_numeric'].sum()
        else:
            total_potential_cost = 0
        
        # Calculate stats
        result['stats'] = {
            'total': len(merged_df),
            'inhouse': len(merged_df[merged_df['source_type'] == 'In House']),
            'crossdock': len(merged_df[merged_df['source_type'] == 'CrossDock']),
            'oldest_date': min([d for d in merged_df['label_date'] if d and d != 'NaT']) if len(merged_df) > 0 else 'N/A',
            'shipvoid_file': os.path.basename(shipvoid_file),
            'legacy_file': os.path.basename(legacy_file) if legacy_file else 'Not found (optional)',
            'potential_cost': total_potential_cost,
            'at_risk_count': len(at_risk_df),
        }
        
        print(f"\n[OK] Data loaded successfully: {len(result['data']):,} records")
        
    except Exception as e:
        result['error'] = str(e)
        print(f"\n[ERROR] Error loading data: {e}")
    
    return result
