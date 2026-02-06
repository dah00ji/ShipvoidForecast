#!/usr/bin/env python3
"""
Shipvoid Forecast & Legacy Unbilled Cartons Cross-Reference Report Generator

This script merges data from:
1. Shipvoid Forecast (Excel .xlsm) - contains container status/location with Inhouse and Crossdock sheets
2. Legacy Unbilled Cartons (CSV) - contains event timestamps and statuses

Both files can be downloaded automatically from SharePoint.

Output: Interactive HTML report with pivot dashboard and filterable table

Data Source: SharePoint folder at:
https://teams.wal-mart.com/:f:/r/sites/AtlasAmbientRDCPlaybookPlanning/Shared%20Documents/Shipvoid%20Forecast/6031
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# Import SharePoint downloader for pulling files from Teams
try:
    from sharepoint_downloader import download_shipvoid_files
    SHAREPOINT_AVAILABLE = True
except ImportError:
    SHAREPOINT_AVAILABLE = False
    print("Warning: SharePoint downloader not available. Using local files only.")


def load_shipvoid_forecast(file_path: str) -> pd.DataFrame:
    """Load and clean the Shipvoid Forecast Excel file (both Inhouse and Crossdock sheets)."""
    print(f"Loading Shipvoid Forecast from: {file_path}")
    
    # Column mapping from Excel to internal names
    # Note: Container ID is now created by concatenating Store + Div + Carton Number
    columns_needed = {
        'Item Number': 'item',
        'Item Description': 'item_description',
        'PO Number': 'po',
        'Status': 'shipvoid_status',
        'Label Date': 'label_date',
        'Store': 'store',
        'Div': 'div',
        'Carton Number': 'carton_number',
        'Whse Dept': 'whse_dept',
        'Area': 'area',
        'Slot': 'slot'
    }
    
    # Columns needed to build container_id
    container_id_source_cols = ['Store', 'Div', 'Carton Number']
    
    # Read both Inhouse Data and Crossdock Data sheets
    dfs = []
    sheet_configs = [
        ('Inhouse Data', 'In House'),
        ('Crossdock Data', 'CrossDock')
    ]
    
    for sheet_name, source_label in sheet_configs:
        try:
            # Read with string dtype for columns used to build container_id
            dtype_spec = {'Store': str, 'Div': str, 'Carton Number': str}
            sheet_df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=dtype_spec)
            
            # Create container_id by concatenating Store + Div + Carton Number
            missing_cols = [c for c in container_id_source_cols if c not in sheet_df.columns]
            if missing_cols:
                print(f"  Warning: Missing columns for container_id creation: {missing_cols}")
                # If Container ID column exists as fallback, use it
                if 'Container ID' in sheet_df.columns:
                    sheet_df['container_id'] = sheet_df['Container ID'].astype(str).str.strip()
                    print(f"  Using existing 'Container ID' column as fallback")
                else:
                    raise ValueError(f"Cannot create container_id - missing: {missing_cols}")
            else:
                # Create container_id from Store + Div + Carton Number
                sheet_df['container_id'] = (
                    sheet_df['Store'].astype(str).str.strip() + 
                    sheet_df['Div'].astype(str).str.strip() + 
                    sheet_df['Carton Number'].astype(str).str.strip()
                )
                print(f"  Created container_id from Store + Div + Carton Number")
            
            # Only select columns that exist in this sheet
            available_cols = [c for c in columns_needed.keys() if c in sheet_df.columns]
            sheet_df = sheet_df[available_cols + ['container_id']].rename(columns=columns_needed)
            sheet_df['source_type'] = source_label
            dfs.append(sheet_df)
            print(f"  Loaded {len(sheet_df):,} records from '{sheet_name}' sheet")
        except Exception as e:
            print(f"  Warning: Could not load sheet '{sheet_name}': {e}")
    
    if not dfs:
        raise ValueError("No data sheets found in Shipvoid Forecast file!")
    
    # Combine all sheets
    df = pd.concat(dfs, ignore_index=True)
    
    # Clean container_id - ensure it's a string and strip whitespace
    df['container_id'] = df['container_id'].astype(str).str.strip()
    
    # Parse label_date - remove time component
    df['label_date'] = pd.to_datetime(df['label_date'], errors='coerce').dt.date
    
    print(f"  Total loaded: {len(df):,} records from Shipvoid Forecast")
    
    # Filter out VF, BILLED OR INACTIVE statuses
    excluded_statuses = ['VF', 'BILLED OR INACTIVE']
    df['shipvoid_status'] = df['shipvoid_status'].astype(str).str.strip().str.upper()
    original_count = len(df)
    df = df[~df['shipvoid_status'].isin(excluded_statuses)]
    filtered_count = original_count - len(df)
    print(f"  Filtered out {filtered_count:,} records with VF / BILLED OR INACTIVE status")
    print(f"  Remaining records: {len(df):,}")
    
    return df


def load_legacy_unbilled(file_path: str) -> pd.DataFrame:
    """Load and process the Legacy Unbilled Cartons CSV file."""
    print(f"Loading Legacy Unbilled Cartons from: {file_path}")
    # Read CSV - ensure container_id is read as string to preserve leading zeros
    df = pd.read_csv(file_path, low_memory=False, dtype={'container_id': str})
    
    # Clean container_id
    df['container_id'] = df['container_id'].str.strip()
    
    # Find the latest event timestamp, corresponding status, event name, and location
    event_ts_cols = ['event_ts_1', 'event_ts_2', 'event_ts_3', 'event_ts_4', 'event_ts_5']
    status_cols = ['status_1', 'status_2', 'status_3', 'status_4', 'status_5']
    event_type_cols = ['event_type_1', 'event_type_2', 'event_type_3', 'event_type_4', 'event_type_5']
    location_cols = ['location_id_1', 'location_id_2', 'location_id_3', 'location_id_4', 'location_id_5']
    
    # Convert event timestamps to datetime
    for col in event_ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    def get_latest_event(row):
        """Find the latest event timestamp, its corresponding status, event name, and location."""
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
    
    print("  Processing event timestamps to find latest events...")
    latest_events = df.apply(get_latest_event, axis=1)
    df = pd.concat([df, latest_events], axis=1)
    
    # Select only needed columns
    result = df[['container_id', 'container_create_date', 'latest_event_ts', 'latest_event_status', 'latest_event_name', 'atlas_location']].copy()
    
    print(f"  Loaded {len(result):,} records from Legacy Unbilled Cartons")
    return result


def merge_data(shipvoid_df: pd.DataFrame, legacy_df: pd.DataFrame) -> pd.DataFrame:
    """Merge the two datasets on container_id with timeline validation.
    
    Since container IDs can be re-used (wrapped), we validate that the
    container_create_date from legacy data matches the label_date from
    shipvoid forecast to ensure we're comparing the same containers.
    """
    print("Merging datasets on container_id...")
    
    # Ensure container_create_date is parsed as datetime
    legacy_df = legacy_df.copy()
    legacy_df['container_create_date'] = pd.to_datetime(legacy_df['container_create_date'], errors='coerce')
    legacy_df['container_create_date_only'] = legacy_df['container_create_date'].dt.date
    
    # Group legacy data by container_id, taking the latest event
    legacy_grouped = legacy_df.sort_values('latest_event_ts', ascending=False)
    legacy_grouped = legacy_grouped.drop_duplicates(subset='container_id', keep='first')
    
    # Merge on container_id
    merged = pd.merge(
        shipvoid_df,
        legacy_grouped[['container_id', 'container_create_date', 'container_create_date_only', 'latest_event_ts', 'latest_event_status', 'latest_event_name', 'atlas_location']],
        on='container_id',
        how='left'
    )
    
    # Timeline validation: filter out records where container_create_date doesn't match label_date
    # This prevents matching re-used/wrapped container IDs from different time periods
    pre_filter_count = len(merged)
    
    # Only validate where both dates exist
    has_both_dates = merged['label_date'].notna() & merged['container_create_date_only'].notna()
    
    # Check if dates match (allowing for same-day match)
    dates_match = merged['label_date'] == merged['container_create_date_only']
    
    # Keep records where: no legacy match found OR dates match
    no_legacy_match = merged['container_create_date_only'].isna()
    valid_timeline = no_legacy_match | dates_match
    
    # For mismatched timelines, clear the legacy data (treat as no match)
    timeline_mismatch = has_both_dates & ~dates_match
    mismatch_count = timeline_mismatch.sum()
    
    if mismatch_count > 0:
        print(f"  Timeline validation: {mismatch_count:,} records had mismatched container_create_date vs label_date")
        print(f"  These container IDs are likely re-used/wrapped - clearing legacy match data")
        # Clear legacy data for mismatched records instead of dropping them
        merged.loc[timeline_mismatch, ['container_create_date', 'container_create_date_only', 'latest_event_ts', 'latest_event_status', 'latest_event_name', 'atlas_location']] = None
    else:
        print(f"  Timeline validation: All matched records have consistent dates")
    
    # Drop the helper column
    merged = merged.drop(columns=['container_create_date_only', 'container_create_date'], errors='ignore')
    
    # Group by container_id to consolidate multiple items per container
    # Keep first occurrence of each container (they should have same status)
    # Build aggregation dict dynamically based on available columns
    agg_cols = {
        'item': 'first',
        'item_description': 'first',
        'po': 'first',
        'shipvoid_status': 'first',
        'label_date': 'first',
        'store': 'first',
        'div': 'first',
        'carton_number': 'first',
        'whse_dept': 'first',
        'area': 'first',
        'slot': 'first',
        'source_type': 'first',
        'latest_event_ts': 'first',
        'latest_event_status': 'first',
        'latest_event_name': 'first',
        'atlas_location': 'first'
    }
    # Only include columns that exist in the merged dataframe
    agg_cols = {k: v for k, v in agg_cols.items() if k in merged.columns}
    
    merged_grouped = merged.groupby('container_id').agg(agg_cols).reset_index()
    
    print(f"  Merged data contains {len(merged_grouped):,} unique containers")
    return merged_grouped


def generate_pivot_data(df: pd.DataFrame) -> dict:
    """Generate pivot data for the dashboard - container count by date."""
    # Group by label_date and count containers
    df['label_date_str'] = df['label_date'].astype(str)
    pivot = df.groupby('label_date_str').size().reset_index(name='container_count')
    pivot = pivot.sort_values('label_date_str')
    
    return {
        'dates': pivot['label_date_str'].tolist(),
        'counts': pivot['container_count'].tolist()
    }


def generate_html_report(df: pd.DataFrame, pivot_data: dict, output_path: str):
    """Generate the interactive HTML report."""
    print(f"Generating HTML report: {output_path}")
    
    # Prepare table data
    df_display = df.copy()
    df_display['label_date'] = df_display['label_date'].astype(str)
    df_display['latest_event_ts'] = df_display['latest_event_ts'].apply(
        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else ''
    )
    df_display = df_display.fillna('')
    
    table_data = df_display.to_dict('records')
    
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>RDC 6006 Cullman, AL - Shipvoid Forecast vs Legacy Unbilled Cartons</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --primary-color: #0071ce;
            --secondary-color: #ffc220;
            --bg-color: #f5f5f5;
            --card-bg: #ffffff;
            --text-color: #333333;
            --border-color: #e0e0e0;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: var(--bg-color);
            color: var(--text-color);
            line-height: 1.6;
        }}
        
        .header {{
            background: linear-gradient(135deg, var(--primary-color), #1a4f7a);
            color: white;
            padding: 20px 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        
        .logo-container {{
            text-align: center;
            margin-bottom: 15px;
        }}
        
        .team-logo {{
            max-height: 80px;
            width: auto;
        }}
        
        .header h1 {{
            font-size: 1.8rem;
            font-weight: 600;
            text-align: center;
        }}
        
        .header p {{
            opacity: 0.9;
            margin-top: 5px;
            text-align: center;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .dashboard {{
            background: var(--card-bg);
            border-radius: 12px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        
        .dashboard h2 {{
            color: var(--primary-color);
            margin-bottom: 20px;
            font-size: 1.3rem;
            text-align: center;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 25px;
        }}
        
        .stat-card {{
            background: linear-gradient(135deg, var(--primary-color), #004c91);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        
        .stat-card.secondary {{
            background: linear-gradient(135deg, var(--secondary-color), #e6a800);
            color: #333;
        }}
        
        .stat-value {{
            font-size: 2.5rem;
            font-weight: bold;
        }}
        
        .stat-label {{
            font-size: 0.9rem;
            opacity: 0.9;
            margin-top: 5px;
        }}
        
        .chart-container {{
            height: 300px;
            position: relative;
        }}
        
        .table-section {{
            background: var(--card-bg);
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        }}
        
        .table-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }}
        
        .table-header h2 {{
            color: var(--primary-color);
            font-size: 1.3rem;
            text-align: center;
            width: 100%;
        }}
        
        .filters {{
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
        }}
        
        .filter-input {{
            padding: 10px 15px;
            border: 2px solid var(--border-color);
            border-radius: 8px;
            font-size: 0.95rem;
            transition: border-color 0.2s;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: var(--primary-color);
        }}
        
        .btn {{
            padding: 10px 20px;
            background: var(--primary-color);
            color: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 0.95rem;
            transition: background 0.2s;
        }}
        
        .btn:hover {{
            background: #005a9e;
        }}
        
        .btn.secondary {{
            background: #6c757d;
        }}
        
        .btn.secondary:hover {{
            background: #5a6268;
        }}
        
        .table-wrapper {{
            overflow-x: auto;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }}
        
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}
        
        th {{
            background: var(--primary-color);
            color: white;
            font-weight: 600;
            cursor: pointer;
            user-select: none;
            position: sticky;
            top: 0;
        }}
        
        th:hover {{
            background: #005a9e;
        }}
        
        th .sort-icon {{
            margin-left: 5px;
            opacity: 0.7;
        }}
        
        tr:hover {{
            background: #f8f9fa;
        }}
        
        .status-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
        }}
        
        .status-DOW {{
            background: #28a745;
            color: white;
        }}
        
        .status-VF {{
            background: #dc3545;
            color: white;
        }}
        
        .status-LOADED {{
            background: #17a2b8;
            color: white;
        }}
        
        .status-PICKED {{
            background: #ffc107;
            color: #333;
        }}
        
        .status-default {{
            background: #6c757d;
            color: white;
        }}
        
        .source-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
        }}
        
        .source-InHouse {{
            background: #28a745;
            color: white;
        }}
        
        .source-CrossDock {{
            background: #17a2b8;
            color: white;
        }}
        
        .pagination {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 20px;
            flex-wrap: wrap;
            gap: 15px;
        }}
        
        .page-info {{
            color: #666;
        }}
        
        .page-buttons {{
            display: flex;
            gap: 5px;
        }}
        
        .page-btn {{
            padding: 8px 15px;
            border: 1px solid var(--border-color);
            background: white;
            cursor: pointer;
            border-radius: 5px;
            transition: all 0.2s;
        }}
        
        .page-btn:hover {{
            background: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }}
        
        .page-btn.active {{
            background: var(--primary-color);
            color: white;
            border-color: var(--primary-color);
        }}
        
        .page-btn:disabled {{
            opacity: 0.5;
            cursor: not-allowed;
        }}
        
        /* Toast notification styles */
        .toast {{
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: #28a745;
            color: white;
            padding: 15px 25px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.2);
            z-index: 9999;
            opacity: 0;
            transform: translateY(20px);
            transition: all 0.3s ease;
            font-weight: 500;
        }}
        
        .toast.show {{
            opacity: 1;
            transform: translateY(0);
        }}
        
        @media (max-width: 768px) {{
            .header {{
                padding: 15px 20px;
            }}
            
            .container {{
                padding: 15px;
            }}
            
            .stats-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}
            
            .stat-value {{
                font-size: 1.8rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="logo-container">
            <img src="team-logo.png" alt="Team Logo" class="team-logo">
        </div>
        <h1>📊 RDC 6006 Cullman, AL - Shipvoid Forecast vs Legacy Unbilled Cartons 📊</h1>
        <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    
    <!-- Toast notification -->
    <div id="toast" class="toast"></div>
    
    <div class="container">
        <div class="dashboard">
            <h2>📊 Dashboard - Container Count by Date</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <div class="stat-value" id="total-picks">{len(df):,}</div>
                    <div class="stat-label">Total Picks</div>
                </div>
                <div class="stat-card secondary">
                    <div class="stat-value" id="inhouse-picks">0</div>
                    <div class="stat-label">Total Inhouse Picks</div>
                </div>
                <div class="stat-card">
                    <div class="stat-value" id="crossdock-picks">0</div>
                    <div class="stat-label">Total CrossDock Picks</div>
                </div>
                <div class="stat-card secondary">
                    <div class="stat-value" id="oldest-date">{min(pivot_data['dates']) if pivot_data['dates'] else 'N/A'}</div>
                    <div class="stat-label">Oldest Date</div>
                </div>
            </div>
            <div class="chart-container">
                <canvas id="dateChart"></canvas>
            </div>
        </div>
        
        <div class="table-section">
            <div class="table-header">
                <h2>📋 Detailed Container Data</h2>
                <div class="filters">
                    <input type="text" id="searchInput" class="filter-input" placeholder="Search all columns...">
                    <select id="sourceFilter" class="filter-input">
                        <option value="">All Sources</option>
                        <option value="In House">In House</option>
                        <option value="CrossDock">CrossDock</option>
                    </select>
                    <select id="statusFilter" class="filter-input">
                        <option value="">All Shipvoid Statuses</option>
                    </select>
                    <select id="whseDeptFilter" class="filter-input">
                        <option value="">All Whse Depts</option>
                    </select>
                    <select id="areaFilter" class="filter-input">
                        <option value="">All Areas</option>
                    </select>
                    <select id="slotFilter" class="filter-input">
                        <option value="">All Slots</option>
                    </select>
                    <select id="latestStatusFilter" class="filter-input">
                        <option value="">All Atlas Statuses</option>
                        <option value="__BLANK__">-- Blank (Not in Legacy) --</option>
                    </select>
                    <select id="latestEventNameFilter" class="filter-input">
                        <option value="">All Event Names</option>
                        <option value="__BLANK__">-- Blank (Not in Legacy) --</option>
                    </select>
                    <select id="dateFilter" class="filter-input">
                        <option value="">All Dates</option>
                    </select>
                    <button class="btn secondary" onclick="resetFilters()">Reset</button>
                    <button class="btn" onclick="exportToCSV()">Export CSV</button>
                    <button class="btn" onclick="copyContainerIds()">📋 Copy Container IDs</button>
                </div>
            </div>
            <div class="table-wrapper">
                <table id="dataTable">
                    <thead>
                        <tr>
                            <th onclick="sortTable('container_id')">Container ID <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('source_type')">Source <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('item')">Item <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('item_description')">Item Description <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('po')">PO <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('whse_dept')">Whse Dept <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('area')">Area <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('slot')">Slot <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('shipvoid_status')">Legacy Cartons <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('latest_event_ts')">Latest Event Time <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('latest_event_name')">Latest Event Name <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('latest_event_status')">Atlas Status <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('atlas_location')">Atlas Location <span class="sort-icon">↕</span></th>
                            <th onclick="sortTable('label_date')">Label Date <span class="sort-icon">↕</span></th>
                        </tr>
                    </thead>
                    <tbody id="tableBody">
                    </tbody>
                </table>
            </div>
            <div class="pagination">
                <div class="page-info">
                    Showing <span id="showingStart">1</span>-<span id="showingEnd">50</span> of <span id="totalRecords">{len(df):,}</span> records
                </div>
                <div>
                    <select id="pageSize" class="filter-input" onchange="changePageSize()">
                        <option value="25">25 per page</option>
                        <option value="50" selected>50 per page</option>
                        <option value="100">100 per page</option>
                        <option value="250">250 per page</option>
                    </select>
                </div>
                <div class="page-buttons" id="pageButtons">
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Data
        const allData = {json.dumps(table_data)};
        const pivotData = {json.dumps(pivot_data)};
        
        // State
        let filteredData = [...allData];
        let currentPage = 1;
        let pageSize = 50;
        let sortColumn = null;
        let sortDirection = 'asc';
        
        // Initialize
        document.addEventListener('DOMContentLoaded', function() {{
            initChart();
            initFilters();
            updateStats();
            renderTable();
        }});
        
        let dateChart = null;
        
        function initChart() {{
            const ctx = document.getElementById('dateChart').getContext('2d');
            dateChart = new Chart(ctx, {{
                type: 'bar',
                data: {{
                    labels: pivotData.dates,
                    datasets: [{{
                        label: 'Container Count',
                        data: pivotData.counts,
                        backgroundColor: 'rgba(0, 113, 206, 0.8)',
                        borderColor: 'rgba(0, 113, 206, 1)',
                        borderWidth: 1
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{
                            display: false
                        }}
                    }},
                    scales: {{
                        y: {{
                            beginAtZero: true,
                            ticks: {{
                                callback: function(value) {{
                                    return value.toLocaleString();
                                }}
                            }}
                        }}
                    }}
                }}
            }});
        }}
        
        function updateChart(data) {{
            // Calculate new pivot data from filtered data
            const dateCounts = {{}};
            data.forEach(row => {{
                const date = row.label_date;
                dateCounts[date] = (dateCounts[date] || 0) + 1;
            }});
            
            const sortedDates = Object.keys(dateCounts).sort();
            const counts = sortedDates.map(d => dateCounts[d]);
            
            // Update chart data
            dateChart.data.labels = sortedDates;
            dateChart.data.datasets[0].data = counts;
            dateChart.update();
        }}
        
        function initFilters() {{
            // Helper function to populate a filter dropdown
            function populateFilter(elementId, dataKey, allowBlank = false) {{
                const filter = document.getElementById(elementId);
                const uniqueValues = [...new Set(allData.map(d => d[dataKey]).filter(s => s && s !== ''))].sort();
                uniqueValues.forEach(val => {{
                    const option = document.createElement('option');
                    option.value = val;
                    option.textContent = val;
                    filter.appendChild(option);
                }});
            }}
            
            // Populate all filters
            populateFilter('dateFilter', 'label_date');
            populateFilter('statusFilter', 'shipvoid_status');
            populateFilter('whseDeptFilter', 'whse_dept');
            populateFilter('areaFilter', 'area');
            populateFilter('slotFilter', 'slot');
            populateFilter('latestStatusFilter', 'latest_event_status');
            populateFilter('latestEventNameFilter', 'latest_event_name');
            
            // Add event listeners
            document.getElementById('searchInput').addEventListener('input', applyFilters);
            document.getElementById('sourceFilter').addEventListener('change', applyFilters);
            document.getElementById('statusFilter').addEventListener('change', applyFilters);
            document.getElementById('whseDeptFilter').addEventListener('change', applyFilters);
            document.getElementById('areaFilter').addEventListener('change', applyFilters);
            document.getElementById('slotFilter').addEventListener('change', applyFilters);
            document.getElementById('latestStatusFilter').addEventListener('change', applyFilters);
            document.getElementById('latestEventNameFilter').addEventListener('change', applyFilters);
            document.getElementById('dateFilter').addEventListener('change', applyFilters);
        }}
        
        function updateStats(data) {{
            // Use provided data (filtered) or default to all data
            const targetData = data || allData;
            
            // Calculate pick counts
            const totalPicks = targetData.length;
            const inhousePicks = targetData.filter(d => d.source_type === 'In House').length;
            const crossdockPicks = targetData.filter(d => d.source_type === 'CrossDock').length;
            
            document.getElementById('total-picks').textContent = totalPicks.toLocaleString();
            document.getElementById('inhouse-picks').textContent = inhousePicks.toLocaleString();
            document.getElementById('crossdock-picks').textContent = crossdockPicks.toLocaleString();
            
            // Update oldest date
            const dates = targetData.map(d => d.label_date).filter(d => d && d !== '');
            const oldestDate = dates.length > 0 ? dates.sort()[0] : 'N/A';
            document.getElementById('oldest-date').textContent = oldestDate;
        }}
        
        function applyFilters() {{
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            const sourceFilter = document.getElementById('sourceFilter').value;
            const statusFilter = document.getElementById('statusFilter').value;
            const whseDeptFilter = document.getElementById('whseDeptFilter').value;
            const areaFilter = document.getElementById('areaFilter').value;
            const slotFilter = document.getElementById('slotFilter').value;
            const latestStatusFilter = document.getElementById('latestStatusFilter').value;
            const latestEventNameFilter = document.getElementById('latestEventNameFilter').value;
            const dateFilter = document.getElementById('dateFilter').value;
            
            filteredData = allData.filter(row => {{
                // Search filter
                if (searchTerm) {{
                    const searchable = Object.values(row).join(' ').toLowerCase();
                    if (!searchable.includes(searchTerm)) return false;
                }}
                
                // Source filter (In House / CrossDock)
                if (sourceFilter && row.source_type !== sourceFilter) return false;
                
                // Shipvoid Status filter
                if (statusFilter && row.shipvoid_status !== statusFilter) return false;
                
                // Whse Dept filter
                if (whseDeptFilter && row.whse_dept !== whseDeptFilter) return false;
                
                // Area filter
                if (areaFilter && row.area !== areaFilter) return false;
                
                // Slot filter
                if (slotFilter && row.slot !== slotFilter) return false;
                
                // Atlas Status filter
                if (latestStatusFilter) {{
                    if (latestStatusFilter === '__BLANK__') {{
                        // Filter for blank/empty latest status (not found in Legacy report)
                        if (row.latest_event_status && row.latest_event_status !== '') return false;
                    }} else {{
                        if (row.latest_event_status !== latestStatusFilter) return false;
                    }}
                }}
                
                // Latest Event Name filter
                if (latestEventNameFilter) {{
                    if (latestEventNameFilter === '__BLANK__') {{
                        // Filter for blank/empty event name (not found in Legacy report)
                        if (row.latest_event_name && row.latest_event_name !== '') return false;
                    }} else {{
                        if (row.latest_event_name !== latestEventNameFilter) return false;
                    }}
                }}
                
                // Date filter
                if (dateFilter && row.label_date !== dateFilter) return false;
                
                return true;
            }});
            
            currentPage = 1;
            updateStats(filteredData);
            updateChart(filteredData);
            renderTable();
        }}
        
        function sortTable(column) {{
            if (sortColumn === column) {{
                sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
            }} else {{
                sortColumn = column;
                sortDirection = 'asc';
            }}
            
            filteredData.sort((a, b) => {{
                let valA = a[column] || '';
                let valB = b[column] || '';
                
                if (typeof valA === 'string') valA = valA.toLowerCase();
                if (typeof valB === 'string') valB = valB.toLowerCase();
                
                if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
                if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            }});
            
            renderTable();
        }}
        
        function renderTable() {{
            const tbody = document.getElementById('tableBody');
            const start = (currentPage - 1) * pageSize;
            const end = Math.min(start + pageSize, filteredData.length);
            const pageData = filteredData.slice(start, end);
            
            tbody.innerHTML = pageData.map(row => `
                <tr>
                    <td>${{row.container_id}}</td>
                    <td><span class="source-badge source-${{(row.source_type || '').replace(' ', '')}}">${{row.source_type || '-'}}</span></td>
                    <td>${{row.item}}</td>
                    <td>${{row.item_description || '-'}}</td>
                    <td>${{row.po}}</td>
                    <td>${{row.whse_dept || '-'}}</td>
                    <td>${{row.area || '-'}}</td>
                    <td>${{row.slot || '-'}}</td>
                    <td><span class="status-badge status-${{row.shipvoid_status || 'default'}}">${{row.shipvoid_status || '-'}}</span></td>
                    <td>${{row.latest_event_ts || '-'}}</td>
                    <td>${{row.latest_event_name || '-'}}</td>
                    <td><span class="status-badge status-${{row.latest_event_status || 'default'}}">${{row.latest_event_status || '-'}}</span></td>
                    <td>${{row.atlas_location || '-'}}</td>
                    <td>${{row.label_date}}</td>
                </tr>
            `).join('');
            
            // Update pagination info
            document.getElementById('showingStart').textContent = filteredData.length > 0 ? start + 1 : 0;
            document.getElementById('showingEnd').textContent = end;
            document.getElementById('totalRecords').textContent = filteredData.length.toLocaleString();
            
            renderPagination();
        }}
        
        function renderPagination() {{
            const totalPages = Math.ceil(filteredData.length / pageSize);
            const pageButtons = document.getElementById('pageButtons');
            
            let html = '';
            html += `<button class="page-btn" onclick="goToPage(1)" ${{currentPage === 1 ? 'disabled' : ''}}>&laquo;</button>`;
            html += `<button class="page-btn" onclick="goToPage(${{currentPage - 1}})" ${{currentPage === 1 ? 'disabled' : ''}}>&lsaquo;</button>`;
            
            // Show limited page numbers
            let startPage = Math.max(1, currentPage - 2);
            let endPage = Math.min(totalPages, currentPage + 2);
            
            for (let i = startPage; i <= endPage; i++) {{
                html += `<button class="page-btn ${{i === currentPage ? 'active' : ''}}" onclick="goToPage(${{i}})">${{i}}</button>`;
            }}
            
            html += `<button class="page-btn" onclick="goToPage(${{currentPage + 1}})" ${{currentPage === totalPages ? 'disabled' : ''}}>&rsaquo;</button>`;
            html += `<button class="page-btn" onclick="goToPage(${{totalPages}})" ${{currentPage === totalPages ? 'disabled' : ''}}>&raquo;</button>`;
            
            pageButtons.innerHTML = html;
        }}
        
        function goToPage(page) {{
            const totalPages = Math.ceil(filteredData.length / pageSize);
            if (page >= 1 && page <= totalPages) {{
                currentPage = page;
                renderTable();
            }}
        }}
        
        function changePageSize() {{
            pageSize = parseInt(document.getElementById('pageSize').value);
            currentPage = 1;
            renderTable();
        }}
        
        function resetFilters() {{
            document.getElementById('searchInput').value = '';
            document.getElementById('sourceFilter').value = '';
            document.getElementById('statusFilter').value = '';
            document.getElementById('whseDeptFilter').value = '';
            document.getElementById('areaFilter').value = '';
            document.getElementById('slotFilter').value = '';
            document.getElementById('latestStatusFilter').value = '';
            document.getElementById('latestEventNameFilter').value = '';
            document.getElementById('dateFilter').value = '';
            filteredData = [...allData];
            currentPage = 1;
            sortColumn = null;
            updateStats(allData);
            updateChart(allData);
            renderTable();
        }}
        
        function exportToCSV() {{
            const headers = ['Container ID', 'Source', 'Item', 'Item Description', 'PO', 'Whse Dept', 'Area', 'Slot', 'Legacy Cartons', 'Latest Event Time', 'Latest Event Name', 'Atlas Status', 'Atlas Location', 'Label Date'];
            const rows = filteredData.map(row => [
                row.container_id,
                row.source_type,
                row.item,
                row.item_description,
                row.po,
                row.whse_dept,
                row.area,
                row.slot,
                row.shipvoid_status,
                row.latest_event_ts,
                row.latest_event_name,
                row.latest_event_status,
                row.atlas_location,
                row.label_date
            ]);
            
            let csvContent = headers.join(',') + '\\n';
            rows.forEach(row => {{
                csvContent += row.map(cell => `"${{cell || ''}}"`).join(',') + '\\n';
            }});
            
            const blob = new Blob([csvContent], {{ type: 'text/csv;charset=utf-8;' }});
            const link = document.createElement('a');
            link.href = URL.createObjectURL(blob);
            link.download = 'shipvoid_report_' + new Date().toISOString().slice(0,10) + '.csv';
            link.click();
        }}
        
        function copyContainerIds() {{
            // Get all unique container IDs from filtered data
            const containerIds = [...new Set(filteredData.map(row => row.container_id))].filter(id => id && id !== '');
            
            if (containerIds.length === 0) {{
                showToast('No container IDs to copy!', 'warning');
                return;
            }}
            
            // Join with newlines for easy pasting
            const text = containerIds.join('\\n');
            
            // Copy to clipboard
            navigator.clipboard.writeText(text).then(() => {{
                showToast(`\u2705 Copied ${{containerIds.length.toLocaleString()}} container IDs to clipboard!`);
            }}).catch(err => {{
                // Fallback for older browsers
                const textarea = document.createElement('textarea');
                textarea.value = text;
                document.body.appendChild(textarea);
                textarea.select();
                document.execCommand('copy');
                document.body.removeChild(textarea);
                showToast(`\u2705 Copied ${{containerIds.length.toLocaleString()}} container IDs to clipboard!`);
            }});
        }}
        
        function showToast(message, type = 'success') {{
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.style.background = type === 'warning' ? '#ffc107' : '#28a745';
            toast.style.color = type === 'warning' ? '#333' : 'white';
            toast.classList.add('show');
            
            setTimeout(() => {{
                toast.classList.remove('show');
            }}, 3000);
        }}
    </script>
</body>
</html>'''
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"  Report saved to: {output_path}")


def find_newest_file(pattern: str, directory: str = ".") -> str | None:
    """Find the newest file matching the given pattern in the directory."""
    import glob
    import os
    
    files = glob.glob(os.path.join(directory, pattern))
    if not files:
        return None
    
    # Sort by modification time, newest first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def main(skip_download: bool = False):
    """
    Main entry point for the report generator.
    
    Args:
        skip_download: If True, skip downloading from SharePoint and use local files only.
    """
    print("="*60)
    print("Shipvoid Forecast Cross-Reference Report Generator")
    print("="*60)
    print()
    
    shipvoid_file = None
    legacy_file = None
    
    # Try to download from SharePoint first (unless skipped)
    if not skip_download and SHAREPOINT_AVAILABLE:
        print("Attempting to download latest files from SharePoint...")
        print()
        try:
            downloaded_shipvoid, downloaded_legacy = download_shipvoid_files(Path("."))
            if downloaded_shipvoid:
                shipvoid_file = str(downloaded_shipvoid)
            if downloaded_legacy:
                legacy_file = str(downloaded_legacy)
            print()
        except Exception as e:
            print(f"\nWarning: Could not download from SharePoint: {e}")
            print("Falling back to local files...")
            print()
    
    # Fall back to local files if download failed or was skipped
    if not shipvoid_file:
        print("Searching for local Shipvoid Forecast files...")
        # Look for Excel files (xlsm/xlsx) - need both Inhouse and Crossdock sheets
        shipvoid_file = find_newest_file("Shipvoid*.xlsm")
        if not shipvoid_file:
            shipvoid_file = find_newest_file("Shipvoid*.xlsx")
        if not shipvoid_file:
            shipvoid_file = find_newest_file("Shipvoid*.xls*")
    
    if not legacy_file:
        print("Searching for local Legacy Unbilled files...")
        legacy_file = find_newest_file("Legacy*.csv")
    
    output_file = "shipvoid_crossref_report.html"
    
    # Check files exist
    if not shipvoid_file:
        print("ERROR: No Shipvoid Forecast file found (looking for Shipvoid*.xlsm)")
        print("  Please ensure a file like 'Shipvoid Forecast YYYY-MM-DD.xlsm' exists in this directory.")
        print("  Or enable SharePoint download to fetch the latest files automatically.")
        return
    if not legacy_file:
        print("ERROR: No Legacy Unbilled Carton Report found (looking for Legacy*.csv)")
        print("  Please ensure a file like 'Legacy_Unbilled_Carton_Report-*.csv' exists in this directory.")
        print("  Or enable SharePoint download to fetch the latest files automatically.")
        return
    
    print(f"  Found Shipvoid file: {shipvoid_file}")
    print(f"  Found Legacy file: {legacy_file}")
    print()
    
    # Load data
    shipvoid_df = load_shipvoid_forecast(shipvoid_file)
    legacy_df = load_legacy_unbilled(legacy_file)
    
    # Merge data
    merged_df = merge_data(shipvoid_df, legacy_df)
    
    # Generate pivot data
    pivot_data = generate_pivot_data(merged_df)
    
    # Generate HTML report
    generate_html_report(merged_df, pivot_data, output_file)
    
    print()
    print("="*60)
    print(f"[SUCCESS] Report generated successfully: {output_file}")
    print("="*60)


if __name__ == "__main__":
    import sys
    
    # Check for --skip-download flag
    skip_download = "--skip-download" in sys.argv or "--local" in sys.argv
    
    if "--help" in sys.argv or "-h" in sys.argv:
        print("Shipvoid Forecast Cross-Reference Report Generator")
        print()
        print("Usage: python generate_report.py [OPTIONS]")
        print()
        print("Options:")
        print("  --skip-download, --local  Skip downloading from SharePoint, use local files only")
        print("  --help, -h                Show this help message")
        print()
        print("Data Source:")
        print("  Files are downloaded from the SharePoint folder:")
        print("  https://teams.wal-mart.com/.../AtlasAmbientRDCPlaybookPlanning/.../Shipvoid Forecast/6031")
        sys.exit(0)
    
    main(skip_download=skip_download)