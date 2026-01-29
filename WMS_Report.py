import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from datetime import timedelta
import requests
import io
import pyarrow.parquet as pq

st.set_page_config(page_title="WMS Performance Report (Internal Transfers)", layout="wide")

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.title("🔒 Login")
        password = st.text_input("Enter password:", type="password", key="password_input")
        
        if password:
            if password == st.secrets["password"]:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Wrong password")
        return False
    return True

if not check_password():
    st.stop()

st.title("📦 WMS Performance Report (Internal Transfers)")



@st.cache_data(ttl=60)
def get_files_list():
    """Get list of Parquet files from GitHub"""
    headers = {"Authorization": f"token {st.secrets['github_token']}"}
    url = f"https://api.github.com/repos/{st.secrets['github_repo']}/contents/{st.secrets['github_folder']}"
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to list files: {response.json().get('message', 'Unknown error')}")
    
    files = [f for f in response.json() if f['name'].endswith('.parquet')]
    if not files:
        raise Exception("No Parquet files found in the folder")
    return files

@st.cache_data(ttl=300)
def download_file_bytes(download_url):
    """Download file from GitHub and cache the raw bytes"""
    headers = {"Authorization": f"token {st.secrets['github_token']}"}
    response = requests.get(download_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")
    return response.content

@st.cache_data(ttl=300)
def get_dates_for_store(file_id):
    """Get unique dates from a file - only parses Date column (fast)"""
    file_bytes = download_file_bytes(file_id)
    df = pd.read_parquet(io.BytesIO(file_bytes), columns=['Date'])
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    return sorted(df['Date'].unique())

def get_filtered_data(file_id, selected_dates):
    """Load data filtered to selected dates only - uses cached file bytes"""
    file_bytes = download_file_bytes(file_id)
    df = pd.read_parquet(io.BytesIO(file_bytes))
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Action start'] = pd.to_datetime(df['Action start'])
    df['Action completion'] = pd.to_datetime(df['Action completion'])
    if isinstance(selected_dates, list):
        df = df[df['Date'].isin(selected_dates)]
    else:
        df = df[df['Date'] == selected_dates]
    return df

@st.cache_data(ttl=60)
def load_data(file_id):
    """Legacy function - loads all data"""
    file_bytes = download_file_bytes(file_id)
    df = pd.read_parquet(io.BytesIO(file_bytes))
    return df

# Helper functions
def calc_kg(row):
    if str(row['Unit']).upper() == 'KILOGRAM':
        return row['Quantity']
    elif pd.notna(row['Reporting Unit']) and str(row['Reporting Unit']).upper() == 'KILOGRAM':
        return row['Quantity'] * row['Relationship']
    return 0

def calc_l(row):
    if str(row['Unit']).upper() == 'LITER':
        return row['Quantity']
    elif pd.notna(row['Reporting Unit']) and str(row['Reporting Unit']).upper() == 'LITER':
        return row['Quantity'] * row['Relationship']
    return 0

def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours}:{minutes:02d}:{seconds:02d}"

def calculate_total_time_no_overlap(actions_df):
    if actions_df.empty:
        return timedelta(0)
    sorted_actions = actions_df.sort_values('Action start').reset_index(drop=True)
    total_time = timedelta(0)
    cumulative_end = sorted_actions.iloc[0]['Action start']
    for _, row in sorted_actions.iterrows():
        start = row['Action start']
        end = row['Action completion']
        effective_start = max(start, cumulative_end)
        if end > effective_start:
            total_time += end - effective_start
        cumulative_end = max(cumulative_end, end)
    return total_time

def get_avg_color(val):
    val = max(0, min(15, val))
    if val <= 7.5:
        ratio = val / 7.5
        r = int(235 + (255 - 235) * ratio)
        g = int(150 + (200 - 150) * ratio)
        b = int(150 + (100 - 150) * ratio)
    else:
        ratio = (val - 7.5) / 7.5
        r = int(255 + (144 - 255) * ratio)
        g = int(200 + (220 - 200) * ratio)
        b = int(100 + (144 - 100) * ratio)
    return f'#{r:02X}{g:02X}{b:02X}'

try:
    files = get_files_list()
    file_names = [f['name'].replace('.parquet', '') for f in files]
    
    # Mode selector
    col_mode, col_rest = st.columns([170, 1200])
    with col_mode:
        mode = st.selectbox("🎯 Mode", ["", "Daily Monitor", "Analytics Mode", "Comparison Mode"], index=0)

    if not mode:
        st.info("👆 Please select a mode to continue")
        st.stop()

    # Different UI based on mode
    if mode == "Comparison Mode":
        # Comparison Mode specific dropdowns - first row
        col1, col2, col3, col4, col5, col6, col_load, col_empty = st.columns([220, 140, 140, 160, 140, 140, 120, 680])

        with col1:
            comparison_type = st.selectbox("📊 Compare", ["", "All Properties", "Property vs Property"], index=0)

        # Initialize variables
        common_dates = []
        all_property_data = {}  # For All Properties mode
        df1 = None
        df2 = None
        property_1 = None
        property_2 = None

        if comparison_type == "Property vs Property":
            with col2:
                property_1 = st.selectbox("🏪 Property 1", [""] + file_names, index=0)

            with col3:
                # Exclude property_1 from property_2 options
                property_2_options = [""] + [f for f in file_names if f != property_1]
                property_2 = st.selectbox("🏪 Property 2", property_2_options, index=0)

            # Get dates only (fast) when both properties selected
            if property_1 and property_2:
                file_1 = next(f for f in files if f['name'].replace('.parquet', '') == property_1)
                file_2 = next(f for f in files if f['name'].replace('.parquet', '') == property_2)

                dates_1 = set(get_dates_for_store(file_1['download_url']))
                dates_2 = set(get_dates_for_store(file_2['download_url']))
                common_dates = sorted(dates_1 & dates_2)

        elif comparison_type == "All Properties":
            with col2:
                st.empty()
            with col3:
                st.empty()

            # Get dates only for all properties (fast)
            all_dates_sets = []
            for file_name in file_names:
                file_obj = next(f for f in files if f['name'].replace('.parquet', '') == file_name)
                dates = get_dates_for_store(file_obj['download_url'])
                all_dates_sets.append(set(dates))

            # Find dates common to ALL properties
            if all_dates_sets:
                common_dates = sorted(set.intersection(*all_dates_sets))

        # Initialize variables for date selection
        selected_comparison_date = None
        start_date = None
        end_date = None

        with col4:
            if common_dates:
                date_type = st.selectbox("📅 Date Type", ["", "Single Date", "Date Range"], index=0, key="comp_date_type")
            else:
                date_type = st.selectbox("📅 Date Type", [""], index=0, disabled=True, key="comp_date_type")

        with col5:
            if common_dates and date_type == "Single Date":
                selected_comparison_date = st.selectbox("📅 Date", [""] + [d.strftime("%d/%m") for d in common_dates], index=0, key="comp_date")
            elif common_dates and date_type == "Date Range":
                start_date = st.selectbox("📅 Start", [""] + [d.strftime("%d/%m") for d in common_dates], index=0, key="comp_start")
            else:
                st.selectbox("📅 Date", [""], index=0, disabled=True, key="comp_date")

        with col6:
            if common_dates and date_type == "Date Range":
                end_date = st.selectbox("📅 End", [""] + [d.strftime("%d/%m") for d in common_dates], index=0, key="comp_end")
            else:
                st.empty()

        # Mode dropdown (only for Date Range)
        if date_type == "Date Range":
            col_agg, col_agg_empty = st.columns([220, 1060])
            with col_agg:
                aggregation_mode = st.selectbox("📈 Mode", ["Total", "Average"], index=0)
        else:
            aggregation_mode = "Total"

        # Determine if Load button should be enabled
        load_enabled = False
        if comparison_type == "Property vs Property" and property_1 and property_2:
            if date_type == "Single Date" and selected_comparison_date:
                load_enabled = True
            elif date_type == "Date Range" and start_date and end_date:
                load_enabled = True
        elif comparison_type == "All Properties":
            if date_type == "Single Date" and selected_comparison_date:
                load_enabled = True
            elif date_type == "Date Range" and start_date and end_date:
                load_enabled = True

        with col_load:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            load_data_clicked = st.button("📥 Load Data", type="primary", key="comparison_load", disabled=not load_enabled)

        # Validation for Comparison Mode
        if not comparison_type:
            st.info("👆 Please select a comparison type")
            st.stop()

        if comparison_type == "Property vs Property" and (not property_1 or not property_2):
            st.info("👆 Please select both properties to compare")
            st.stop()

        if not common_dates:
            st.warning("⚠️ No common dates found between the selected properties")
            st.stop()

        if not date_type:
            st.info("👆 Please select a date type")
            st.stop()

        if date_type == "Single Date":
            if not selected_comparison_date:
                st.info("👆 Please select a date")
                st.stop()
            # Convert to date object
            comparison_dates = [next(d for d in common_dates if d.strftime("%d/%m") == selected_comparison_date)]
        else:  # Date Range
            if not start_date or not end_date:
                st.info("👆 Please select both start and end dates")
                st.stop()
            start_date_obj = next(d for d in common_dates if d.strftime("%d/%m") == start_date)
            end_date_obj = next(d for d in common_dates if d.strftime("%d/%m") == end_date)
            if start_date_obj > end_date_obj:
                st.warning("⚠️ Start date must be before or equal to end date")
                st.stop()
            comparison_dates = [d for d in common_dates if start_date_obj <= d <= end_date_obj]

        # Track loaded state in session
        if 'comp_loaded' not in st.session_state:
            st.session_state.comp_loaded = False
            st.session_state.comp_dates = None
            st.session_state.comp_type = None

        if load_data_clicked:
            st.session_state.comp_loaded = True
            st.session_state.comp_dates = comparison_dates
            st.session_state.comp_type = comparison_type

        # Reset if selection changed
        if (st.session_state.comp_dates != comparison_dates or
            st.session_state.comp_type != comparison_type):
            st.session_state.comp_loaded = False

        if not st.session_state.comp_loaded:
            st.info("👆 Click 'Load Data' to generate the report")
            st.stop()

        # Only load data once, then cache it
        if 'comp_data_cache' not in st.session_state or load_data_clicked:
            with st.spinner("Loading data for selected dates..."):
                if comparison_type == "Property vs Property":
                    file_1 = next(f for f in files if f['name'].replace('.parquet', '') == property_1)
                    file_2 = next(f for f in files if f['name'].replace('.parquet', '') == property_2)
                    df1 = get_filtered_data(file_1['download_url'], comparison_dates)
                    df2 = get_filtered_data(file_2['download_url'], comparison_dates)
                    st.session_state.comp_data_cache = {'type': 'pvp', 'df1': df1, 'df2': df2}
                elif comparison_type == "All Properties":
                    all_property_data = {}
                    for file_name in file_names:
                        file_obj = next(f for f in files if f['name'].replace('.parquet', '') == file_name)
                        all_property_data[file_name] = get_filtered_data(file_obj['download_url'], comparison_dates)
                    st.session_state.comp_data_cache = {'type': 'all', 'data': all_property_data}
        else:
            if comparison_type == "Property vs Property":
                df1 = st.session_state.comp_data_cache['df1']
                df2 = st.session_state.comp_data_cache['df2']
            elif comparison_type == "All Properties":
                all_property_data = st.session_state.comp_data_cache['data']

    elif mode == "Analytics Mode":
        # Analytics Mode - show UI but with coming soon message
        col2, col3, col4, col5, col6, col_empty = st.columns([140, 160, 140, 140, 140, 580])

        with col2:
            selected_store = st.selectbox("🏪 Store", [""] + file_names, index=0)

        unique_dates = []
        if selected_store:
            selected_file = next(f for f in files if f['name'].replace('.parquet', '') == selected_store)
            unique_dates = get_dates_for_store(selected_file['download_url'])

        with col3:
            if unique_dates:
                date_type = st.selectbox("📅 Date Type", ["", "Single Date", "Date Range"], index=0)
            else:
                date_type = st.selectbox("📅 Date Type", [""], index=0, disabled=True)

        selected_date = None
        start_date = None
        end_date = None

        with col4:
            if unique_dates and date_type == "Single Date":
                selected_date = st.selectbox("📅 Date", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            elif unique_dates and date_type == "Date Range":
                start_date = st.selectbox("📅 Start", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            else:
                st.selectbox("📅 Date", [""], index=0, disabled=True)

        with col5:
            if unique_dates and date_type == "Date Range":
                end_date = st.selectbox("📅 End", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            else:
                st.empty()

        with col6:
            st.empty()

        # Show coming soon message immediately
        st.info("🚧 **Analytics Mode coming soon!**")
        st.stop()

    else:
        # Daily Monitor Mode - original dropdowns
        col2, col3, col4, col5, col6, col_load, col_empty = st.columns([165, 110, 130, 130, 130, 120, 420])

        with col2:
            view_type = st.selectbox("👁️ View", ["", "Department View", "Worker View"], index=0)
        with col3:
            selected_store = st.selectbox("🏪 Store", [""] + file_names, index=0)

        # Cache dates in session state to avoid reloading on every interaction
        if 'cached_store' not in st.session_state:
            st.session_state.cached_store = None
            st.session_state.cached_dates = []

        # Only fetch dates if store changed
        if selected_store:
            if st.session_state.cached_store != selected_store:
                selected_file = next(f for f in files if f['name'].replace('.parquet', '') == selected_store)
                with st.spinner("Loading dates..."):
                    st.session_state.cached_dates = get_dates_for_store(selected_file['download_url'])
                st.session_state.cached_store = selected_store
            unique_dates = st.session_state.cached_dates
        else:
            unique_dates = []

        with col4:
            if unique_dates:
                date_type = st.selectbox("📅 Date Type", ["", "Single Date", "Date Range"], index=0)
            else:
                date_type = st.selectbox("📅 Date Type", [""], index=0, disabled=True)

        selected_date = None
        start_date = None
        end_date = None

        with col5:
            if unique_dates and date_type == "Single Date":
                selected_date = st.selectbox("📅 Date", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            elif unique_dates and date_type == "Date Range":
                start_date = st.selectbox("📅 Start", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            else:
                st.selectbox("📅 Date", [""], index=0, disabled=True)

        with col6:
            if unique_dates and date_type == "Date Range":
                end_date = st.selectbox("📅 End", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            else:
                st.empty()

        # Mode dropdown (only for Date Range)
        if date_type == "Date Range":
            col_agg_daily, col_agg_daily_empty = st.columns([165, 1115])
            with col_agg_daily:
                daily_aggregation_mode = st.selectbox("📈 Mode", ["Total", "Average"], index=0, key="daily_agg_mode")
        else:
            daily_aggregation_mode = "Total"

        # Determine if Load button should be enabled
        load_enabled = False
        if view_type and selected_store:
            if date_type == "Single Date" and selected_date:
                load_enabled = True
            elif date_type == "Date Range" and start_date and end_date:
                load_enabled = True

        # Load Data button
        with col_load:
            st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
            load_data_clicked = st.button("📥 Load Data", type="primary", key="daily_load", disabled=not load_enabled)

        # Build selected_dates list
        selected_dates = None
        if date_type == "Single Date" and selected_date:
            selected_dates = [next(d for d in unique_dates if d.strftime("%d/%m") == selected_date)]
        elif date_type == "Date Range" and start_date and end_date:
            start_date_obj = next(d for d in unique_dates if d.strftime("%d/%m") == start_date)
            end_date_obj = next(d for d in unique_dates if d.strftime("%d/%m") == end_date)
            if start_date_obj > end_date_obj:
                st.warning("⚠️ Start date must be before or equal to end date")
                st.stop()
            selected_dates = [d for d in unique_dates if start_date_obj <= d <= end_date_obj]

        # Track loaded state in session
        if 'daily_loaded' not in st.session_state:
            st.session_state.daily_loaded = False
            st.session_state.daily_store = None
            st.session_state.daily_dates = None

        if load_data_clicked:
            st.session_state.daily_loaded = True
            st.session_state.daily_store = selected_store
            st.session_state.daily_dates = selected_dates

        # Reset if store or dates changed
        if (st.session_state.daily_store != selected_store or 
            st.session_state.get('daily_dates') != selected_dates):
            st.session_state.daily_loaded = False
            
        if not view_type or not selected_store or not date_type:
            st.info("👆 Please make all selections to continue")
            st.stop()

        if date_type == "Single Date" and not selected_date:
            st.info("👆 Please select a date")
            st.stop()

        if date_type == "Date Range" and (not start_date or not end_date):
            st.info("👆 Please select both start and end dates")
            st.stop()

        if not st.session_state.daily_loaded:
            st.info("👆 Click 'Load Data' to generate the report")
            st.stop()

        # Only load data once, then cache it
        if 'daily_day_df' not in st.session_state or load_data_clicked:
            selected_file = next(f for f in files if f['name'].replace('.parquet', '') == selected_store)
            
            with st.spinner("Loading data for selected date(s)..."):
                day_df = get_filtered_data(selected_file['download_url'], selected_dates)

            day_df['Kg'] = day_df.apply(calc_kg, axis=1)
            day_df['Liters'] = day_df.apply(calc_l, axis=1)
            st.session_state.daily_day_df = day_df
            st.session_state.daily_selected_dates = selected_dates
        else:
            day_df = st.session_state.daily_day_df
            selected_dates = st.session_state.daily_selected_dates

    # ============== DAILY MONITOR MODE ==============
    if mode == "Daily Monitor":
        
        # ============== DEPARTMENT VIEW ==============
        if view_type == "Department View":

            # Check if average mode applies
            num_days = len(selected_dates)
            is_average_mode = daily_aggregation_mode == "Average" and num_days > 1

            # Initialize sort state
            if 'dept_sort_col' not in st.session_state:
                st.session_state.dept_sort_col = 'Total Weight'
                st.session_state.dept_sort_asc = False

            unique_actions_dept = day_df.groupby(['Cost Center', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            unique_actions_dept['picking_time'] = unique_actions_dept['Action completion'] - unique_actions_dept['Action start']

            dept_times = unique_actions_dept.groupby('Cost Center')['picking_time'].sum().reset_index()

            dept_stats = day_df.groupby('Cost Center').agg({
                'Document': 'nunique',
                'Code': 'count',
                'Kg': 'sum',
                'Liters': 'sum'
            }).reset_index()
            dept_stats.columns = ['Cost Center', '# of Orders', 'Item Requests', 'Kilograms', 'Liters']
            dept_stats['Total Weight'] = dept_stats['Kilograms'] + dept_stats['Liters']

            dept_report = dept_stats.merge(dept_times, on='Cost Center')

            # Apply average mode if selected
            if is_average_mode:
                dept_report['display_orders'] = dept_report['# of Orders'] / num_days
                dept_report['display_requests'] = dept_report['Item Requests'] / num_days
                dept_report['display_kg'] = dept_report['Kilograms'] / num_days
                dept_report['display_liters'] = dept_report['Liters'] / num_days
                dept_report['display_weight'] = dept_report['Total Weight'] / num_days
                dept_report['display_picking_time'] = dept_report['picking_time'] / num_days
            else:
                dept_report['display_orders'] = dept_report['# of Orders']
                dept_report['display_requests'] = dept_report['Item Requests']
                dept_report['display_kg'] = dept_report['Kilograms']
                dept_report['display_liters'] = dept_report['Liters']
                dept_report['display_weight'] = dept_report['Total Weight']
                dept_report['display_picking_time'] = dept_report['picking_time']

            dept_report['Total Picking Time'] = dept_report['display_picking_time'].apply(format_timedelta)

            max_orders = dept_report['display_orders'].max()
            max_requests = dept_report['display_requests'].max()
            max_kg = dept_report['display_kg'].max()
            max_l = dept_report['display_liters'].max()
            max_weight = dept_report['display_weight'].max()
            max_time = dept_report['display_picking_time'].max().total_seconds()

            # Dynamic headers based on mode
            if is_average_mode:
                orders_header = 'Avg Orders'
                requests_header = 'Avg Requests'
                kg_header = 'Avg Kg'
                liters_header = 'Avg Liters'
                weight_header = 'Avg Weight'
                time_header = 'Avg Picking Time'
            else:
                orders_header = '# of Orders'
                requests_header = 'Item Requests'
                kg_header = 'Kilograms'
                liters_header = 'Liters'
                weight_header = 'Total Weight'
                time_header = 'Total Picking Time'

            # Sort controls in one row
            sort_options = [orders_header, requests_header, kg_header, liters_header, weight_header, time_header]

            # Reset sort column if it's not in current options (mode changed)
            if st.session_state.dept_sort_col not in sort_options:
                st.session_state.dept_sort_col = weight_header
            col_sort1, col_sort2, col_sort3 = st.columns([2, 2, 6])
            with col_sort1:
                sort_col_display = st.selectbox(
                    "Sort by",
                    sort_options,
                    index=sort_options.index(st.session_state.dept_sort_col),
                    key="sort_select"
                )
            with col_sort2:
                sort_order = st.selectbox(
                    "Order",
                    ["Largest ↓", "Smallest ↑"],
                    index=0 if not st.session_state.dept_sort_asc else 1,
                    key="sort_order"
                )
            
            if sort_col_display != st.session_state.dept_sort_col or (sort_order == "Smallest ↑") != st.session_state.dept_sort_asc:
                st.session_state.dept_sort_col = sort_col_display
                st.session_state.dept_sort_asc = (sort_order == "Smallest ↑")
                st.rerun()

            # Sort by selected column
            sort_col = st.session_state.dept_sort_col
            sort_asc = st.session_state.dept_sort_asc

            sort_col_map = {
                orders_header: 'display_orders',
                requests_header: 'display_requests',
                kg_header: 'display_kg',
                liters_header: 'display_liters',
                weight_header: 'display_weight',
                time_header: 'display_picking_time'
            }
            sort_by_col = sort_col_map.get(sort_col, 'display_weight')
            if sort_by_col == 'display_picking_time':
                dept_report = dept_report.sort_values(sort_by_col, ascending=sort_asc, key=lambda x: x.apply(lambda td: td.total_seconds())).reset_index(drop=True)
            else:
                dept_report = dept_report.sort_values(sort_by_col, ascending=sort_asc).reset_index(drop=True)

            html = '''
            <style>
                .wms-table {
                    border-collapse: collapse;
                    width: auto;
                    font-family: Arial, sans-serif;
                    font-size: 14px;
                }
                .wms-table th {
                    background-color: #4472C4;
                    color: white;
                    padding: 10px;
                    text-align: center;
                    border: 1px solid #2F5496;
                }
                .wms-table td {
                    padding: 8px;
                    border: 1px solid #B4C6E7;
                    text-align: center;
                    color: black;
                }
                .wms-table tr:nth-child(odd) {
                    background-color: #D6DCE4;
                }
                .wms-table tr:nth-child(even) {
                    background-color: #EDEDED;
                }
                .dept-name {
                    font-weight: bold;
                    text-align: left !important;
                    color: black;
                }
                .progress-cell {
                    position: relative;
                    padding: 0 !important;
                }
                .progress-bar {
                    height: 100%;
                    position: absolute;
                    left: 0;
                    top: 0;
                }
                .progress-text {
                    position: relative;
                    z-index: 1;
                    padding: 8px;
                    color: black;
                }
            </style>
            '''
            
            headers = [
                ('Cost Center', '280px'),
                (orders_header, '110px'),
                (requests_header, '180px'),
                (kg_header, '120px'),
                (liters_header, '120px'),
                (weight_header, '120px'),
                (time_header, '150px')
            ]

            html += '<table class="wms-table">'
            html += '<tr>'
            for h, w in headers:
                html += f'<th style="width: {w};">{h}</th>'
            html += '</tr>'

            for _, row in dept_report.iterrows():
                html += '<tr>'

                html += f'<td class="dept-name">{row["Cost Center"]}</td>'

                # Format values based on mode
                orders_str = f"{row['display_orders']:.1f}" if is_average_mode else f"{int(row['display_orders'])}"
                requests_str = f"{row['display_requests']:.1f}" if is_average_mode else f"{int(row['display_requests'])}"

                pct = (row['display_orders'] / max_orders * 100) if max_orders > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{orders_str}</div>
                </td>'''

                pct = (row['display_requests'] / max_requests * 100) if max_requests > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{requests_str}</div>
                </td>'''

                pct = (row['display_kg'] / max_kg * 100) if max_kg > 0 else 0
                kg_formatted = f"{row['display_kg']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #FFC000;"></div>
                    <div class="progress-text">{kg_formatted}</div>
                </td>'''
                
                pct = (row['display_liters'] / max_l * 100) if max_l > 0 else 0
                liters_formatted = f"{row['display_liters']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #70AD47;"></div>
                    <div class="progress-text">{liters_formatted}</div>
                </td>'''
                
                pct = (row['display_weight'] / max_weight * 100) if max_weight > 0 else 0
                weight_formatted = f"{row['display_weight']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #9B59B6;"></div>
                    <div class="progress-text">{weight_formatted}</div>
                </td>'''

                pct = (row['display_picking_time'].total_seconds() / max_time * 100) if max_time > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #C65B5B;"></div>
                    <div class="progress-text">{row["Total Picking Time"]}</div>
                </td>'''

                html += '</tr>'
            
            html += '</table>'
            
            st.markdown(html, unsafe_allow_html=True)
            
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
        
        # ============== WORKER VIEW ==============

        elif view_type == "Worker View":

            # Check if average mode applies
            num_days = len(selected_dates)
            is_average_mode = daily_aggregation_mode == "Average" and num_days > 1

            # Initialize sort state for worker view
            if 'worker_sort_col' not in st.session_state:
                st.session_state.worker_sort_col = 'Total Weight'
                st.session_state.worker_sort_asc = False

            unique_actions = day_df.groupby(['Name', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            unique_actions['picking_time'] = unique_actions['Action completion'] - unique_actions['Action start']

            picker_times = unique_actions.groupby('Name')['picking_time'].sum().reset_index()
            total_picking_time_no_overlap = calculate_total_time_no_overlap(unique_actions)

            picker_stats = day_df.groupby('Name').agg({
                'Code': 'count',
                'Kg': 'sum',
                'Liters': 'sum'
            }).reset_index()
            picker_stats.columns = ['Name', 'Requests fulfilled', 'Kilograms', 'Liters']
            picker_stats['Name'] = picker_stats['Name'].str.title()
            picker_times['Name'] = picker_times['Name'].str.title()

            report = picker_stats.merge(picker_times, on='Name')

            report['picking_minutes'] = report['picking_time'].dt.total_seconds() / 60
            report['Requests per minute'] = report['Requests fulfilled'] / report['picking_minutes']
            report['Total Weight'] = report['Kilograms'] + report['Liters']
            report['Weight per min'] = report['Total Weight'] / report['picking_minutes']

            # Apply average mode if selected
            if is_average_mode:
                report['display_picking_time'] = report['picking_time'] / num_days
                report['display_requests'] = report['Requests fulfilled'] / num_days
                report['display_kg'] = report['Kilograms'] / num_days
                report['display_liters'] = report['Liters'] / num_days
                report['display_weight'] = report['Total Weight'] / num_days
            else:
                report['display_picking_time'] = report['picking_time']
                report['display_requests'] = report['Requests fulfilled']
                report['display_kg'] = report['Kilograms']
                report['display_liters'] = report['Liters']
                report['display_weight'] = report['Total Weight']

            report['Picking Time Display'] = report['display_picking_time'].apply(format_timedelta)

            # Dynamic headers based on mode
            if is_average_mode:
                picking_time_header = 'Avg Picking Time'
                requests_header = 'Avg Requests'
                kg_header = 'Avg Kg'
                liters_header = 'Avg Liters'
                weight_header = 'Avg Weight'
            else:
                picking_time_header = 'Picking Time'
                requests_header = 'Requests fulfilled'
                kg_header = 'Kilograms'
                liters_header = 'Liters'
                weight_header = 'Total Weight'

            max_time = report['display_picking_time'].max().total_seconds()
            max_requests = report['display_requests'].max()
            max_kg = report['display_kg'].max()
            max_l = report['display_liters'].max()
            max_weight = report['display_weight'].max()

            # Sort controls in one row
            sort_options = [picking_time_header, requests_header, 'Requests per minute', kg_header, liters_header, weight_header, 'Weight per min']

            # Reset sort column if it's not in current options (mode changed)
            if st.session_state.worker_sort_col not in sort_options:
                st.session_state.worker_sort_col = weight_header
            col_sort1, col_sort2, col_sort3 = st.columns([2, 2, 6])
            with col_sort1:
                sort_col_display = st.selectbox(
                    "Sort by",
                    sort_options,
                    index=sort_options.index(st.session_state.worker_sort_col),
                    key="worker_sort_select"
                )
            with col_sort2:
                sort_order = st.selectbox(
                    "Order",
                    ["Largest ↓", "Smallest ↑"],
                    index=0 if not st.session_state.worker_sort_asc else 1,
                    key="worker_sort_order"
                )

            if sort_col_display != st.session_state.worker_sort_col or (sort_order == "Smallest ↑") != st.session_state.worker_sort_asc:
                st.session_state.worker_sort_col = sort_col_display
                st.session_state.worker_sort_asc = (sort_order == "Smallest ↑")
                st.rerun()

            # Sort by selected column
            sort_col = st.session_state.worker_sort_col
            sort_asc = st.session_state.worker_sort_asc

            sort_col_map = {
                picking_time_header: 'display_picking_time',
                requests_header: 'display_requests',
                'Requests per minute': 'Requests per minute',
                kg_header: 'display_kg',
                liters_header: 'display_liters',
                weight_header: 'display_weight',
                'Weight per min': 'Weight per min'
            }
            sort_by_col = sort_col_map.get(sort_col, 'Weight per min')
            if sort_by_col == 'display_picking_time':
                report = report.sort_values(sort_by_col, ascending=sort_asc, key=lambda x: x.apply(lambda td: td.total_seconds())).reset_index(drop=True)
            else:
                report = report.sort_values(sort_by_col, ascending=sort_asc).reset_index(drop=True)
            
            html = '''
            <style>
                .wms-table {
                    border-collapse: collapse;
                    width: auto;
                    font-family: Arial, sans-serif;
                    font-size: 14px;
                }
                .wms-table th {
                    background-color: #4472C4;
                    color: white;
                    padding: 10px;
                    text-align: center;
                    border: 1px solid #2F5496;
                }
                .wms-table td {
                    padding: 8px;
                    border: 1px solid #B4C6E7;
                    text-align: center;
                    color: black;
                }
                .wms-table tr:nth-child(odd) {
                    background-color: #D6DCE4;
                }
                .wms-table tr:nth-child(even) {
                    background-color: #EDEDED;
                }
                .picker-name {
                    font-weight: bold;
                    text-align: left !important;
                    color: black;
                }
                .progress-cell {
                    position: relative;
                    padding: 0 !important;
                }
                .progress-bar {
                    height: 100%;
                    position: absolute;
                    left: 0;
                    top: 0;
                }
                .progress-text {
                    position: relative;
                    z-index: 1;
                    padding: 8px;
                    color: black;
                }
                .stats-table {
                    border-collapse: collapse;
                    margin-top: 30px;
                    font-family: Arial, sans-serif;
                }
                .stats-table th {
                    background-color: #4472C4;
                    color: white;
                    padding: 10px;
                    border: 1px solid #2F5496;
                }
                .stats-table td {
                    padding: 10px;
                    border: 1px solid #B4C6E7;
                    background-color: #D6DCE4;
                    text-align: center;
                    color: black;
                }
                .stats-title {
                    font-size: 18px;
                    text-decoration: underline;
                    margin-bottom: 10px;
                    color: black;
                }
            </style>
            '''
            
            headers = [
                ('Picker', '180px'),
                (picking_time_header, '130px'),
                (requests_header, '120px'),
                ('Requests per minute', '150px'),
                (kg_header, '100px'),
                (liters_header, '100px'),
                (weight_header, '110px'),
                ('Weight per min', '110px')
            ]

            html += '<table class="wms-table">'
            html += '<tr>'
            for h, w in headers:
                html += f'<th style="width: {w};">{h}</th>'
            html += '</tr>'

            for _, row in report.iterrows():
                html += '<tr>'
                html += f'<td class="picker-name">{row["Name"]}</td>'

                # Format values based on mode
                requests_str = f"{row['display_requests']:.1f}" if is_average_mode else f"{int(row['display_requests'])}"

                pct = (row['display_picking_time'].total_seconds() / max_time * 100) if max_time > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #C65B5B;"></div>
                    <div class="progress-text">{row["Picking Time Display"]}</div>
                </td>'''

                pct = (row['display_requests'] / max_requests * 100) if max_requests > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{requests_str}</div>
                </td>'''

                html += f'<td>{row["Requests per minute"]:.2f}</td>'

                pct = (row['display_kg'] / max_kg * 100) if max_kg > 0 else 0
                kg_formatted = f"{row['display_kg']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #FFC000;"></div>
                    <div class="progress-text">{kg_formatted}</div>
                </td>'''
                
                pct = (row['display_liters'] / max_l * 100) if max_l > 0 else 0
                liters_formatted = f"{row['display_liters']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #70AD47;"></div>
                    <div class="progress-text">{liters_formatted}</div>
                </td>'''
                
                pct = (row['display_weight'] / max_weight * 100) if max_weight > 0 else 0
                weight_formatted = f"{row['display_weight']:,.2f}"
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #9B59B6;"></div>
                    <div class="progress-text">{weight_formatted}</div>
                </td>'''

                color = get_avg_color(row['Weight per min'])
                html += f'<td style="background-color: {color}; font-weight: bold;">{row["Weight per min"]:,.2f}</td>'

                html += '</tr>'

            html += '</table>'

            # Statistics section
            total_picking_time = total_picking_time_no_overlap
            total_requests_sum = report['Requests fulfilled'].sum()
            total_minutes = total_picking_time.total_seconds() / 60
            avg_requests_min = total_requests_sum / total_minutes if total_minutes > 0 else 0
            total_kg = report['Kilograms'].sum()
            total_l = report['Liters'].sum()
            total_weight_sum = total_kg + total_l
            weight_per_min = total_weight_sum / total_minutes if total_minutes > 0 else 0

            # For date ranges with average mode, show average values
            if is_average_mode:
                display_picking_time_total = total_picking_time / num_days
                total_picking_time_str = format_timedelta(display_picking_time_total)
                display_requests_total = total_requests_sum / num_days
                display_kg_total = total_kg / num_days
                display_l_total = total_l / num_days
                display_weight_total = total_weight_sum / num_days
            else:
                total_picking_time_str = format_timedelta(total_picking_time)
                display_requests_total = total_requests_sum
                display_kg_total = total_kg
                display_l_total = total_l
                display_weight_total = total_weight_sum

            # Calculate picking finish time (always average for date ranges)
            if num_days > 1:
                daily_finish = day_df.groupby('Date')['Action completion'].max()
                finish_times = daily_finish.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
                avg_seconds = finish_times.mean()
                avg_hours = int(avg_seconds // 3600)
                avg_minutes_finish = int((avg_seconds % 3600) // 60)
                avg_secs = int(avg_seconds % 60)
                if avg_hours < 12:
                    picking_finish_str = f"{avg_hours:02d}:{avg_minutes_finish:02d}:{avg_secs:02d} AM"
                else:
                    h = avg_hours if avg_hours <= 12 else avg_hours - 12
                    picking_finish_str = f"{h:02d}:{avg_minutes_finish:02d}:{avg_secs:02d} PM"
                picking_finish_summary_header = 'Avg Picking Finish'
            else:
                picking_finish = day_df['Action completion'].max()
                picking_finish_str = picking_finish.strftime("%I:%M:%S %p") if pd.notna(picking_finish) else ""
                picking_finish_summary_header = 'Picking Finish'

            # Dynamic summary headers
            if is_average_mode:
                picking_time_summary_header = 'Avg Picking Time'
                requests_summary_header = 'Avg Requests'
                kg_summary_header = 'Avg Kg'
                l_summary_header = 'Avg L'
                weight_summary_header = 'Avg Weight'
            else:
                picking_time_summary_header = 'Total Picking Time'
                requests_summary_header = 'Total Requests'
                kg_summary_header = 'Total Kg'
                l_summary_header = 'Total L'
                weight_summary_header = 'Total Weight'

            # Format request display
            requests_total_str = f"{display_requests_total:.1f}" if is_average_mode else f"{int(display_requests_total)}"

            html += f'''
            <table class="stats-table" style="margin-top: 15px;">
                <tr>
                    <th>{picking_time_summary_header}</th>
                    <th>{picking_finish_summary_header}</th>
                    <th>{requests_summary_header}</th>
                    <th>Avg Requests/min</th>
                    <th>{kg_summary_header}</th>
                    <th>{l_summary_header}</th>
                    <th>{weight_summary_header}</th>
                    <th>Weight/min</th>
                </tr>
                <tr>
                    <td>{total_picking_time_str}</td>
                    <td>{picking_finish_str}</td>
                    <td>{requests_total_str}</td>
                    <td>{avg_requests_min:.2f}</td>
                    <td>{display_kg_total:,.2f}</td>
                    <td>{display_l_total:,.2f}</td>
                    <td>{display_weight_total:,.2f}</td>
                    <td>{weight_per_min:.2f}</td>
                </tr>
            </table>
            '''

            
            st.markdown(html, unsafe_allow_html=True)
            
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
    
    # ============== COMPARISON MODE ==============
    elif mode == "Comparison Mode":

        if comparison_type == "Property vs Property":
            # Data is already filtered to selected dates
            # Calculate Kg and Liters
            df1['Kg'] = df1.apply(calc_kg, axis=1)
            df1['Liters'] = df1.apply(calc_l, axis=1)
            df2['Kg'] = df2.apply(calc_kg, axis=1)
            df2['Liters'] = df2.apply(calc_l, axis=1)

            num_days = len(comparison_dates)
            is_average_mode = aggregation_mode == "Average" and num_days > 1

            # Calculate metrics for Property 1
            unique_actions_1 = df1.groupby(['Name', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            total_picking_time_1 = calculate_total_time_no_overlap(unique_actions_1)

            # For date ranges, calculate average picking finish time per day
            if len(comparison_dates) > 1:
                daily_finish_1 = df1.groupby('Date')['Action completion'].max()
                # Extract time of day and average it
                finish_times_1 = daily_finish_1.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
                avg_seconds_1 = finish_times_1.mean()
                avg_hours_1 = int(avg_seconds_1 // 3600)
                avg_minutes_1 = int((avg_seconds_1 % 3600) // 60)
                avg_secs_1 = int(avg_seconds_1 % 60)
                picking_finish_1 = pd.Timestamp(f"2000-01-01 {avg_hours_1:02d}:{avg_minutes_1:02d}:{avg_secs_1:02d}")
            else:
                picking_finish_1 = df1['Action completion'].max()

            # Unique documents (orders) for the property on selected date
            total_orders_1 = df1['Document'].nunique()
            total_requests_1 = len(df1)
            total_kg_1 = df1['Kg'].sum()
            total_liters_1 = df1['Liters'].sum()
            total_weight_1 = total_kg_1 + total_liters_1

            # Calculate metrics for Property 2
            unique_actions_2 = df2.groupby(['Name', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            total_picking_time_2 = calculate_total_time_no_overlap(unique_actions_2)

            # For date ranges, calculate average picking finish time per day
            if len(comparison_dates) > 1:
                daily_finish_2 = df2.groupby('Date')['Action completion'].max()
                finish_times_2 = daily_finish_2.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
                avg_seconds_2 = finish_times_2.mean()
                avg_hours_2 = int(avg_seconds_2 // 3600)
                avg_minutes_2 = int((avg_seconds_2 % 3600) // 60)
                avg_secs_2 = int(avg_seconds_2 % 60)
                picking_finish_2 = pd.Timestamp(f"2000-01-01 {avg_hours_2:02d}:{avg_minutes_2:02d}:{avg_secs_2:02d}")
            else:
                picking_finish_2 = df2['Action completion'].max()

            # Unique documents (orders) for the property on selected date
            total_orders_2 = df2['Document'].nunique()
            total_requests_2 = len(df2)
            total_kg_2 = df2['Kg'].sum()
            total_liters_2 = df2['Liters'].sum()
            total_weight_2 = total_kg_2 + total_liters_2

            # Apply average mode if selected
            if is_average_mode:
                display_picking_time_1 = total_picking_time_1 / num_days
                display_picking_time_2 = total_picking_time_2 / num_days
                display_orders_1 = total_orders_1 / num_days
                display_orders_2 = total_orders_2 / num_days
                display_requests_1 = total_requests_1 / num_days
                display_requests_2 = total_requests_2 / num_days
                display_weight_1 = total_weight_1 / num_days
                display_weight_2 = total_weight_2 / num_days
            else:
                display_picking_time_1 = total_picking_time_1
                display_picking_time_2 = total_picking_time_2
                display_orders_1 = total_orders_1
                display_orders_2 = total_orders_2
                display_requests_1 = total_requests_1
                display_requests_2 = total_requests_2
                display_weight_1 = total_weight_1
                display_weight_2 = total_weight_2

            # Format values
            picking_time_str_1 = format_timedelta(display_picking_time_1)
            picking_time_str_2 = format_timedelta(display_picking_time_2)
            picking_finish_str_1 = picking_finish_1.strftime("%I:%M:%S %p") if pd.notna(picking_finish_1) else "N/A"
            picking_finish_str_2 = picking_finish_2.strftime("%I:%M:%S %p") if pd.notna(picking_finish_2) else "N/A"

            # Format orders and requests based on mode
            if is_average_mode:
                orders_str_1 = f"{display_orders_1:,.1f}"
                orders_str_2 = f"{display_orders_2:,.1f}"
                requests_str_1 = f"{display_requests_1:,.1f}"
                requests_str_2 = f"{display_requests_2:,.1f}"
            else:
                orders_str_1 = f"{int(display_orders_1):,}"
                orders_str_2 = f"{int(display_orders_2):,}"
                requests_str_1 = f"{int(display_requests_1):,}"
                requests_str_2 = f"{int(display_requests_2):,}"

            # Date range display and column headers
            if len(comparison_dates) == 1:
                date_display = comparison_dates[0].strftime("%d/%m/%Y")
                picking_finish_header = "Picking Finish"
                picking_time_header = "Total Picking Time"
                orders_header = "# of Orders"
                requests_header = "Item Requests"
                weight_header = "Total Weight"
            else:
                date_display = f"{comparison_dates[0].strftime('%d/%m/%Y')} - {comparison_dates[-1].strftime('%d/%m/%Y')}"
                picking_finish_header = "Avg Picking Finish"
                if is_average_mode:
                    picking_time_header = "Avg Picking Time"
                    orders_header = "Avg Orders"
                    requests_header = "Avg Requests"
                    weight_header = "Avg Weight"
                else:
                    picking_time_header = "Total Picking Time"
                    orders_header = "# of Orders"
                    requests_header = "Item Requests"
                    weight_header = "Total Weight"

            # Calculate percentages for bar charts (relative to max of the two)
            max_picking_time = max(display_picking_time_1.total_seconds(), display_picking_time_2.total_seconds(), 1)
            max_orders = max(display_orders_1, display_orders_2, 1)
            max_requests = max(display_requests_1, display_requests_2, 1)
            max_weight = max(display_weight_1, display_weight_2, 1)

            pct_time_1 = (display_picking_time_1.total_seconds() / max_picking_time) * 100
            pct_time_2 = (display_picking_time_2.total_seconds() / max_picking_time) * 100
            pct_orders_1 = (display_orders_1 / max_orders) * 100
            pct_orders_2 = (display_orders_2 / max_orders) * 100
            pct_requests_1 = (display_requests_1 / max_requests) * 100
            pct_requests_2 = (display_requests_2 / max_requests) * 100
            pct_weight_1 = (display_weight_1 / max_weight) * 100
            pct_weight_2 = (display_weight_2 / max_weight) * 100

            # Build comparison HTML table
            html = f'''
            <style>
                .comparison-title {{
                    font-size: 18px;
                    font-weight: bold;
                    margin-bottom: 15px;
                    color: #2F5496;
                }}
                .comparison-table {{
                    border-collapse: collapse;
                    width: auto;
                    font-family: Arial, sans-serif;
                    font-size: 14px;
                }}
                .comparison-table th {{
                    background-color: #4472C4;
                    color: white;
                    padding: 12px 20px;
                    text-align: center;
                    border: 1px solid #2F5496;
                }}
                .comparison-table td {{
                    padding: 0;
                    border: 1px solid #B4C6E7;
                    text-align: center;
                    color: black;
                    height: 40px;
                }}
                .comparison-table tr:nth-child(odd) td {{
                    background-color: #EDEDED;
                }}
                .comparison-table tr:nth-child(even) td {{
                    background-color: #D6DCE4;
                }}
                .property-name {{
                    font-weight: bold;
                    text-align: left !important;
                    padding: 10px 15px !important;
                }}
                .progress-cell {{
                    position: relative;
                    padding: 0 !important;
                    width: 140px;
                }}
                .progress-bar {{
                    height: 100%;
                    position: absolute;
                    left: 0;
                    top: 0;
                }}
                .progress-text {{
                    position: relative;
                    z-index: 1;
                    padding: 10px;
                    color: black;
                }}
            </style>

            <div class="comparison-title">Property vs Property Comparison - {date_display}</div>

            <table class="comparison-table">
                <tr>
                    <th style="width: 140px;">Property</th>
                    <th style="width: 170px;">{picking_time_header}</th>
                    <th style="width: 140px;">{picking_finish_header}</th>
                    <th style="width: 140px;">{orders_header}</th>
                    <th style="width: 140px;">{requests_header}</th>
                    <th style="width: 140px;">{weight_header}</th>
                </tr>
                <tr>
                    <td class="property-name">{property_1}</td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_time_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{picking_time_str_1}</div>
                    </td>
                    <td style="padding: 10px;">{picking_finish_str_1}</td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_orders_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{orders_str_1}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_requests_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{requests_str_1}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_weight_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{display_weight_1:,.2f}</div>
                    </td>
                </tr>
                <tr>
                    <td class="property-name">{property_2}</td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_time_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{picking_time_str_2}</div>
                    </td>
                    <td style="padding: 10px;">{picking_finish_str_2}</td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_orders_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{orders_str_2}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_requests_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{requests_str_2}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_weight_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{display_weight_2:,.2f}</div>
                    </td>
                </tr>
            </table>
            '''

            st.markdown(html, unsafe_allow_html=True)

            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()

        elif comparison_type == "All Properties":
            num_days = len(comparison_dates)
            is_average_mode = aggregation_mode == "Average" and num_days > 1

            # Date range display and column headers
            if len(comparison_dates) == 1:
                date_display = comparison_dates[0].strftime("%d/%m/%Y")
                picking_finish_header = "Picking Finish"
                picking_time_header = "Total Picking Time"
                orders_header = "# of Orders"
                requests_header = "Item Requests"
                weight_header = "Total Weight"
            else:
                date_display = f"{comparison_dates[0].strftime('%d/%m/%Y')} - {comparison_dates[-1].strftime('%d/%m/%Y')}"
                picking_finish_header = "Avg Picking Finish"
                if is_average_mode:
                    picking_time_header = "Avg Picking Time"
                    orders_header = "Avg Orders"
                    requests_header = "Avg Requests"
                    weight_header = "Avg Weight"
                else:
                    picking_time_header = "Total Picking Time"
                    orders_header = "# of Orders"
                    requests_header = "Item Requests"
                    weight_header = "Total Weight"

            # Calculate metrics for all properties (data is already filtered)
            property_metrics = []
            for prop_name, df in all_property_data.items():
                df['Kg'] = df.apply(calc_kg, axis=1)
                df['Liters'] = df.apply(calc_l, axis=1)

                unique_actions = df.groupby(['Name', 'Action Code']).agg({
                    'Action start': 'first',
                    'Action completion': 'first'
                }).reset_index()

                # For date ranges, calculate average picking finish time per day
                if len(comparison_dates) > 1:
                    daily_finish = df.groupby('Date')['Action completion'].max()
                    finish_times = daily_finish.apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
                    avg_seconds = finish_times.mean()
                    avg_hours = int(avg_seconds // 3600)
                    avg_minutes = int((avg_seconds % 3600) // 60)
                    avg_secs = int(avg_seconds % 60)
                    picking_finish = pd.Timestamp(f"2000-01-01 {avg_hours:02d}:{avg_minutes:02d}:{avg_secs:02d}")
                else:
                    picking_finish = df['Action completion'].max()

                total_picking_time = calculate_total_time_no_overlap(unique_actions)
                total_orders = df['Document'].nunique()
                total_requests = len(df)
                total_weight = df['Kg'].sum() + df['Liters'].sum()

                # Apply average mode if selected
                if is_average_mode:
                    display_picking_time = total_picking_time / num_days
                    display_orders = total_orders / num_days
                    display_requests = total_requests / num_days
                    display_weight = total_weight / num_days
                else:
                    display_picking_time = total_picking_time
                    display_orders = total_orders
                    display_requests = total_requests
                    display_weight = total_weight

                property_metrics.append({
                    'name': prop_name,
                    'picking_time': display_picking_time,
                    'picking_finish': picking_finish,
                    'orders': display_orders,
                    'requests': display_requests,
                    'weight': display_weight
                })

            # Calculate max values for bar percentages
            max_time = max((m['picking_time'].total_seconds() for m in property_metrics), default=1) or 1
            max_orders = max((m['orders'] for m in property_metrics), default=1) or 1
            max_requests = max((m['requests'] for m in property_metrics), default=1) or 1
            max_weight = max((m['weight'] for m in property_metrics), default=1) or 1

            # Initialize sort state for all properties comparison
            if 'allprop_sort_col' not in st.session_state:
                st.session_state.allprop_sort_col = weight_header
                st.session_state.allprop_sort_asc = False

            # Sort controls
            sort_options = [picking_time_header, picking_finish_header, orders_header, requests_header, weight_header]

            # Reset sort column if it's not in current options (mode changed)
            if st.session_state.allprop_sort_col not in sort_options:
                st.session_state.allprop_sort_col = weight_header

            col_sort1, col_sort2, col_sort3 = st.columns([2, 2, 6])
            with col_sort1:
                sort_col_display = st.selectbox(
                    "Sort by",
                    sort_options,
                    index=sort_options.index(st.session_state.allprop_sort_col),
                    key="allprop_sort_select"
                )
            with col_sort2:
                sort_order = st.selectbox(
                    "Order",
                    ["Largest ↓", "Smallest ↑"],
                    index=0 if not st.session_state.allprop_sort_asc else 1,
                    key="allprop_sort_order"
                )

            if sort_col_display != st.session_state.allprop_sort_col or (sort_order == "Smallest ↑") != st.session_state.allprop_sort_asc:
                st.session_state.allprop_sort_col = sort_col_display
                st.session_state.allprop_sort_asc = (sort_order == "Smallest ↑")
                st.rerun()

            # Sort property_metrics
            sort_key_map = {
                picking_time_header: lambda m: m['picking_time'].total_seconds(),
                picking_finish_header: lambda m: m['picking_finish'].hour * 3600 + m['picking_finish'].minute * 60 + m['picking_finish'].second if pd.notna(m['picking_finish']) else 0,
                orders_header: lambda m: m['orders'],
                requests_header: lambda m: m['requests'],
                weight_header: lambda m: m['weight']
            }
            sort_key = sort_key_map.get(st.session_state.allprop_sort_col, lambda m: m['weight'])
            property_metrics = sorted(property_metrics, key=sort_key, reverse=not st.session_state.allprop_sort_asc)

            # Build rows
            rows = []
            for m in property_metrics:
                pct_time = (m['picking_time'].total_seconds() / max_time) * 100
                pct_orders = (m['orders'] / max_orders) * 100
                pct_requests = (m['requests'] / max_requests) * 100
                pct_weight = (m['weight'] / max_weight) * 100
                time_str = format_timedelta(m['picking_time'])
                finish_str = m['picking_finish'].strftime("%I:%M:%S %p") if pd.notna(m['picking_finish']) else "N/A"

                # Format numbers based on mode
                orders_str = f"{m['orders']:,.1f}" if is_average_mode else f"{int(m['orders']):,}"
                requests_str = f"{m['requests']:,.1f}" if is_average_mode else f"{int(m['requests']):,}"

                rows.append(f'''<tr>
                    <td class="property-name">{m['name']}</td>
                    <td class="progress-cell"><div class="progress-bar" style="width: {pct_time}%; background-color: #6B9AC4;"></div><div class="progress-text">{time_str}</div></td>
                    <td style="padding: 10px;">{finish_str}</td>
                    <td class="progress-cell"><div class="progress-bar" style="width: {pct_orders}%; background-color: #6B9AC4;"></div><div class="progress-text">{orders_str}</div></td>
                    <td class="progress-cell"><div class="progress-bar" style="width: {pct_requests}%; background-color: #6B9AC4;"></div><div class="progress-text">{requests_str}</div></td>
                    <td class="progress-cell"><div class="progress-bar" style="width: {pct_weight}%; background-color: #6B9AC4;"></div><div class="progress-text">{m['weight']:,.2f}</div></td>
                </tr>''')

            html = '''
            <style>
                .comparison-title { font-size: 18px; font-weight: bold; margin-bottom: 15px; color: #2F5496; }
                .comparison-table { border-collapse: collapse; width: auto; font-family: Arial, sans-serif; font-size: 14px; }
                .comparison-table th { background-color: #4472C4; color: white; padding: 12px 20px; text-align: center; border: 1px solid #2F5496; }
                .comparison-table td { padding: 0; border: 1px solid #B4C6E7; text-align: center; color: black; height: 40px; }
                .comparison-table tr:nth-child(odd) td { background-color: #EDEDED; }
                .comparison-table tr:nth-child(even) td { background-color: #D6DCE4; }
                .property-name { font-weight: bold; text-align: left !important; padding: 10px 15px !important; }
                .progress-cell { position: relative; padding: 0 !important; width: 140px; }
                .progress-bar { height: 100%; position: absolute; left: 0; top: 0; }
                .progress-text { position: relative; z-index: 1; padding: 10px; color: black; }
            </style>
            <div class="comparison-title">All Properties Comparison - ''' + date_display + '''</div>
            <table class="comparison-table">
                <tr>
                    <th style="width: 140px;">Property</th>
                    <th style="width: 170px;">''' + picking_time_header + '''</th>
                    <th style="width: 140px;">''' + picking_finish_header + '''</th>
                    <th style="width: 140px;">''' + orders_header + '''</th>
                    <th style="width: 140px;">''' + requests_header + '''</th>
                    <th style="width: 140px;">''' + weight_header + '''</th>
                </tr>''' + ''.join(rows) + '''
            </table>
            '''

            st.markdown(html, unsafe_allow_html=True)

            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
        
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the Google Sheet is shared as 'Anyone with the link can view'")












