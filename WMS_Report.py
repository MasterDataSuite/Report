import streamlit as st
import pandas as pd
from datetime import timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

st.set_page_config(page_title="WMS Performance Report", layout="wide")

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

st.title("📦 WMS Performance Report")

# Google Drive folder ID
FOLDER_ID = st.secrets["folder_id"]

@st.cache_data(ttl=60)
def get_files_list():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    service = build('drive', 'v3', credentials=credentials)
    query = f"'{FOLDER_ID}' in parents and (mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or mimeType='application/vnd.ms-excel')"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        raise Exception("No Excel files found in the folder")
    return files

@st.cache_data(ttl=60)
def load_data(file_id):
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=['https://www.googleapis.com/auth/drive.readonly']
    )
    service = build('drive', 'v3', credentials=credentials)
    request = service.files().get_media(fileId=file_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_buffer.seek(0)
    df = pd.read_excel(file_buffer, sheet_name='Input')
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
    file_names = [f['name'].replace('.xlsx', '').replace('.xls', '') for f in files]
    
    # Mode selector
    col_mode, col_rest = st.columns([170, 1200])
    with col_mode:
        mode = st.selectbox("🎯 Mode", ["", "Daily Monitor", "Analytics Mode", "Comparison Mode"], index=0)

    if not mode:
        st.info("👆 Please select a mode to continue")
        st.stop()

    # Different UI based on mode
    if mode == "Comparison Mode":
        # Comparison Mode specific dropdowns
        col1, col2, col3, col4, col5, col6, col_empty = st.columns([220, 140, 140, 140, 160, 140, 800])

        with col1:
            comparison_type = st.selectbox("📊 Compare", ["", "Property vs Property"], index=0)

        with col2:
            property_1 = st.selectbox("🏪 Property 1", [""] + file_names, index=0)

        with col3:
            # Exclude property_1 from property_2 options
            property_2_options = [""] + [f for f in file_names if f != property_1]
            property_2 = st.selectbox("🏪 Property 2", property_2_options, index=0)

        # Load data and find common dates when both properties selected
        common_dates = []
        df1 = None
        df2 = None

        if property_1 and property_2:
            file_1 = next(f for f in files if f['name'].replace('.xlsx', '').replace('.xls', '') == property_1)
            file_2 = next(f for f in files if f['name'].replace('.xlsx', '').replace('.xls', '') == property_2)

            df1 = load_data(file_1['id'])
            df1['Date'] = pd.to_datetime(df1['Date']).dt.date
            df1['Action start'] = pd.to_datetime(df1['Action start'])
            df1['Action completion'] = pd.to_datetime(df1['Action completion'])

            df2 = load_data(file_2['id'])
            df2['Date'] = pd.to_datetime(df2['Date']).dt.date
            df2['Action start'] = pd.to_datetime(df2['Action start'])
            df2['Action completion'] = pd.to_datetime(df2['Action completion'])

            dates_1 = set(df1['Date'].unique())
            dates_2 = set(df2['Date'].unique())
            common_dates = sorted(dates_1 & dates_2)

        with col4:
            date_type = st.selectbox("📅 Date Type", ["", "Single Date", "Date Range"], index=0)

        with col5:
            if common_dates and date_type == "Single Date":
                selected_comparison_date = st.selectbox("📅 Date", [""] + [d.strftime("%d/%m") for d in common_dates], index=0)
            elif common_dates and date_type == "Date Range":
                start_date = st.selectbox("📅 Start", [""] + [d.strftime("%d/%m") for d in common_dates], index=0)
            else:
                st.selectbox("📅 Date", [""], index=0, disabled=True)

        with col6:
            if common_dates and date_type == "Date Range":
                end_date = st.selectbox("📅 End", [""] + [d.strftime("%d/%m") for d in common_dates], index=0)
            else:
                st.empty()

        # Validation for Comparison Mode
        if not comparison_type:
            st.info("👆 Please select a comparison type")
            st.stop()

        if not property_1 or not property_2:
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

    else:
        # Daily Monitor / Analytics Mode - original dropdowns
        col2, col3, col4, col_empty = st.columns([165, 110, 130, 800])

        with col2:
            view_type = st.selectbox("👁️ View", ["", "Department View", "Worker View"], index=0)

        with col3:
            selected_store = st.selectbox("🏪 Store", [""] + file_names, index=0)

        with col4:
            if selected_store:
                selected_file = next(f for f in files if f['name'].replace('.xlsx', '').replace('.xls', '') == selected_store)
                df = load_data(selected_file['id'])
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                df['Action start'] = pd.to_datetime(df['Action start'])
                df['Action completion'] = pd.to_datetime(df['Action completion'])
                unique_dates = sorted(df['Date'].unique())
                selected_date = st.selectbox("📅 Date", [""] + [d.strftime("%d/%m") for d in unique_dates], index=0)
            else:
                selected_date = st.selectbox("📅 Date", [""], index=0)

        if not view_type or not selected_store or not selected_date:
            st.info("👆 Please make all selections to continue")
            st.stop()

        # Convert selected_date back to date object
        selected_date = next(d for d in unique_dates if d.strftime("%d/%m") == selected_date)
        day_df = df[df['Date'] == selected_date].copy()

        day_df['Kg'] = day_df.apply(calc_kg, axis=1)
        day_df['Liters'] = day_df.apply(calc_l, axis=1)

    # ============== DAILY MONITOR MODE ==============
    if mode == "Daily Monitor":
        
        # ============== DEPARTMENT VIEW ==============
        if view_type == "Department View":
            
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
                'Action Code': 'nunique',
                'Code': 'count',
                'Kg': 'sum',
                'Liters': 'sum'
            }).reset_index()
            dept_stats.columns = ['Cost Center', '# of Orders', 'Unique Item Requests', 'Kilograms', 'Liters']
            dept_stats['Total Weight'] = dept_stats['Kilograms'] + dept_stats['Liters']
            
            dept_report = dept_stats.merge(dept_times, on='Cost Center')
            dept_report['Total Picking Time'] = dept_report['picking_time'].apply(format_timedelta)
            
            # Sort by selected column
            sort_col = st.session_state.dept_sort_col
            sort_asc = st.session_state.dept_sort_asc
            
            if sort_col == 'Total Picking Time':
                dept_report = dept_report.sort_values('picking_time', ascending=sort_asc).reset_index(drop=True)
            elif sort_col in dept_report.columns:
                dept_report = dept_report.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)
            
            max_orders = dept_report['# of Orders'].max()
            max_requests = dept_report['Unique Item Requests'].max()
            max_kg = dept_report['Kilograms'].max()
            max_l = dept_report['Liters'].max()
            max_weight = dept_report['Total Weight'].max()
            max_time = dept_report['picking_time'].max().total_seconds()
            
            # Sort controls in one row
            sort_options = ['# of Orders', 'Unique Item Requests', 'Kilograms', 'Liters', 'Total Weight', 'Total Picking Time']
            col_sort1, col_sort2, col_sort3 = st.columns([2, 2, 6])
            with col_sort1:
                sort_col_display = st.selectbox(
                    "Sort by",
                    sort_options,
                    index=sort_options.index(st.session_state.dept_sort_col) if st.session_state.dept_sort_col in sort_options else 4,
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
                ('Cost Center', '280px', False),
                ('# of Orders', '110px', True),
                ('Unique Item Requests', '180px', True),
                ('Kilograms', '120px', True),
                ('Liters', '120px', True),
                ('Total Weight', '120px', True),
                ('Total Picking Time', '150px', True)
            ]
            
            html += '<table class="wms-table">'
            html += '<tr>'
            for h, w, sortable in headers:
                html += f'<th style="width: {w};">{h}</th>'
            html += '</tr>'
            
            for _, row in dept_report.iterrows():
                html += '<tr>'
                
                html += f'<td class="dept-name">{row["Cost Center"]}</td>'
                
                pct = (row['# of Orders'] / max_orders * 100) if max_orders > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{int(row["# of Orders"])}</div>
                </td>'''
                
                pct = (row['Unique Item Requests'] / max_requests * 100) if max_requests > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{int(row["Unique Item Requests"])}</div>
                </td>'''
                
                pct = (row['Kilograms'] / max_kg * 100) if max_kg > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #FFC000;"></div>
                    <div class="progress-text">{row["Kilograms"]:.2f}</div>
                </td>'''
                
                pct = (row['Liters'] / max_l * 100) if max_l > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #70AD47;"></div>
                    <div class="progress-text">{row["Liters"]:.2f}</div>
                </td>'''
                
                pct = (row['Total Weight'] / max_weight * 100) if max_weight > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #9B59B6;"></div>
                    <div class="progress-text">{row["Total Weight"]:.2f}</div>
                </td>'''
                
                pct = (row['picking_time'].total_seconds() / max_time * 100) if max_time > 0 else 0
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
            
            # Initialize sort state for worker view
            if 'worker_sort_col' not in st.session_state:
                st.session_state.worker_sort_col = 'Weight per min'
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
            
            report['Picking Time'] = report['picking_time'].apply(format_timedelta)
            
            # Sort by selected column
            sort_col = st.session_state.worker_sort_col
            sort_asc = st.session_state.worker_sort_asc
            
            if sort_col == 'Picking Time':
                report = report.sort_values('picking_time', ascending=sort_asc).reset_index(drop=True)
            elif sort_col in report.columns:
                report = report.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)
            
            report = report[['Name', 'Picking Time', 'Requests fulfilled', 'Requests per minute', 
                             'Kilograms', 'Liters', 'Total Weight', 'Weight per min', 'picking_time', 'picking_minutes']]
            
            max_time = report['picking_time'].max().total_seconds()
            max_requests = report['Requests fulfilled'].max()
            max_kg = report['Kilograms'].max()
            max_l = report['Liters'].max()
            max_weight = report['Total Weight'].max()
            
            # Sort controls in one row
            sort_options = ['Picking Time', 'Requests fulfilled', 'Requests per minute', 'Kilograms', 'Liters', 'Total Weight', 'Weight per min']
            col_sort1, col_sort2, col_sort3 = st.columns([2, 2, 6])
            with col_sort1:
                sort_col_display = st.selectbox(
                    "Sort by",
                    sort_options,
                    index=sort_options.index(st.session_state.worker_sort_col) if st.session_state.worker_sort_col in sort_options else 6,
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
                ('Picking Time', '120px'),
                ('Requests fulfilled', '140px'),
                ('Requests per minute', '150px'),
                ('Kilograms', '100px'),
                ('Liters', '100px'),
                ('Total Weight', '110px'),
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
                
                pct = (row['picking_time'].total_seconds() / max_time * 100) if max_time > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #C65B5B;"></div>
                    <div class="progress-text">{row["Picking Time"]}</div>
                </td>'''
                
                pct = (row['Requests fulfilled'] / max_requests * 100) if max_requests > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
                    <div class="progress-text">{int(row["Requests fulfilled"])}</div>
                </td>'''
                
                html += f'<td>{row["Requests per minute"]:.2f}</td>'
                
                pct = (row['Kilograms'] / max_kg * 100) if max_kg > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #FFC000;"></div>
                    <div class="progress-text">{row["Kilograms"]:.2f}</div>
                </td>'''
                
                pct = (row['Liters'] / max_l * 100) if max_l > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #70AD47;"></div>
                    <div class="progress-text">{row["Liters"]:.2f}</div>
                </td>'''
                
                pct = (row['Total Weight'] / max_weight * 100) if max_weight > 0 else 0
                html += f'''<td class="progress-cell">
                    <div class="progress-bar" style="width: {pct}%; background-color: #9B59B6;"></div>
                    <div class="progress-text">{row["Total Weight"]:.2f}</div>
                </td>'''
                
                color = get_avg_color(row['Weight per min'])
                html += f'<td style="background-color: {color}; font-weight: bold;">{row["Weight per min"]:.2f}</td>'
                
                html += '</tr>'
            
            html += '</table>'
            
            # Statistics section
            total_picking_time = total_picking_time_no_overlap
            total_picking_time_str = format_timedelta(total_picking_time)
            total_requests = report['Requests fulfilled'].sum()
            total_minutes = total_picking_time.total_seconds() / 60
            avg_requests_min = total_requests / total_minutes if total_minutes > 0 else 0
            total_kg = report['Kilograms'].sum()
            total_l = report['Liters'].sum()
            total_weight = total_kg + total_l
            weight_per_min = total_weight / total_minutes if total_minutes > 0 else 0
            
            picking_finish = day_df['Action completion'].max()
            picking_finish_str = picking_finish.strftime("%I:%M:%S %p") if pd.notna(picking_finish) else ""
            
            html += f'''
            <table class="stats-table" style="margin-top: 15px;">
                <tr>
                    <th>Total Picking Time</th>
                    <th>Picking Finish</th>
                    <th>Total Requests</th>
                    <th>Avg Requests/min</th>
                    <th>Total Kg</th>
                    <th>Total L</th>
                    <th>Total Weight</th>
                    <th>Weight/min</th>
                </tr>
                <tr>
                    <td>{total_picking_time_str}</td>
                    <td>{picking_finish_str}</td>
                    <td>{int(total_requests)}</td>
                    <td>{avg_requests_min:.2f}</td>
                    <td>{total_kg:.2f}</td>
                    <td>{total_l:.2f}</td>
                    <td>{total_weight:.2f}</td>
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
            # Filter data for selected dates
            df1_filtered = df1[df1['Date'].isin(comparison_dates)].copy()
            df2_filtered = df2[df2['Date'].isin(comparison_dates)].copy()

            # Calculate Kg and Liters
            df1_filtered['Kg'] = df1_filtered.apply(calc_kg, axis=1)
            df1_filtered['Liters'] = df1_filtered.apply(calc_l, axis=1)
            df2_filtered['Kg'] = df2_filtered.apply(calc_kg, axis=1)
            df2_filtered['Liters'] = df2_filtered.apply(calc_l, axis=1)

            # Calculate metrics for Property 1
            unique_actions_1 = df1_filtered.groupby(['Name', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            total_picking_time_1 = calculate_total_time_no_overlap(unique_actions_1)
            picking_finish_1 = df1_filtered['Action completion'].max()
            total_orders_1 = df1_filtered['Action Code'].nunique()
            total_requests_1 = len(df1_filtered)
            total_kg_1 = df1_filtered['Kg'].sum()
            total_liters_1 = df1_filtered['Liters'].sum()
            total_weight_1 = total_kg_1 + total_liters_1

            # Calculate metrics for Property 2
            unique_actions_2 = df2_filtered.groupby(['Name', 'Action Code']).agg({
                'Action start': 'first',
                'Action completion': 'first'
            }).reset_index()
            total_picking_time_2 = calculate_total_time_no_overlap(unique_actions_2)
            picking_finish_2 = df2_filtered['Action completion'].max()
            total_orders_2 = df2_filtered['Action Code'].nunique()
            total_requests_2 = len(df2_filtered)
            total_kg_2 = df2_filtered['Kg'].sum()
            total_liters_2 = df2_filtered['Liters'].sum()
            total_weight_2 = total_kg_2 + total_liters_2

            # Format values
            picking_time_str_1 = format_timedelta(total_picking_time_1)
            picking_time_str_2 = format_timedelta(total_picking_time_2)
            picking_finish_str_1 = picking_finish_1.strftime("%I:%M:%S %p") if pd.notna(picking_finish_1) else "N/A"
            picking_finish_str_2 = picking_finish_2.strftime("%I:%M:%S %p") if pd.notna(picking_finish_2) else "N/A"

            # Date range display
            if len(comparison_dates) == 1:
                date_display = comparison_dates[0].strftime("%d/%m/%Y")
            else:
                date_display = f"{comparison_dates[0].strftime('%d/%m/%Y')} - {comparison_dates[-1].strftime('%d/%m/%Y')}"

            # Calculate percentages for bar charts (relative to max of the two)
            max_picking_time = max(total_picking_time_1.total_seconds(), total_picking_time_2.total_seconds(), 1)
            max_orders = max(total_orders_1, total_orders_2, 1)
            max_requests = max(total_requests_1, total_requests_2, 1)
            max_weight = max(total_weight_1, total_weight_2, 1)

            pct_time_1 = (total_picking_time_1.total_seconds() / max_picking_time) * 100
            pct_time_2 = (total_picking_time_2.total_seconds() / max_picking_time) * 100
            pct_orders_1 = (total_orders_1 / max_orders) * 100
            pct_orders_2 = (total_orders_2 / max_orders) * 100
            pct_requests_1 = (total_requests_1 / max_requests) * 100
            pct_requests_2 = (total_requests_2 / max_requests) * 100
            pct_weight_1 = (total_weight_1 / max_weight) * 100
            pct_weight_2 = (total_weight_2 / max_weight) * 100

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
                    <th style="width: 180px;">Property</th>
                    <th style="width: 140px;">Total Picking Time</th>
                    <th style="width: 140px;">Picking Finish</th>
                    <th style="width: 140px;"># of Orders</th>
                    <th style="width: 140px;">Item Requests</th>
                    <th style="width: 140px;">Total Weight</th>
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
                        <div class="progress-text">{total_orders_1:,}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_requests_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{total_requests_1:,}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_weight_1}%; background-color: #6B9AC4;"></div>
                        <div class="progress-text">{total_weight_1:,.2f}</div>
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
                        <div class="progress-text">{total_orders_2:,}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_requests_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{total_requests_2:,}</div>
                    </td>
                    <td class="progress-cell">
                        <div class="progress-bar" style="width: {pct_weight_2}%; background-color: #97B8D6;"></div>
                        <div class="progress-text">{total_weight_2:,.2f}</div>
                    </td>
                </tr>
            </table>
            '''

            st.markdown(html, unsafe_allow_html=True)

            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
    
    # ============== ANALYTICS MODE ==============
    elif mode == "Analytics Mode":
        st.info("🚧 Analytics Mode coming soon! This will include:\n\n- Trends over time")
        
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the Google Sheet is shared as 'Anyone with the link can view'")





