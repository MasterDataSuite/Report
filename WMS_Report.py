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
    
    # All dropdowns in one row
    col1, col2, col3, col4, col_empty = st.columns([170, 165, 110, 130, 800])
    
    with col1:
        mode = st.selectbox("🎯 Mode", ["", "Daily Monitor", "Analytics Mode", "Comparison Mode"], index=0)
    
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
    
    if not mode or not view_type or not selected_store or not selected_date:
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
                    ["Descending ↓", "Ascending ↑"],
                    index=0 if not st.session_state.dept_sort_asc else 1,
                    key="sort_order"
                )
            
            if sort_col_display != st.session_state.dept_sort_col or (sort_order == "Ascending ↑") != st.session_state.dept_sort_asc:
                st.session_state.dept_sort_col = sort_col_display
                st.session_state.dept_sort_asc = (sort_order == "Ascending ↑")
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
            report['Kg per min'] = report['Kilograms'] / report['picking_minutes']
            report['L per min'] = report['Liters'] / report['picking_minutes']
            report['Avg per min'] = report['Kg per min'] + report['L per min']
            
            report['Picking Time'] = report['picking_time'].apply(format_timedelta)
            
            report = report[['Name', 'Picking Time', 'Requests fulfilled', 'Requests per minute', 
                             'Kilograms', 'Liters', 'Kg per min', 'L per min', 'Avg per min', 'picking_time', 'picking_minutes']]
            
            max_time = report['picking_time'].max().total_seconds()
            max_requests = report['Requests fulfilled'].max()
            max_kg = report['Kilograms'].max()
            max_l = report['Liters'].max()
            
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
                ('Kg per min', '100px'),
                ('L per min', '100px'),
                ('Avg per min', '100px')
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
                
                html += f'<td>{row["Kg per min"]:.2f}</td>'
                html += f'<td>{row["L per min"]:.2f}</td>'
                
                color = get_avg_color(row['Avg per min'])
                html += f'<td style="background-color: {color}; font-weight: bold;">{row["Avg per min"]:.3f}</td>'
                
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
            avg_kg_min = total_kg / total_minutes if total_minutes > 0 else 0
            avg_l_min = total_l / total_minutes if total_minutes > 0 else 0
            avg_per_min = avg_kg_min + avg_l_min
            
            picking_finish = day_df['Action completion'].max()
            picking_finish_str = picking_finish.strftime("%I:%M:%S %p") if pd.notna(picking_finish) else ""
            
            date_display = selected_date.strftime("%d/%m")
            
            html += f'''
            <table class="stats-table" style="margin-top: 15px;">
                <tr>
                    <th>Total Picking Time</th>
                    <th>Picking Finish</th>
                    <th>Total Requests</th>
                    <th>Avg Requests/min</th>
                    <th>Total Kg</th>
                    <th>Total L</th>
                    <th>Kg/min</th>
                    <th>L/min</th>
                    <th>Combined/min</th>
                </tr>
                <tr>
                    <td>{total_picking_time_str}</td>
                    <td>{picking_finish_str}</td>
                    <td>{int(total_requests)}</td>
                    <td>{avg_requests_min:.2f}</td>
                    <td>{total_kg:.2f}</td>
                    <td>{total_l:.2f}</td>
                    <td>{avg_kg_min:.2f}</td>
                    <td>{avg_l_min:.2f}</td>
                    <td>{avg_per_min:.2f}</td>
                </tr>
            </table>
            '''

            
            st.markdown(html, unsafe_allow_html=True)
            
            if st.button("🔄 Refresh Data"):
                st.cache_data.clear()
                st.rerun()
    
    # ============== COMPARISON MODE ==============
    elif mode == "Comparison Mode":
        st.info("🚧 Comparison Mode coming soon! This will include:\n\n- Property vs Property\n- All Properties Overview\n- Departments per Property\n- Workers per Property")
    
    # ============== ANALYTICS MODE ==============
    elif mode == "Analytics Mode":
        st.info("🚧 Analytics Mode coming soon! This will include:\n\n- Trends over time")
        
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the Google Sheet is shared as 'Anyone with the link can view'")




