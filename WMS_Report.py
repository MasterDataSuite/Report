import streamlit as st
import pandas as pd
from datetime import timedelta

st.set_page_config(page_title="WMS Performance Report", layout="wide")

st.title("📦 WMS Performance Report")

# Google Sheet ID
SHEET_ID = "1gU5p4OK_eRkGiTGtn7CYm8aSzN7nxaCCjV8uSboWWKk"

@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_data():
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"
    df = pd.read_csv(url)
    return df

# Load data
try:
    df = load_data()
    
    # Convert date columns
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    df['Action start'] = pd.to_datetime(df['Action start'])
    df['Action completion'] = pd.to_datetime(df['Action completion'])
    
    # Date selector
    unique_dates = sorted(df['Date'].unique())
    selected_date = st.selectbox("📅 Select Date", unique_dates, format_func=lambda x: x.strftime("%d/%m"))
    
    # Filter by date
    day_df = df[df['Date'] == selected_date].copy()
    
    # Calculate Kilograms per row
    def calc_kg(row):
        if str(row['Unit']).upper() == 'KILOGRAM':
            return row['Quantity']
        elif pd.notna(row['Reporting Unit']) and str(row['Reporting Unit']).upper() == 'KILOGRAM':
            return row['Quantity'] * row['Relationship']
        return 0
    
    # Calculate Liters per row
    def calc_l(row):
        if str(row['Unit']).upper() == 'LITER':
            return row['Quantity']
        elif pd.notna(row['Reporting Unit']) and str(row['Reporting Unit']).upper() == 'LITER':
            return row['Quantity'] * row['Relationship']
        return 0
    
    day_df['Kg'] = day_df.apply(calc_kg, axis=1)
    day_df['Liters'] = day_df.apply(calc_l, axis=1)
    
    # Get unique actions with their times
    unique_actions = day_df.groupby(['Name', 'Action Code']).agg({
        'Action start': 'first',
        'Action completion': 'first'
    }).reset_index()
    unique_actions['picking_time'] = unique_actions['Action completion'] - unique_actions['Action start']
    
    # Aggregate per picker
    picker_times = unique_actions.groupby('Name')['picking_time'].sum().reset_index()
    
    # Aggregate other metrics per picker
    picker_stats = day_df.groupby('Name').agg({
        'Code': 'count',  # Requests fulfilled (row count)
        'Kg': 'sum',
        'Liters': 'sum'
    }).reset_index()
    picker_stats.columns = ['Name', 'Requests fulfilled', 'Kilograms', 'Liters']
    
    # Merge with picking times
    report = picker_stats.merge(picker_times, on='Name')
    
    # Calculate per-minute metrics
    report['picking_minutes'] = report['picking_time'].dt.total_seconds() / 60
    report['Requests per minute'] = report['Requests fulfilled'] / report['picking_minutes']
    report['Kg per min'] = report['Kilograms'] / report['picking_minutes']
    report['L per min'] = report['Liters'] / report['picking_minutes']
    report['Avg per min'] = report['Kg per min'] + report['L per min']
    
    # Format picking time for display
    def format_timedelta(td):
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    
    report['Picking Time'] = report['picking_time'].apply(format_timedelta)
    
    # Reorder columns
    report = report[['Name', 'Picking Time', 'Requests fulfilled', 'Requests per minute', 
                     'Kilograms', 'Liters', 'Kg per min', 'L per min', 'Avg per min', 'picking_time', 'picking_minutes']]
    
    # Calculate max values for progress bars
    max_time = report['picking_time'].max().total_seconds()
    max_requests = report['Requests fulfilled'].max()
    max_kg = report['Kilograms'].max()
    max_l = report['Liters'].max()
    
    # Color function for Avg per min
    def get_avg_color(val):
        if val >= 10:
            return '#90EE90'  # Green
        elif val >= 7:
            return '#FFFF00'  # Yellow
        elif val >= 5:
            return '#FFA500'  # Orange
        else:
            return '#FF6B6B'  # Red
    
    # Build HTML table
    html = '''
    <style>
        .wms-table {
            border-collapse: collapse;
            width: 100%;
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
            background-color: #C65B5B !important;
            color: black;
            font-weight: bold;
            text-align: left !important;
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
        .date-box {
            background-color: #FFFF00;
            padding: 5px 15px;
            border: 1px solid #000;
            display: inline-block;
            color: black;
        }
    </style>
    '''
    
    headers = ['Picker', 'Picking Time', 'Requests fulfilled', 'Requests per minute', 
               'Kilograms', 'Liters', 'Kg per min', 'L per min', 'Avg per min']
    
    html += '<table class="wms-table">'
    html += '<tr>'
    for h in headers:
        html += f'<th>{h}</th>'
    html += '</tr>'
    
    for _, row in report.iterrows():
        html += '<tr>'
        
        # Picker name
        html += f'<td class="picker-name">{row["Name"]}</td>'
        
        # Picking Time with progress bar
        pct = (row['picking_time'].total_seconds() / max_time * 100) if max_time > 0 else 0
        html += f'''<td class="progress-cell">
            <div class="progress-bar" style="width: {pct}%; background-color: #C65B5B;"></div>
            <div class="progress-text">{row["Picking Time"]}</div>
        </td>'''
        
        # Requests fulfilled with progress bar
        pct = (row['Requests fulfilled'] / max_requests * 100) if max_requests > 0 else 0
        html += f'''<td class="progress-cell">
            <div class="progress-bar" style="width: {pct}%; background-color: #5B9BD5;"></div>
            <div class="progress-text">{int(row["Requests fulfilled"])}</div>
        </td>'''
        
        # Requests per minute
        html += f'<td>{row["Requests per minute"]:.2f}</td>'
        
        # Kilograms with progress bar
        pct = (row['Kilograms'] / max_kg * 100) if max_kg > 0 else 0
        html += f'''<td class="progress-cell">
            <div class="progress-bar" style="width: {pct}%; background-color: #FFC000;"></div>
            <div class="progress-text">{row["Kilograms"]:.2f}</div>
        </td>'''
        
        # Liters with progress bar
        pct = (row['Liters'] / max_l * 100) if max_l > 0 else 0
        html += f'''<td class="progress-cell">
            <div class="progress-bar" style="width: {pct}%; background-color: #70AD47;"></div>
            <div class="progress-text">{row["Liters"]:.2f}</div>
        </td>'''
        
        # Kg per min
        html += f'<td>{row["Kg per min"]:.2f}</td>'
        
        # L per min
        html += f'<td>{row["L per min"]:.2f}</td>'
        
        # Avg per min with color
        color = get_avg_color(row['Avg per min'])
        html += f'<td style="background-color: {color}; font-weight: bold;">{row["Avg per min"]:.3f}</td>'
        
        html += '</tr>'
    
    html += '</table>'
    
    # Calculate totals for statistics
    total_picking_time = report['picking_time'].sum()
    total_picking_time_str = format_timedelta(total_picking_time)
    total_requests = report['Requests fulfilled'].sum()
    total_minutes = total_picking_time.total_seconds() / 60
    avg_requests_min = total_requests / total_minutes if total_minutes > 0 else 0
    total_kg = report['Kilograms'].sum()
    total_l = report['Liters'].sum()
    avg_kg_min = total_kg / total_minutes if total_minutes > 0 else 0
    avg_l_min = total_l / total_minutes if total_minutes > 0 else 0
    avg_per_min = avg_kg_min + avg_l_min
    
    # Get picking finish time (latest Action completion)
    picking_finish = day_df['Action completion'].max()
    picking_finish_str = picking_finish.strftime("%I:%M:%S %p") if pd.notna(picking_finish) else ""
    
    date_display = selected_date.strftime("%d/%m")
    
    # Statistics section
    html += f'''
    <div style="margin-top: 40px;">
        <span class="stats-title">Statistics for {date_display}</span>
        <span class="date-box" style="margin-left: 300px;">{date_display}</span>
    </div>
    <table class="stats-table" style="margin-top: 15px;">
        <tr>
            <th>Total Picking Time</th>
            <th>Total Requests</th>
            <th>Avg Requests per minute</th>
            <th>Total Kg</th>
            <th>Total L</th>
            <th colspan="3">Avg Per minute</th>
        </tr>
        <tr>
            <td>{total_picking_time_str}</td>
            <td>{int(total_requests)}</td>
            <td>{avg_requests_min:.2f}</td>
            <td>{total_kg:.2f} Kg</td>
            <td>{total_l:.2f} L</td>
            <td>{avg_kg_min:.2f} Kg</td>
            <td>{avg_l_min:.2f} L</td>
            <td>{avg_per_min:.2f}</td>
        </tr>
    </table>
    <div style="margin-top: 30px;">
        <span class="date-box">{date_display}</span>
    </div>
    <table class="stats-table" style="margin-top: 10px;">
        <tr>
            <th>Picking Finish</th>
            <td>{picking_finish_str}</td>
        </tr>
    </table>
    '''
    
    st.markdown(html, unsafe_allow_html=True)
    
    # Add refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the Google Sheet is shared as 'Anyone with the link can view'")
