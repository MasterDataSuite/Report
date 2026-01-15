import streamlit as st
import pandas as pd


st.title("📦 WMS Performance Report")

uploaded_file = st.file_uploader("Upload WMS Report", type=["xlsx"])

if uploaded_file:
    # Read Sheet2
    df = pd.read_excel(uploaded_file, sheet_name="Sheet2", header=None)
    
    # Display the sheet
    st.dataframe(df, use_container_width=True, height=600)

else:
    st.info("👆 Upload your WMS report file")