import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

import json

# Page config
st.set_page_config(
    page_title="Phoenix Crime Data Dashboard",
    page_icon="🚔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:5000/api"

# Cache API calls to avoid repeated requests
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_api_data(endpoint, params=None):
    """Fetch data from Flask API with caching"""
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        response = requests.get(url, params=params or {})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"API Error: {e}")
        return None

def main():
    st.title("🚔 Phoenix Crime Data Dashboard")
    st.markdown("Real-time analysis of Phoenix crime statistics")
    
    # Check API health
    health = fetch_api_data("health")
    if not health or health.get('status') != 'healthy':
        st.error("⚠️ API is not responding. Make sure your Flask API is running on http://localhost:5000")
        st.stop()
    
    # Sidebar for filters
    st.sidebar.header("📊 Dashboard Controls")
    
    # Get crime types for filter
    crime_types_data = fetch_api_data("crime-types")
    crime_types = ['All'] + (crime_types_data.get('crime_types', []) if crime_types_data else [])
    
    selected_crime_type = st.sidebar.selectbox(
        "Select Crime Type",
        crime_types
    )
    
    # Year filter
    current_year = datetime.now().year
    selected_year = st.sidebar.selectbox(
        "Select Year",
        ['All'] + list(range(current_year, current_year - 10, -1))
    )
    
    # ZIP code filter
    zip_code = st.sidebar.text_input("ZIP Code (optional)", placeholder="e.g., 85001")
    
    # Main dashboard content
    col1, col2, col3, col4 = st.columns(4)
    
    # Get statistics
    stats = fetch_api_data("stats")
    if stats:
        with col1:
            st.metric("Total Crimes", f"{stats['total_crimes']:,}")
        
        with col2:
            years_span = len(stats.get('crimes_by_year', []))
            st.metric("Years of Data", years_span)
        
        with col3:
            crime_types_count = len(stats.get('top_crime_types', []))
            st.metric("Crime Types", crime_types_count)
        
        with col4:
            if stats.get('date_range'):
                latest_date = stats['date_range']['latest'][:10]  # Extract date part
                st.metric("Latest Data", latest_date)
    
    st.divider()
    
    # Row 1: Crime types and timeline
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("🔍 Top Crime Types")
        if stats and stats.get('top_crime_types'):
            # Create bar chart
            crime_df = pd.DataFrame(stats['top_crime_types'])
            fig = px.bar(
                crime_df.head(10), 
                x='count', 
                y='crime_type',
                orientation='h',
                title="Most Common Crimes",
                labels={'count': 'Number of Crimes', 'crime_type': 'Crime Type'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Crimes by Year")
        if stats and stats.get('crimes_by_year'):
            # Create line chart
            yearly_df = pd.DataFrame(stats['crimes_by_year'])
            yearly_df['year'] = yearly_df['year'].astype(int)
            yearly_df = yearly_df.sort_values('year')
            
            fig = px.line(
                yearly_df, 
                x='year', 
                y='count',
                title="Crime Trends Over Time",
                markers=True,
                labels={'count': 'Number of Crimes', 'year': 'Year'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Row 2: ZIP code analysis and timeline
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("🏘️ Crimes by ZIP Code")
        zip_stats = fetch_api_data("crimes/by-zip")
        if zip_stats and zip_stats.get('zip_code_stats'):
            zip_df = pd.DataFrame(zip_stats['zip_code_stats'])
            fig = px.bar(
                zip_df.head(10),
                x='zip_code',
                y='crime_count',
                title="Top 10 ZIP Codes by Crime Count",
                labels={'crime_count': 'Number of Crimes', 'zip_code': 'ZIP Code'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📅 Monthly Timeline")
        timeline = fetch_api_data("crimes/timeline")
        if timeline and timeline.get('timeline'):
            timeline_df = pd.DataFrame(timeline['timeline'])
            timeline_df['month'] = pd.to_datetime(timeline_df['month'])
            timeline_df = timeline_df.sort_values('month')
            
            fig = px.line(
                timeline_df,
                x='month',
                y='crime_count',
                title="Crime Trends by Month",
                markers=True,
                labels={'crime_count': 'Number of Crimes', 'month': 'Month'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Row 3: Recent crimes and filtered data
    st.subheader("🕐 Recent Crime Activity")
    
    # Build filters for API call
    filters = {}
    if selected_crime_type != 'All':
        filters['crime_type'] = selected_crime_type
    if selected_year != 'All':
        filters['year'] = selected_year
    if zip_code:
        filters['zip_code'] = zip_code
    
    # Get filtered crimes
    crimes_data = fetch_api_data("crimes", {**filters, 'per_page': 20, 'page': 1})
    
    if crimes_data and crimes_data.get('crimes'):
        # Display summary of filters
        if filters:
            filter_text = ", ".join([f"{k}: {v}" for k, v in filters.items()])
            st.info(f"Showing results filtered by: {filter_text}")
            st.info(f"Found {crimes_data['pagination']['total_records']:,} matching records")
        
        # Display recent crimes table
        crimes_df = pd.DataFrame(crimes_data['crimes'])
        
        # Clean up the dataframe for display
        display_df = crimes_df.copy()
        display_df['occurred_date'] = pd.to_datetime(display_df['occurred_date']).dt.strftime('%Y-%m-%d %H:%M')
        
        # Select and rename columns for better display
        display_columns = {
            'incident_id': 'Incident ID',
            'crime_type': 'Crime Type', 
            'occurred_date': 'Date & Time',
            'address': 'Address',
            'zip_code': 'ZIP',
            'premise_type': 'Premise'
        }
        
        display_df = display_df[list(display_columns.keys())].rename(columns=display_columns)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Pagination info
        pagination = crimes_data['pagination']
        st.caption(f"Showing page {pagination['page']} of {pagination['total_pages']} ({pagination['total_records']:,} total records)")
    
    else:
        st.warning("No crime data available with current filters")
    
    # Footer
    st.divider()
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        st.caption("Data updates every 5 minutes")
    
    with col3:
        st.caption(f"Dashboard last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()