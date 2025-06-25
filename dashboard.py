import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Real-time Trading Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Base URL
API_BASE_URL = "http://localhost:8000/api"

class DashboardAPI:
    @staticmethod
    def get_total_revenue(hours=24):
        try:
            response = requests.get(f"{API_BASE_URL}/revenue/total?hours={hours}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error fetching total revenue: {e}")
            return None
    
    @staticmethod
    def get_revenue_breakdown(breakdown_type, hours=24):
        try:
            response = requests.get(f"{API_BASE_URL}/revenue/breakdown/{breakdown_type}?hours={hours}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error fetching {breakdown_type} breakdown: {e}")
            return None
    
    @staticmethod
    def get_fx_rates():
        try:
            response = requests.get(f"{API_BASE_URL}/fx-rates/current")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error fetching FX rates: {e}")
            return None
    
    @staticmethod
    def get_fx_trends(currency_pair, hours=24):
        try:
            response = requests.get(f"{API_BASE_URL}/fx-rates/trends?currency_pair={currency_pair}&hours={hours}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error fetching FX trends: {e}")
            return None
    
    @staticmethod
    def get_hourly_activity(hours=24):
        try:
            response = requests.get(f"{API_BASE_URL}/transactions/hourly?hours={hours}")
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"Error fetching hourly activity: {e}")
            return None

def main():
    st.title("🏦 Real-time Trading Dashboard")
    st.markdown("---")
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    time_period = st.sidebar.selectbox("Time Period", [24, 48, 72, 168], index=0)
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=True)
    
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Create placeholder for auto-refresh
    if auto_refresh:
        placeholder = st.empty()
        
        while True:
            with placeholder.container():
                render_dashboard(time_period)
            time.sleep(30)
            st.rerun()
    else:
        render_dashboard(time_period)

def render_dashboard(time_period):
    # Row 1: Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    # Total Revenue
    total_revenue_data = DashboardAPI.get_total_revenue(time_period)
    if total_revenue_data:
        with col1:
            st.metric(
                label=f"💰 Total Revenue (Last {time_period}h)",
                value=f"${total_revenue_data['total_revenue_usd']:,.2f}",
                delta="USD"
            )
    
    # Get breakdown data for metrics
    country_data = DashboardAPI.get_revenue_breakdown("country", time_period)
    currency_data = DashboardAPI.get_revenue_breakdown("currency", time_period)
    user_data = DashboardAPI.get_revenue_breakdown("user", time_period)
    
    with col2:
        if country_data and country_data['breakdown']:
            top_country = country_data['breakdown'][0]
            st.metric(
                label="🌍 Top Country",
                value=top_country['value'],
                delta=f"${top_country['usd_amount']:,.0f}"
            )
    
    with col3:
        if currency_data and currency_data['breakdown']:
            top_currency = currency_data['breakdown'][0]
            st.metric(
                label="💱 Top Currency",
                value=top_currency['value'],
                delta=f"${top_currency['usd_amount']:,.0f}"
            )
    
    with col4:
        hourly_data = DashboardAPI.get_hourly_activity(time_period)
        if hourly_data and hourly_data['hourly_activity']:
            total_transactions = sum(h['transaction_count'] for h in hourly_data['hourly_activity'])
            st.metric(
                label="📊 Total Transactions",
                value=f"{total_transactions:,}",
                delta=f"Last {time_period}h"
            )
    
    st.markdown("---")
    
    # Row 2: Revenue Breakdown Charts
    st.subheader("📈 Revenue Breakdown Analysis")
    
    breakdown_col1, breakdown_col2 = st.columns(2)
    
    # Country Breakdown
    with breakdown_col1:
        st.write("**Revenue by Country**")
        if country_data and country_data['breakdown']:
            df_country = pd.DataFrame(country_data['breakdown'])
            fig_country = px.pie(
                df_country, 
                values='usd_amount', 
                names='value',
                title="Revenue Distribution by Country"
            )
            fig_country.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig_country, use_container_width=True)
        else:
            st.info("No country data available")
    
    # Currency Breakdown
    with breakdown_col2:
        st.write("**Revenue by Currency**")
        if currency_data and currency_data['breakdown']:
            df_currency = pd.DataFrame(currency_data['breakdown'])
            fig_currency = px.bar(
                df_currency, 
                x='value', 
                y='usd_amount',
                title="Revenue by Original Currency",
                color='usd_amount',
                color_continuous_scale='viridis'
            )
            fig_currency.update_layout(xaxis_title="Currency", yaxis_title="Revenue (USD)")
            st.plotly_chart(fig_currency, use_container_width=True)
        else:
            st.info("No currency data available")
    
    # Row 3: User Analysis
    st.subheader("👤 Top Users Analysis")
    if user_data and user_data['breakdown']:
        df_user = pd.DataFrame(user_data['breakdown'][:10])  # Top 10 users
        fig_user = px.bar(
            df_user, 
            x='usd_amount', 
            y='value',
            orientation='h',
            title="Top 10 Users by Revenue",
            color='usd_amount',
            color_continuous_scale='blues'
        )
        fig_user.update_layout(yaxis_title="User ID", xaxis_title="Revenue (USD)")
        st.plotly_chart(fig_user, use_container_width=True)
    else:
        st.info("No user data available")
    
    st.markdown("---")
    
    # Row 4: FX Rates and Trends
    st.subheader("💱 FX Rates & Trends")
    
    fx_col1, fx_col2 = st.columns(2)
    
    # Current FX Rates
    with fx_col1:
        st.write("**Current FX Rates**")
        fx_rates_data = DashboardAPI.get_fx_rates()
        if fx_rates_data and fx_rates_data['fx_rates']:
            df_fx = pd.DataFrame(fx_rates_data['fx_rates'])
            df_fx['timestamp'] = pd.to_datetime(df_fx['timestamp'])
            
            # Display as table
            st.dataframe(
                df_fx[['currency_pair', 'rate', 'timestamp']].sort_values('currency_pair'),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No FX rates data available")
    
    # FX Trends
    with fx_col2:
        st.write("**FX Rate Trends**")
        if fx_rates_data and fx_rates_data['fx_rates']:
            # Select currency pair for trends
            currency_pairs = [rate['currency_pair'] for rate in fx_rates_data['fx_rates']]
            if currency_pairs:
                selected_pair = st.selectbox("Select Currency Pair", currency_pairs, key="fx_trends")
                
                trends_data = DashboardAPI.get_fx_trends(selected_pair, time_period)
                if trends_data and trends_data['trends']:
                    df_trends = pd.DataFrame(trends_data['trends'])
                    df_trends['timestamp'] = pd.to_datetime(df_trends['timestamp'])
                    
                    fig_trends = px.line(
                        df_trends, 
                        x='timestamp', 
                        y='rate',
                        title=f"{selected_pair} Rate Trends",
                        markers=True
                    )
                    fig_trends.update_layout(
                        xaxis_title="Time", 
                        yaxis_title="Exchange Rate"
                    )
                    st.plotly_chart(fig_trends, use_container_width=True)
                else:
                    st.info("No trends data available for selected pair")
    
    st.markdown("---")
    
    # Row 5: Hourly Sales Activity
    st.subheader("📊 Hourly Sales Activity")
    if hourly_data and hourly_data['hourly_activity']:
        df_hourly = pd.DataFrame(hourly_data['hourly_activity'])
        df_hourly['hour'] = pd.to_datetime(df_hourly['hour'])
        
        # Create subplot with secondary y-axis
        fig_hourly = go.Figure()
        
        # Add transaction count
        fig_hourly.add_trace(
            go.Bar(
                x=df_hourly['hour'],
                y=df_hourly['transaction_count'],
                name='Transaction Count',
                yaxis='y',
                opacity=0.7
            )
        )
        
        # Add revenue line
        fig_hourly.add_trace(
            go.Scatter(
                x=df_hourly['hour'],
                y=df_hourly['revenue_usd'],
                mode='lines+markers',
                name='Revenue (USD)',
                yaxis='y2',
                line=dict(color='red', width=3)
            )
        )
        
        # Update layout for dual y-axis
        fig_hourly.update_layout(
            title="Hourly Transaction Count vs Revenue",
            xaxis_title="Hour",
            yaxis=dict(title="Transaction Count", side="left"),
            yaxis2=dict(title="Revenue (USD)", side="right", overlaying="y"),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig_hourly, use_container_width=True)
    else:
        st.info("No hourly activity data available")
    
    # Footer
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data covers last {time_period} hours")

if __name__ == "__main__":
    main()