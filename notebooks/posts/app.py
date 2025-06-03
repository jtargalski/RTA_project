import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time
import altair as alt
from datetime import datetime, timedelta
 
# --- MongoDB setup ---
mongo_client = MongoClient('mongodb://root:admin@mongo:27017/')
db = mongo_client['social_media_analytics']
collection = db['post_stats']
history_collection = db['conversion_rate_history']
 
st.set_page_config(page_title="Social Media Analytics Dashboard", layout="wide")
st.title("📊 Social Media Posts Analytics Dashboard")
 
REFRESH_INTERVAL = 5  # seconds
placeholder = st.empty()
 
# --- Session state for real-time conversion history ---
if "conversion_history" not in st.session_state:
    st.session_state.conversion_history = []
 
# --- Helper functions ---
def classify_activity(rate, avg_rate):
    """Classify engagement rate compared to the average."""
    if rate > avg_rate * 1.5:
        return '📈 High'
    elif rate < avg_rate * 0.5:
        return '📉 Low'
    return '⚖️ Average'
 
def bar_chart_with_avg(series, label):
    """Plot a bar chart with an average line for any metric series."""
    avg = series.mean()
    df_chart = pd.DataFrame({
        'post_id': series.index,
        label: series.values
    })
    base = alt.Chart(df_chart).mark_bar().encode(
        x=alt.X('post_id:N', title='Post ID'),
        y=alt.Y(f'{label}:Q', title=label)
    )
    rule = alt.Chart(pd.DataFrame({'avg': [avg]})).mark_rule(color='red', strokeDash=[4, 4]).encode(
        y='avg:Q'
    )
    st.altair_chart(base + rule, use_container_width=True)
 
def load_current_stats():
    data = list(collection.find({}, {'_id': 0}))
    return pd.DataFrame(data) if data else pd.DataFrame()
 
def load_conversion_history():
    data = list(history_collection.find({}, {'_id': 0}))
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    return df.sort_values('timestamp')
 
# --- Main Dashboard Loop ---
while True:
    with placeholder.container():
        df = load_current_stats()
 
        if df.empty:
            st.info("⏳ Waiting for data...")
        else:
            # --- Process and enrich data ---
            df = df.sort_values(by='impressions', ascending=False)
            df['conversion_rate'] = (df['conversion_rate'] * 100).round(2)
            df['engagement_rate'] = df['engagements'] / df['impressions']
 
            avg_engagement_rate = df['engagement_rate'].mean()
            avg_conversion_rate = df['conversion_rate'].mean()
            avg_conversions = df['conversions'].mean()
 
            df['engagement_activity'] = df['engagement_rate'].apply(lambda r: classify_activity(r, avg_engagement_rate))
            df['is_trending'] = df['conversions'].apply(lambda c: '🔥 Trending' if c > avg_conversions * 1.5 else '')
 
            # --- Flag anomaly in impressions ---
            mean_impr = df['impressions'].mean()
            std_impr = df['impressions'].std()
            threshold = mean_impr + 2 * std_impr
            df['flagged'] = df['impressions'] > threshold
            flagged_posts = df[df['flagged']]
 
            if not flagged_posts.empty:
                st.markdown(
                    "<h2 style='color:#ff3333;'>🚨 Abnormally High Impressions Detected!</h2>",
                    unsafe_allow_html=True
                )
                st.warning(
                    f"⚠️ {len(flagged_posts)} post(s) exceed 2 standard deviations above the mean impressions — review suggested."
                )
                for _, row in flagged_posts.iterrows():
                    st.markdown(
                        f"<h4 style='color:#d7263d;'>🔎 <b>{row['post_id']}</b> — {row['impressions']} impressions</h4>",
                        unsafe_allow_html=True
                    )
 
            # --- KPI Metrics ---
            k1, k2, k3 = st.columns(3)
            k1.metric("Total Posts", len(df))
            k2.metric("Avg Engagement Rate", f"{avg_engagement_rate*100:.2f}%")
            k3.metric("Avg Conversion Rate", f"{avg_conversion_rate:.2f}%")
 
            # --- Detailed Table ---
            st.dataframe(df[['post_id', 'impressions', 'engagements', 'conversions', 'conversion_rate', 'engagement_activity', 'is_trending', 'flagged']])
 
            # --- Alerts for low engagement ---
            low_eng = df[df['engagement_activity'] == '📉 Low']
            if not low_eng.empty:
                st.warning(f"⚠️ {len(low_eng)} post(s) have low engagement. Consider reviewing.")
 
            # --- Bar Charts ---
            st.subheader("📊 Core Metrics Bar Charts")
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("📢 Impressions per Post")
                bar_chart_with_avg(df.set_index('post_id')['impressions'], 'Impressions')
                st.subheader("💬 Engagements per Post")
                bar_chart_with_avg(df.set_index('post_id')['engagements'], 'Engagements')
            with col2:
                st.subheader("🛒 Conversions per Post")
                bar_chart_with_avg(df.set_index('post_id')['conversions'], 'Conversions')
                st.subheader("📈 Conversion Rate (%) per Post")
                bar_chart_with_avg(df.set_index('post_id')['conversion_rate'], 'Conversion Rate (%)')
 
        # --- Real-time line chart ---
        hist_df = load_conversion_history()
        now = datetime.utcnow()
        one_min_ago = now - timedelta(seconds=60)
 
        if not hist_df.empty:
            recent = hist_df[hist_df['timestamp'] >= one_min_ago]
            for _, row in recent.iterrows():
                point = (row['timestamp'], row['average_conversion_rate'])
                if point not in st.session_state.conversion_history:
                    st.session_state.conversion_history.append(point)
 
            st.session_state.conversion_history = [
                p for p in st.session_state.conversion_history if p[0] >= one_min_ago
            ]
 
            if st.session_state.conversion_history:
                avg_df = pd.DataFrame(st.session_state.conversion_history, columns=['timestamp', 'average_conversion_rate'])
                st.subheader("⏱️ Avg Conversion Rate (Last 60 Seconds)")
                line = alt.Chart(avg_df).mark_line(point=True).encode(
                    x=alt.X('timestamp:T', title='Time'),
                    y=alt.Y('average_conversion_rate:Q', title='Avg Conversion Rate (%)'),
                    tooltip=['timestamp:T', 'average_conversion_rate:Q']
                ).properties(height=300)
                st.altair_chart(line, use_container_width=True)
            else:
                st.warning("⚠️ No recent conversion rate data.")
        else:
            st.info("ℹ️ No historical conversion rate data yet.")
 
    time.sleep(REFRESH_INTERVAL)
