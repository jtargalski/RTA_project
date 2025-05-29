import streamlit as st
from pymongo import MongoClient
import pandas as pd
import time
import altair as alt
from datetime import datetime, timedelta

# MongoDB setup
mongo_client = MongoClient('mongodb://root:admin@mongo:27017/')
db = mongo_client['social_media_analytics']
collection = db['post_stats']
history_collection = db['conversion_rate_history']

st.set_page_config(page_title="Social Media Analytics Dashboard", layout="wide")
st.title("📊 Social Media Posts Analytics Dashboard")

REFRESH_INTERVAL = 5  # seconds
placeholder = st.empty()

# Initialize session state for real-time conversion history
if "conversion_history" not in st.session_state:
    st.session_state.conversion_history = []

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

while True:
    with placeholder.container():
        df = load_current_stats()

        if df.empty:
            st.info("Waiting for data...")
        else:
            df = df.sort_values(by='impressions', ascending=False)
            df['conversion_rate'] = (df['conversion_rate'] * 100).round(2)

            st.dataframe(df)

            # Helper to plot bar chart with average line
            def bar_chart_with_avg(series, title, y_label):
                avg = series.mean()
                df_chart = pd.DataFrame({
                    'post_id': series.index,
                    y_label: series.values
                })
                base = alt.Chart(df_chart).mark_bar().encode(
                    x=alt.X('post_id:N', title='Post ID'),
                    y=alt.Y(f'{y_label}:Q', title=y_label)
                )
                rule = alt.Chart(pd.DataFrame({'avg': [avg]})).mark_rule(color='red', strokeDash=[4, 4]).encode(
                    y=f'avg:Q'
                )
                st.altair_chart(base + rule, use_container_width=True)

            st.subheader("📢 Impressions per Post")
            bar_chart_with_avg(df.set_index('post_id')['impressions'], "Impressions", "Impressions")

            st.subheader("💬 Engagements per Post")
            bar_chart_with_avg(df.set_index('post_id')['engagements'], "Engagements", "Engagements")

            st.subheader("🛒 Conversions per Post")
            bar_chart_with_avg(df.set_index('post_id')['conversions'], "Conversions", "Conversions")

            st.subheader("📈 Conversion Rate (%) per Post")
            bar_chart_with_avg(df.set_index('post_id')['conversion_rate'], "Conversion Rate (%)", "Conversion Rate (%)")

        # Live-updating line chart of avg conversion rate over last 60 seconds
        hist_df = load_conversion_history()
        now = datetime.utcnow()
        one_minute_ago = now - timedelta(seconds=60)

        if not hist_df.empty:
            recent_df = hist_df[hist_df['timestamp'] >= one_minute_ago]

            # Add new unique points to session state
            for _, row in recent_df.iterrows():
                point = (row['timestamp'], row['average_conversion_rate'])
                if point not in st.session_state.conversion_history:
                    st.session_state.conversion_history.append(point)

            # Keep only points from last 60 seconds
            st.session_state.conversion_history = [
                (ts, val) for ts, val in st.session_state.conversion_history if ts >= one_minute_ago
            ]

            if st.session_state.conversion_history:
                avg_df = pd.DataFrame(st.session_state.conversion_history, columns=['timestamp', 'average_conversion_rate'])

                st.subheader("⏱️ Avg Conversion Rate (Last 60 Seconds)")
                line = alt.Chart(avg_df).mark_line(point=True).encode(
                    x=alt.X('timestamp:T', title='Time'),
                    y=alt.Y('average_conversion_rate:Q', title='Avg Conversion Rate (%)'),
                    tooltip=['timestamp:T', 'average_conversion_rate:Q']
                ).properties(
                    width='container',
                    height=300
                )
                st.altair_chart(line, use_container_width=True)
            else:
                st.warning("No recent conversion rate data.")
        else:
            st.info("No historical conversion rate data yet.")

    time.sleep(REFRESH_INTERVAL)
