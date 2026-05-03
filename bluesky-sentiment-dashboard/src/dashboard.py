import os
import shutil
from datetime import datetime, timedelta, timezone

import duckdb
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from src.config import DUCKDB_PATH, BRAND_KEYWORDS

SNAPSHOT_PATH = f"{DUCKDB_PATH}.snapshot"

st.set_page_config(
    page_title="Bluesky Sentiment Monitor",
    page_icon="○",
    layout="wide",
)

st.markdown("""
<style>
  .stApp { background-color: #F5F5F7; }
  body, .stMarkdown, .stMetric {
    font-family: -apple-system, BlinkMacSystemFont, "San Francisco",
                 "Helvetica Neue", sans-serif;
  }
  div[data-testid="metric-container"] {
    background-color: #FFFFFF;
    border: 1px solid #E5E5E9;
    border-radius: 12px;
    padding: 16px;
    box-sizing: border-box;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  div[data-testid="stPlotlyChart"] {
    background-color: #FFFFFF;
    border: 1px solid #E5E5E9;
    border-radius: 12px;
    padding: 8px;
    box-sizing: border-box;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.12);
  }
  div[data-testid="stPlotlyChart"] > div,
  div[data-testid="stPlotlyChart"] .js-plotly-plot,
  div[data-testid="stPlotlyChart"] .plot-container {
    width: 100% !important;
    max-width: 100% !important;
  }
  h1 { font-weight: 700; color: #1D1D1F; }
  h2, h3 { font-weight: 600; color: #1D1D1F; }
</style>
""", unsafe_allow_html=True)

COLORS = {"positive": "#34C759", "neutral": "#8E8E93", "negative": "#FF3B30"}

CHART_LAYOUT = dict(
    paper_bgcolor="#FFFFFF", plot_bgcolor="#FFFFFF",
    font=dict(family="-apple-system, BlinkMacSystemFont, sans-serif",
              color="#1D1D1F", size=13),
    xaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    yaxis=dict(gridcolor="#E5E5E9", linecolor="#E5E5E9"),
    margin=dict(l=48, r=16, t=56, b=56),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right", x=1,
        bgcolor="rgba(0,0,0,0)",
    ),
)


def open_snapshot():
    if os.path.exists(DUCKDB_PATH):
        shutil.copy(DUCKDB_PATH, SNAPSHOT_PATH)
        wal_src = f"{DUCKDB_PATH}.wal"
        wal_dst = f"{SNAPSHOT_PATH}.wal"
        if os.path.exists(wal_src):
            shutil.copy(wal_src, wal_dst)
        elif os.path.exists(wal_dst):
            os.remove(wal_dst)
    db = duckdb.connect(SNAPSHOT_PATH)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_events (
            uri              VARCHAR PRIMARY KEY,
            cid              VARCHAR,
            did              VARCHAR,
            text             VARCHAR,
            langs            VARCHAR[],
            created_at       TIMESTAMP,
            time_us          BIGINT,
            sentiment_label  VARCHAR,
            sentiment_score  FLOAT,
            sentiment_pos    FLOAT,
            sentiment_neu    FLOAT,
            sentiment_neg    FLOAT,
            ingested_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    return db


def to_bsky_url(uri: str) -> str:
    parts = uri.split("/")
    if len(parts) < 5:
        return ""
    did, rkey = parts[2], parts[-1]
    return f"https://bsky.app/profile/{did}/post/{rkey}"


st.markdown(f"# Bluesky Sentiment Monitor — {', '.join(BRAND_KEYWORDS)}")
window_choice = st.selectbox("Window", ["1 hour", "6 hours", "24 hours", "7 days"], index=2)
window_hours = {"1 hour": 1, "6 hours": 6, "24 hours": 24, "7 days": 168}[window_choice]


SCROLL_RESTORER = """
<script>
  (function() {
    const win = window.parent || window;
    const KEY = 'bluesky-dashboard-scroll';
    const doc = win.document.scrollingElement || win.document.documentElement;

    const saved = win.sessionStorage.getItem(KEY);
    if (saved !== null) {
      const y = parseInt(saved, 10);
      requestAnimationFrame(() => doc.scrollTo({top: y, behavior: 'instant'}));
      setTimeout(() => doc.scrollTo({top: y, behavior: 'instant'}), 50);
      setTimeout(() => doc.scrollTo({top: y, behavior: 'instant'}), 200);
    }

    if (!win.__scrollRestorerInstalled) {
      win.__scrollRestorerInstalled = true;
      win.addEventListener('scroll', () => {
        win.sessionStorage.setItem(KEY, doc.scrollTop);
      }, { passive: true });
    }
  })();
</script>
"""


@st.fragment(run_every="60s")
def render_data():
    components.html(SCROLL_RESTORER, height=0)
    db = open_snapshot()

    def query(sql, params=None):
        return db.execute(sql, params or []).fetchdf()

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=window_hours)
    time_axis = dict(
        range=[since, now],
        tickformat="%b %d %H:%M",
        type="date",
        nticks=6,
        tickangle=0,
    )

    total_in_db = query("SELECT COUNT(*) AS n FROM sentiment_events").iloc[0]["n"]
    print(f"[dashboard] sentiment_events total rows: {total_in_db}, since={since.isoformat()}")
    if total_in_db:
        sample = query("SELECT MIN(created_at) AS mn, MAX(created_at) AS mx FROM sentiment_events").iloc[0]
        print(f"[dashboard]   created_at range: {sample.mn} → {sample.mx}")

    kpi = query("""
        SELECT
            COUNT(*) AS total,
            AVG(sentiment_score) AS avg_score,
            SUM(CASE WHEN sentiment_label='positive' THEN 1 ELSE 0 END) AS pos,
            SUM(CASE WHEN sentiment_label='negative' THEN 1 ELSE 0 END) AS neg
        FROM sentiment_events WHERE created_at >= ?
    """, [since]).iloc[0]

    with st.container(height=120, border=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Posts", f"{int(kpi.total):,}")
        c2.metric("Avg Sentiment", f"{kpi.avg_score:+.2f}" if kpi.total else "—")
        c3.metric("Positive", f"{(kpi.pos / kpi.total * 100):.1f}%" if kpi.total else "—")
        c4.metric("Negative", f"{(kpi.neg / kpi.total * 100):.1f}%" if kpi.total else "—")

    ts = query("""
        SELECT date_trunc('hour', created_at) AS hour,
               AVG(sentiment_score) AS avg_score,
               COUNT(*) AS posts
        FROM sentiment_events WHERE created_at >= ?
        GROUP BY 1 ORDER BY 1
    """, [since])

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts["hour"], y=ts["avg_score"],
        mode="lines+markers", line=dict(color="#0071E3", width=2),
        marker=dict(size=6),
    ))
    fig.update_layout(**CHART_LAYOUT, height=320, title="Sentiment Over Time")
    fig.update_xaxes(**time_axis)
    with st.container(height=360, border=False):
        st.plotly_chart(fig, use_container_width=True)

    col_left, col_right = st.columns(2)

    with col_left:
        bd = query("""
            SELECT date_trunc('hour', created_at) AS hour, sentiment_label, COUNT(*) AS n
            FROM sentiment_events WHERE created_at >= ?
            GROUP BY 1, 2 ORDER BY 1
        """, [since])
        fig2 = go.Figure()
        for label in ["positive", "neutral", "negative"]:
            d = bd[bd["sentiment_label"] == label]
            fig2.add_trace(go.Bar(
                x=d["hour"], y=d["n"], name=label.title(),
                marker_color=COLORS[label],
                width=3_600_000 * 0.8,
            ))
        fig2.update_layout(**CHART_LAYOUT, barmode="stack", height=320,
                           title="Hourly Volume by Sentiment")
        fig2.update_xaxes(**time_axis)
        with st.container(height=360, border=False):
            st.plotly_chart(fig2, use_container_width=True)

    with col_right:
        authors = query("""
            SELECT did, COUNT(*) AS n, AVG(sentiment_score) AS avg_score
            FROM sentiment_events WHERE created_at >= ?
            GROUP BY 1 ORDER BY n DESC LIMIT 8
        """, [since])
        authors["author"] = authors["did"].str.slice(-12)
        fig3 = go.Figure(go.Bar(
            x=authors["n"], y=authors["author"], orientation="h",
            marker_color="#0071E3",
        ))
        fig3.update_layout(**CHART_LAYOUT, height=320, title="Top Authors")
        with st.container(height=360, border=False):
            st.plotly_chart(fig3, use_container_width=True)

    st.markdown("### Recent Posts")
    recent = query("""
        SELECT created_at, did, text, sentiment_label, sentiment_score, uri
        FROM sentiment_events ORDER BY created_at DESC LIMIT 20
    """)
    recent["link"] = recent["uri"].apply(to_bsky_url)
    st.dataframe(
        recent[["created_at", "did", "text", "sentiment_label", "sentiment_score", "link"]],
        use_container_width=True, hide_index=True, height=600,
        column_config={"link": st.column_config.LinkColumn("link")},
    )

    st.caption(f"Refreshes every 1m · last updated {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")

    db.close()


render_data()
