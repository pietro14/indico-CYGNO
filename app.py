"""
CYGNO Indico Dashboard — Streamlit app
Visualize, filter, and analyze CYGNO collaboration meetings.
"""

import io
import os
import sqlite3
from collections import Counter
from datetime import datetime
from itertools import combinations

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from wordcloud import WordCloud

from scraper import DB_PATH, init_db, get_meta, scrape_events

# --- Page config ---
st.set_page_config(
    page_title="CYGNO Meetings Dashboard",
    page_icon="🔬",
    layout="wide",
)

# --- Institution normalization map ---
INSTITUTION_MAP = {
    "GSSI & INFN": "GSSI & INFN LNGS",
    "GSSI and INFN LNGS": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute / INFN": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute / Istituto Nazionale di Fisica Nucleare LNGS": "GSSI & INFN LNGS",
    "Istituto Nazionale di Fisica Nucleare, GSSI": "GSSI & INFN LNGS",
    "Istituto Nazionale di Fisica Nucleare": "INFN",
    "INFN - LNF": "INFN - LNF (Frascati)",
    "LNF": "INFN - LNF (Frascati)",
    "Laboratori Nazionali di Frascati": "INFN - LNF (Frascati)",
    "INFN Roma": "Sapienza & INFN Roma",
    "RM1": "Sapienza & INFN Roma",
    "ROMA1": "Sapienza & INFN Roma",
    "Sapienza": "Sapienza & INFN Roma",
    "Sapienza & INFN Roma": "Sapienza & INFN Roma",
    "Sapienza Università di Roma": "Sapienza & INFN Roma",
    "Sapienza Università di Roma, INFN Roma1": "Sapienza & INFN Roma",
    "La Sapienza Università di Roma": "Sapienza & INFN Roma",
    "ROMA3": "Università Roma Tre & INFN",
    "Roma Tre University, INFN Roma Tre": "Università Roma Tre & INFN",
    "Università Roma Tre": "Università Roma Tre & INFN",
    "Università degli Studi Roma Tre": "Università Roma Tre & INFN",
    "Universidade de Coimbra": "University of Coimbra",
    "LIBPhys-UC, Department of Physics, University of Coimbra": "University of Coimbra",
    "Laboratório de Instrumentação e Física Experimental de Partículas": "LIP",
    "Laboratory for Instrumentation, Biomedical Engineering and Radiation Physics": "LIP",
}

# Stop words for word cloud
STOP_WORDS = {
    "meeting", "meetings", "cygno", "update", "status", "general", "weekly",
    "discussion", "review", "report", "progress", "the", "and", "for", "with",
    "from", "about", "new", "del", "della", "dei", "delle", "per", "con",
    "una", "uno", "sul", "sulla", "di",
}


def normalize_institution(name):
    if pd.isna(name):
        return name
    return INSTITUTION_MAP.get(name, name)


# --- Data loading ---
@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        """
        SELECT
            c.id as contribution_id,
            m.title as meeting,
            m.event_url as agenda,
            m.date,
            m.category,
            c.title as contribution,
            c.speaker,
            c.institution,
            c.pdf_url as pdf
        FROM contributions c
        JOIN meetings m ON c.meeting_id = m.id
        ORDER BY m.date DESC
        """,
        conn,
    )
    conn.close()
    if not df.empty:
        df["date_parsed"] = pd.to_datetime(df["date"], format="%Y-%m-%d %H:%M", errors="coerce")
        now = pd.Timestamp.now()
        df = df[df["date_parsed"] <= now].copy()
        df["institution"] = df["institution"].apply(normalize_institution)
        df["pdf"] = df["pdf"].replace({"no PDF": None, "": None})
    return df


def load_meeting_count():
    if not os.path.exists(DB_PATH):
        return 0
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM meetings WHERE date <= datetime('now', 'localtime')").fetchone()[0]
    conn.close()
    return count


def get_last_update():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    init_db(DB_PATH)
    val = get_meta(conn, "last_scrape_timestamp")
    conn.close()
    return val


def generate_wordcloud(text, width=800, height=400):
    """Generate a word cloud image and return as bytes."""
    wc = WordCloud(
        width=width, height=height, background_color="white",
        stopwords=STOP_WORDS, colormap="viridis",
        max_words=80, min_font_size=10,
    ).generate(text)
    buf = io.BytesIO()
    fig, ax = plt.subplots(figsize=(width / 100, height / 100), dpi=100)
    ax.imshow(wc, interpolation="bilinear")
    ax.axis("off")
    fig.tight_layout(pad=0)
    fig.savefig(buf, format="png", bbox_inches="tight", pad_inches=0)
    plt.close(fig)
    buf.seek(0)
    return buf


def build_collaboration_graph(df_subset, min_shared=2):
    """Build a networkx graph where speakers are connected if they present at the same meeting."""
    valid = df_subset[df_subset["speaker"].notna() & (df_subset["speaker"] != "N/A") & (df_subset["speaker"] != "")]
    meetings_speakers = valid.groupby("agenda")["speaker"].apply(set).to_dict()

    edge_weights = Counter()
    for speakers in meetings_speakers.values():
        if len(speakers) >= 2:
            for s1, s2 in combinations(sorted(speakers), 2):
                edge_weights[(s1, s2)] += 1

    G = nx.Graph()
    for (s1, s2), weight in edge_weights.items():
        if weight >= min_shared:
            G.add_edge(s1, s2, weight=weight)

    # Only keep nodes that have edges
    isolated = list(nx.isolates(G))
    G.remove_nodes_from(isolated)
    return G


def plot_network(G, title=""):
    """Plot a networkx graph using plotly."""
    if len(G.nodes) == 0:
        return None

    pos = nx.spring_layout(G, k=2 / max(len(G.nodes) ** 0.5, 1), iterations=50, seed=42)

    # Edges
    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y, mode="lines",
        line=dict(width=0.8, color="#888"),
        hoverinfo="none",
    )

    # Nodes
    node_x = [pos[n][0] for n in G.nodes()]
    node_y = [pos[n][1] for n in G.nodes()]
    node_degree = [G.degree(n) for n in G.nodes()]
    node_text = [f"{n}<br>Connections: {G.degree(n)}" for n in G.nodes()]

    node_trace = go.Scatter(
        x=node_x, y=node_y, mode="markers+text",
        text=[n if G.degree(n) >= 3 else "" for n in G.nodes()],
        textposition="top center", textfont=dict(size=9),
        hovertext=node_text, hoverinfo="text",
        marker=dict(
            size=[max(8, d * 3) for d in node_degree],
            color=node_degree, colorscale="YlOrRd",
            colorbar=dict(title="Connections", thickness=15),
            line=dict(width=1, color="#333"),
        ),
    )

    fig = go.Figure(data=[edge_trace, node_trace])
    fig.update_layout(
        title=title, showlegend=False,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        margin=dict(t=40, b=10, l=10, r=10),
        height=600,
    )
    return fig


def apply_date_preset(df, preset):
    """Filter dataframe by date preset, return filtered df."""
    now = pd.Timestamp.now()
    if preset == "Last 6 months":
        return df[df["date_parsed"] >= now - pd.DateOffset(months=6)]
    elif preset == "Last year":
        return df[df["date_parsed"] >= now - pd.DateOffset(years=1)]
    elif preset == "Last 2 years":
        return df[df["date_parsed"] >= now - pd.DateOffset(years=2)]
    return df  # "All time"


# --- Sidebar ---
st.sidebar.title("CYGNO Meetings")
st.sidebar.markdown("---")

if st.sidebar.button("🔄 Update Data", use_container_width=True):
    with st.sidebar.status("Scraping Indico...", expanded=True) as status:
        info = st.sidebar.empty()

        def progress_cb(msg, current, total):
            if total > 0:
                info.text(f"{msg}\n({current}/{total})")
            else:
                info.text(msg)

        try:
            init_db(DB_PATH)
            new_count = scrape_events(db_path=DB_PATH, progress_callback=progress_cb)
            status.update(label=f"Done! {new_count} new event(s) added.", state="complete")
        except Exception as e:
            status.update(label=f"Error: {e}", state="error")

    load_data.clear()
    st.rerun()

last_update = get_last_update()
if last_update:
    st.sidebar.caption(f"Last update: {last_update[:19].replace('T', ' ')}")
else:
    st.sidebar.caption("No data yet — click Update Data to start.")

meeting_count = load_meeting_count()
st.sidebar.metric("Total meetings (past)", meeting_count)

st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigate",
    ["📋 Meeting Browser", "📊 Analytics", "🔗 Collaboration Network",
     "☁️ Word Cloud", "👤 Speakers", "🏛️ Institutions"],
)

# --- Load data ---
df = load_data()

if df.empty:
    st.warning("No data available. Click **Update Data** in the sidebar to scrape meetings from Indico.")
    st.stop()


# ====================================================================
# PAGE: Meeting Browser
# ====================================================================
if page == "📋 Meeting Browser":
    st.header("Meeting Browser")

    with st.expander("🔍 Filters", expanded=True):
        # Quick date presets
        preset = st.radio(
            "Quick date range",
            ["All time", "Last 6 months", "Last year", "Last 2 years"],
            horizontal=True, key="browser_preset",
        )

        col1, col2 = st.columns(2)
        with col1:
            filtered_by_preset = apply_date_preset(df, preset)
            date_min = filtered_by_preset["date_parsed"].min()
            date_max = filtered_by_preset["date_parsed"].max()
            if pd.notna(date_min) and pd.notna(date_max):
                date_range = st.date_input(
                    "Date range (fine-tune)",
                    value=(date_min.date(), date_max.date()),
                    min_value=df["date_parsed"].min().date(),
                    max_value=df["date_parsed"].max().date(),
                )
            else:
                date_range = None
        with col2:
            keyword = st.text_input("Keyword search (meeting or contribution title)")

        col3, col4, col5 = st.columns(3)
        with col3:
            speaker_search = st.text_input("Speaker name")
        with col4:
            institutions = sorted(df["institution"].dropna().unique())
            inst_filter = st.multiselect("Institution", institutions)
        with col5:
            categories = sorted(df["category"].dropna().unique())
            cat_filter = st.multiselect("Category", categories)

        only_pdf = st.checkbox("Show only contributions with PDF", value=True, key="browser_pdf")

    filtered = df.copy()

    if date_range and len(date_range) == 2:
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        filtered = filtered[
            (filtered["date_parsed"] >= start) & (filtered["date_parsed"] <= end + pd.Timedelta(days=1))
        ]
    if keyword:
        kw = keyword.lower()
        filtered = filtered[
            filtered["meeting"].str.lower().str.contains(kw, na=False)
            | filtered["contribution"].str.lower().str.contains(kw, na=False)
        ]
    if speaker_search:
        filtered = filtered[
            filtered["speaker"].str.lower().str.contains(speaker_search.lower(), na=False)
        ]
    if inst_filter:
        filtered = filtered[filtered["institution"].isin(inst_filter)]
    if cat_filter:
        filtered = filtered[filtered["category"].isin(cat_filter)]
    if only_pdf:
        filtered = filtered[filtered["pdf"].notna()]

    st.caption(f"{len(filtered)} contributions across {filtered['agenda'].nunique()} meetings")

    # Download button
    col_dl, col_spacer = st.columns([1, 4])
    with col_dl:
        csv_data = filtered[["date", "category", "meeting", "contribution", "speaker", "institution", "agenda", "pdf"]].to_csv(index=False)
        st.download_button("📥 Download CSV", csv_data, "cygno_meetings_filtered.csv", "text/csv", use_container_width=True)

    # Table view
    display_df = filtered[["date", "category", "meeting", "contribution", "speaker", "institution", "agenda", "pdf"]].copy()
    display_df = display_df.rename(columns={
        "date": "Date", "category": "Category", "meeting": "Meeting",
        "contribution": "Contribution", "speaker": "Speaker",
        "institution": "Institution", "agenda": "Agenda", "pdf": "PDF",
    })

    st.dataframe(
        display_df,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="📄 View"),
        },
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    # Meeting detail view
    st.markdown("---")
    st.subheader("Meeting Detail View")
    meeting_list = filtered.drop_duplicates(subset=["agenda"]).sort_values("date_parsed", ascending=False)
    meeting_options = {f"{row['date'][:10]} — {row['meeting']}": row["agenda"] for _, row in meeting_list.iterrows()}

    if meeting_options:
        selected_meeting_label = st.selectbox("Select a meeting", list(meeting_options.keys()))
        selected_agenda = meeting_options[selected_meeting_label]
        meeting_data = filtered[filtered["agenda"] == selected_agenda]

        meeting_info = meeting_data.iloc[0]
        st.markdown(f"**{meeting_info['meeting']}** | {meeting_info['date']} | Category: {meeting_info['category']}")
        st.markdown(f"[Open agenda]({selected_agenda})")

        for _, row in meeting_data.iterrows():
            if row["contribution"]:
                with st.container(border=True):
                    cols = st.columns([3, 2, 2, 1])
                    cols[0].markdown(f"**{row['contribution']}**")
                    cols[1].write(row["speaker"] if row["speaker"] else "—")
                    cols[2].write(row["institution"] if row["institution"] else "—")
                    if pd.notna(row["pdf"]):
                        cols[3].markdown(f"[📄 PDF]({row['pdf']})")


# ====================================================================
# PAGE: Analytics
# ====================================================================
elif page == "📊 Analytics":
    st.header("Analytics")

    df_valid = df.dropna(subset=["date_parsed"]).copy()
    df_valid["month"] = df_valid["date_parsed"].dt.to_period("M").dt.to_timestamp()
    df_valid["year"] = df_valid["date_parsed"].dt.year
    df_valid["day_of_week"] = df_valid["date_parsed"].dt.day_name()
    df_valid["month_of_year"] = df_valid["date_parsed"].dt.month_name()
    df_valid["month_num"] = df_valid["date_parsed"].dt.month

    # --- Row 1: Meetings over time + Category breakdown over time ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Meetings over time")
        meetings_by_month = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("month")
            .size()
            .reset_index(name="count")
        )
        fig = px.bar(meetings_by_month, x="month", y="count", labels={"month": "", "count": "Meetings"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Category breakdown over time")
        cat_by_year = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby(["year", "category"])
            .size()
            .reset_index(name="count")
        )
        fig = px.bar(cat_by_year, x="year", y="count", color="category",
                     labels={"year": "", "count": "Meetings", "category": "Category"},
                     barmode="stack")
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 2: Meetings by category (pie) + Newcomers chart ---
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Meetings by category")
        cat_counts = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("category").size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        fig = px.pie(cat_counts, values="count", names="category", hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("New speakers over time")
        valid_spk = df_valid[df_valid["speaker"].notna() & (df_valid["speaker"] != "N/A") & (df_valid["speaker"] != "")]
        first_appearance = valid_spk.groupby("speaker")["date_parsed"].min().reset_index()
        first_appearance["year"] = first_appearance["date_parsed"].dt.year
        newcomers = first_appearance.groupby("year").size().reset_index(name="new_speakers")
        newcomers["cumulative"] = newcomers["new_speakers"].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Bar(x=newcomers["year"], y=newcomers["new_speakers"], name="New speakers", marker_color="#3498db"))
        fig.add_trace(go.Scatter(x=newcomers["year"], y=newcomers["cumulative"], name="Cumulative", mode="lines+markers", yaxis="y2", marker_color="#e74c3c"))
        fig.update_layout(
            yaxis=dict(title="New speakers / year"),
            yaxis2=dict(title="Total speakers", overlaying="y", side="right"),
            margin=dict(t=10, b=10), legend=dict(orientation="h", y=1.1),
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 3: Day of week + Month of year ---
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Meetings by day of the week")
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        meetings_by_dow = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("day_of_week").size()
            .reindex(day_order, fill_value=0)
            .reset_index(name="count")
        )
        meetings_by_dow.columns = ["day", "count"]
        fig = px.bar(meetings_by_dow, x="day", y="count",
                     labels={"day": "", "count": "Meetings"},
                     color="count", color_continuous_scale="Blues")
        fig.update_layout(margin=dict(t=10, b=10), showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        st.subheader("Meetings by month of the year")
        month_order = ["January", "February", "March", "April", "May", "June",
                       "July", "August", "September", "October", "November", "December"]
        meetings_by_moy = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("month_of_year").size()
            .reindex(month_order, fill_value=0)
            .reset_index(name="count")
        )
        meetings_by_moy.columns = ["month_name", "count"]
        fig = px.bar(meetings_by_moy, x="month_name", y="count",
                     labels={"month_name": "", "count": "Meetings"},
                     color="count", color_continuous_scale="Oranges")
        fig.update_layout(margin=dict(t=10, b=10), showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 4: Activity heatmap ---
    st.subheader("Activity heatmap (meetings per month/year)")
    meetings_unique = df_valid.drop_duplicates(subset=["agenda"]).copy()
    heatmap_data = meetings_unique.groupby(["year", "month_num"]).size().reset_index(name="count")
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    heatmap_pivot = heatmap_data.pivot(index="year", columns="month_num", values="count").fillna(0)
    heatmap_pivot.columns = [month_labels[int(c) - 1] for c in heatmap_pivot.columns]
    fig = px.imshow(heatmap_pivot, labels=dict(x="Month", y="Year", color="Meetings"),
                    color_continuous_scale="YlOrRd", aspect="auto")
    fig.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # --- Row 5: Top speakers + Institution breakdown ---
    col7, col8 = st.columns(2)

    with col7:
        st.subheader("Top 20 speakers")
        speakers_data = (
            df[df["speaker"].notna() & (df["speaker"] != "N/A") & (df["speaker"] != "")]
            .groupby("speaker").size().nlargest(20)
            .reset_index(name="contributions")
        )
        fig = px.bar(speakers_data, x="contributions", y="speaker", orientation="h",
                     labels={"speaker": "", "contributions": "Contributions"})
        fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10), height=500)

        event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="top_speakers_chart")
        if event and event.selection and event.selection.points:
            clicked_idx = event.selection.points[0]["point_index"]
            clicked_speaker = speakers_data.iloc[clicked_idx]["speaker"]
            st.info(f"**{clicked_speaker}** — switch to 👤 Speakers page and select this speaker to see their full profile.")

    with col8:
        st.subheader("Institution breakdown")
        insts = (
            df[df["institution"].notna() & (df["institution"] != "N/A") & (df["institution"] != "")]
            .groupby("institution").size()
            .reset_index(name="contributions")
            .sort_values("contributions", ascending=False)
        )
        fig = px.pie(insts.head(15), values="contributions", names="institution", hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10), height=500)
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 6: Contribution trends + PDF availability ---
    col9, col10 = st.columns(2)

    with col9:
        st.subheader("Contribution trends")
        granularity = st.radio("Group by", ["Month", "Year"], horizontal=True, key="trend_gran")
        if granularity == "Month":
            trend = df_valid.groupby("month").size().reset_index(name="contributions")
            fig = px.line(trend, x="month", y="contributions", labels={"month": "", "contributions": "Contributions"})
        else:
            trend = df_valid.groupby("year").size().reset_index(name="contributions")
            fig = px.line(trend, x="year", y="contributions", labels={"year": "", "contributions": "Contributions"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col10:
        st.subheader("PDF availability")
        has_pdf = df["pdf"].notna()
        pdf_stats = pd.DataFrame({
            "status": ["Has PDF", "No PDF"],
            "count": [has_pdf.sum(), (~has_pdf).sum()],
        })
        fig = px.pie(pdf_stats, values="count", names="status",
                     color="status", color_discrete_map={"Has PDF": "#2ecc71", "No PDF": "#e74c3c"},
                     hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# PAGE: Collaboration Network
# ====================================================================
elif page == "🔗 Collaboration Network":
    st.header("Speaker Collaboration Network")
    st.caption("Speakers are connected when they present at the same meeting. Larger nodes = more connections.")

    df_valid = df.dropna(subset=["date_parsed"]).copy()
    df_valid["year"] = df_valid["date_parsed"].dt.year
    years = sorted(df_valid["year"].unique())

    col1, col2 = st.columns([1, 3])
    with col1:
        min_shared = st.slider("Minimum shared meetings", 1, 10, 2, key="net_min")
    with col2:
        view_mode = st.radio("View", ["All time", "By year"], horizontal=True, key="net_mode")

    if view_mode == "All time":
        G = build_collaboration_graph(df_valid, min_shared=min_shared)
        fig = plot_network(G, title=f"All time (min. {min_shared} shared meetings)")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{G.number_of_nodes()} speakers, {G.number_of_edges()} connections")
        else:
            st.info("No connections found with current settings. Try lowering the minimum shared meetings.")
    else:
        selected_year = st.select_slider("Year", options=years, value=years[-1], key="net_year")
        year_df = df_valid[df_valid["year"] == selected_year]
        G = build_collaboration_graph(year_df, min_shared=max(1, min_shared // 2))
        fig = plot_network(G, title=f"Year {selected_year} (min. {max(1, min_shared // 2)} shared meetings)")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            st.caption(f"{G.number_of_nodes()} speakers, {G.number_of_edges()} connections")
        else:
            st.info(f"No connections found for {selected_year}. Try adjusting the settings.")

    # Network evolution stats
    st.markdown("---")
    st.subheader("Network evolution")
    evolution = []
    for y in years:
        year_df = df_valid[df_valid["year"] == y]
        G_y = build_collaboration_graph(year_df, min_shared=1)
        evolution.append({
            "year": y,
            "speakers": G_y.number_of_nodes(),
            "connections": G_y.number_of_edges(),
            "avg_connections": round(sum(dict(G_y.degree()).values()) / max(G_y.number_of_nodes(), 1), 1),
        })
    evo_df = pd.DataFrame(evolution)

    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(evo_df, x="year", y="speakers", markers=True,
                      labels={"year": "", "speakers": "Active speakers"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(evo_df, x="year", y="avg_connections", markers=True,
                      labels={"year": "", "avg_connections": "Avg connections per speaker"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# PAGE: Word Cloud
# ====================================================================
elif page == "☁️ Word Cloud":
    st.header("Word Cloud — Contribution Topics")

    df_valid = df.dropna(subset=["date_parsed"]).copy()
    df_valid["year"] = df_valid["date_parsed"].dt.year
    years = sorted(df_valid["year"].unique())

    # All-time word cloud
    all_titles = df_valid[df_valid["contribution"].notna() & (df_valid["contribution"] != "")]
    all_text = " ".join(all_titles["contribution"].tolist())

    if all_text.strip():
        st.subheader("All time")
        buf = generate_wordcloud(all_text)
        col_wc, _ = st.columns([2, 1])
        with col_wc:
            st.image(buf)
    else:
        st.info("No contribution titles available for word cloud.")

    # Word cloud evolution over time
    st.markdown("---")
    st.subheader("Evolution over time")

    selected_year = st.select_slider("Select year", options=years, value=years[-1], key="wc_year")
    year_titles = df_valid[(df_valid["year"] == selected_year) & df_valid["contribution"].notna() & (df_valid["contribution"] != "")]
    year_text = " ".join(year_titles["contribution"].tolist())

    if year_text.strip():
        buf = generate_wordcloud(year_text)
        col_wc2, _ = st.columns([2, 1])
        with col_wc2:
            st.image(buf)
        st.caption(f"{len(year_titles)} contributions in {selected_year}")
    else:
        st.info(f"No contribution titles available for {selected_year}.")

    # Top terms comparison across years
    st.markdown("---")
    st.subheader("Top terms by year")

    term_data = []
    for y in years:
        yt = df_valid[(df_valid["year"] == y) & df_valid["contribution"].notna() & (df_valid["contribution"] != "")]
        words = []
        for title in yt["contribution"]:
            for w in title.lower().split():
                w_clean = w.strip(".,;:!?()-/")
                if len(w_clean) > 3 and w_clean not in STOP_WORDS:
                    words.append(w_clean)
        top = Counter(words).most_common(10)
        for word, count in top:
            term_data.append({"year": y, "term": word, "count": count})

    if term_data:
        term_df = pd.DataFrame(term_data)
        # Get top 15 terms overall for the heatmap
        top_terms = term_df.groupby("term")["count"].sum().nlargest(15).index.tolist()
        hm_data = term_df[term_df["term"].isin(top_terms)].pivot_table(
            index="term", columns="year", values="count", fill_value=0
        )
        fig = px.imshow(hm_data, labels=dict(x="Year", y="Term", color="Frequency"),
                        color_continuous_scale="Blues", aspect="auto")
        fig.update_layout(margin=dict(t=10, b=10), height=400)
        st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# PAGE: Speaker Profiles
# ====================================================================
elif page == "👤 Speakers":
    st.header("Speaker Profiles")

    valid_speakers = df[df["speaker"].notna() & (df["speaker"] != "N/A") & (df["speaker"] != "")]

    if valid_speakers.empty:
        st.info("No speaker data available.")
        st.stop()

    speaker_counts = valid_speakers.groupby("speaker").size().sort_values(ascending=False)
    speakers_list = speaker_counts.index.tolist()

    selected_speaker = st.selectbox("Select a speaker", speakers_list, index=0)
    speaker_df = df[df["speaker"] == selected_speaker].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Contributions", len(speaker_df))
    col2.metric("Meetings", speaker_df["agenda"].nunique())
    institutions = speaker_df["institution"].unique()
    col3.metric("Institution(s)", ", ".join(i for i in institutions if i and i != "N/A"))

    if speaker_df["date_parsed"].notna().any():
        date_range_str = (
            f"{speaker_df['date_parsed'].min().strftime('%Y-%m-%d')} → "
            f"{speaker_df['date_parsed'].max().strftime('%Y-%m-%d')}"
        )
        st.caption(f"Active period: {date_range_str}")

    st.subheader("Contributions")
    only_pdf_speaker = st.checkbox("Show only contributions with PDF", value=True, key="speaker_pdf")
    display_speaker = speaker_df.copy()
    if only_pdf_speaker:
        display_speaker = display_speaker[display_speaker["pdf"].notna()]

    # Download
    csv_sp = display_speaker[["date", "category", "meeting", "contribution", "institution", "agenda", "pdf"]].to_csv(index=False)
    st.download_button("📥 Download CSV", csv_sp, f"cygno_{selected_speaker.replace(' ', '_')}.csv", "text/csv")

    display = display_speaker[["date", "category", "meeting", "contribution", "institution", "agenda", "pdf"]].rename(columns={
        "date": "Date", "category": "Category", "meeting": "Meeting", "contribution": "Contribution",
        "institution": "Institution", "agenda": "Agenda", "pdf": "PDF",
    })
    st.dataframe(
        display,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="📄 View"),
        },
        use_container_width=True,
        hide_index=True,
    )

    if speaker_df["date_parsed"].notna().any():
        st.subheader("Activity timeline")
        timeline = speaker_df.dropna(subset=["date_parsed"]).copy()
        timeline["month"] = timeline["date_parsed"].dt.to_period("M").dt.to_timestamp()
        monthly = timeline.groupby("month").size().reset_index(name="contributions")
        fig = px.bar(monthly, x="month", y="contributions",
                     labels={"month": "", "contributions": "Contributions"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)


# ====================================================================
# PAGE: Institution Profiles
# ====================================================================
elif page == "🏛️ Institutions":
    st.header("Institution Profiles")

    valid_insts = df[df["institution"].notna() & (df["institution"] != "N/A") & (df["institution"] != "")]
    if valid_insts.empty:
        st.info("No institution data available.")
        st.stop()

    inst_counts = valid_insts.groupby("institution").size().sort_values(ascending=False)
    inst_list = inst_counts.index.tolist()

    selected_inst = st.selectbox("Select an institution", inst_list, index=0)
    inst_df = df[df["institution"] == selected_inst].copy()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total contributions", len(inst_df))
    col2.metric("Meetings", inst_df["agenda"].nunique())
    col3.metric("Speakers", inst_df["speaker"].nunique())

    st.subheader("Speakers from this institution")
    by_speaker = (
        inst_df[(inst_df["speaker"] != "N/A") & (inst_df["speaker"] != "")]
        .groupby("speaker").size()
        .reset_index(name="contributions")
        .sort_values("contributions", ascending=False)
    )
    fig = px.bar(by_speaker, x="contributions", y="speaker", orientation="h",
                 labels={"speaker": "", "contributions": "Contributions"})
    fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("All contributions")
    only_pdf_inst = st.checkbox("Show only contributions with PDF", value=True, key="inst_pdf")
    display_inst = inst_df.copy()
    if only_pdf_inst:
        display_inst = display_inst[display_inst["pdf"].notna()]

    csv_inst = display_inst[["date", "category", "meeting", "contribution", "speaker", "agenda", "pdf"]].to_csv(index=False)
    st.download_button("📥 Download CSV", csv_inst, f"cygno_{selected_inst.replace(' ', '_')}.csv", "text/csv")

    display = display_inst[["date", "category", "meeting", "contribution", "speaker", "agenda", "pdf"]].rename(columns={
        "date": "Date", "category": "Category", "meeting": "Meeting", "contribution": "Contribution",
        "speaker": "Speaker", "agenda": "Agenda", "pdf": "PDF",
    })
    st.dataframe(
        display,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="📄 View"),
        },
        use_container_width=True,
        hide_index=True,
    )

    if inst_df["date_parsed"].notna().any():
        st.subheader("Activity timeline")
        timeline = inst_df.dropna(subset=["date_parsed"]).copy()
        timeline["month"] = timeline["date_parsed"].dt.to_period("M").dt.to_timestamp()
        monthly = timeline.groupby("month").size().reset_index(name="contributions")
        fig = px.line(monthly, x="month", y="contributions",
                      labels={"month": "", "contributions": "Contributions"}, markers=True)
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
