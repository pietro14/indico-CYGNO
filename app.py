"""
CYGNO Indico Dashboard — Streamlit app
Visualize, filter, and analyze CYGNO collaboration meetings.
"""

import os
import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

from scraper import DB_PATH, DEFAULT_START_URL, init_db, get_meta, scrape_events

# --- Page config ---
st.set_page_config(
    page_title="CYGNO Meetings Dashboard",
    page_icon="🔬",
    layout="wide",
)


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
    return df


def load_meeting_count():
    if not os.path.exists(DB_PATH):
        return 0
    conn = sqlite3.connect(DB_PATH)
    count = conn.execute("SELECT COUNT(*) FROM meetings").fetchone()[0]
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


# --- Sidebar ---
st.sidebar.title("CYGNO Meetings")
st.sidebar.markdown("---")

# Update button
if st.sidebar.button("🔄 Update Data", use_container_width=True):
    with st.sidebar.status("Scraping Indico...", expanded=True) as status:
        info = st.sidebar.empty()

        def progress_cb(msg, count):
            info.text(f"{msg}\n({count} new events so far)")

        try:
            init_db(DB_PATH)
            new_count = scrape_events(
                db_path=DB_PATH,
                start_url=DEFAULT_START_URL,
                progress_callback=progress_cb,
            )
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
st.sidebar.metric("Total meetings", meeting_count)

st.sidebar.markdown("---")
page = st.sidebar.radio(
    "Navigate",
    ["📋 Meeting Browser", "📊 Analytics", "👤 Speakers", "🏛️ Institutions"],
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
        col1, col2 = st.columns(2)
        with col1:
            date_min = df["date_parsed"].min()
            date_max = df["date_parsed"].max()
            if pd.notna(date_min) and pd.notna(date_max):
                date_range = st.date_input(
                    "Date range",
                    value=(date_min.date(), date_max.date()),
                    min_value=date_min.date(),
                    max_value=date_max.date(),
                )
            else:
                date_range = None
        with col2:
            keyword = st.text_input("Keyword search (meeting or contribution title)")

        col3, col4 = st.columns(2)
        with col3:
            speaker_search = st.text_input("Speaker name")
        with col4:
            institutions = sorted(df["institution"].dropna().unique())
            inst_filter = st.multiselect("Institution", institutions)

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

    st.caption(f"{len(filtered)} contributions across {filtered['agenda'].nunique()} meetings")

    display_df = filtered[["date", "meeting", "contribution", "speaker", "institution", "agenda", "pdf"]].copy()
    display_df = display_df.rename(columns={
        "date": "Date",
        "meeting": "Meeting",
        "contribution": "Contribution",
        "speaker": "Speaker",
        "institution": "Institution",
        "agenda": "Agenda",
        "pdf": "PDF",
    })

    st.dataframe(
        display_df,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="Download"),
        },
        use_container_width=True,
        hide_index=True,
        height=600,
    )


# ====================================================================
# PAGE: Analytics
# ====================================================================
elif page == "📊 Analytics":
    st.header("Analytics")

    df_valid = df.dropna(subset=["date_parsed"]).copy()
    df_valid["month"] = df_valid["date_parsed"].dt.to_period("M").dt.to_timestamp()
    df_valid["year"] = df_valid["date_parsed"].dt.year

    # --- Row 1: Meetings over time + Activity trends ---
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
        st.subheader("Activity trends")
        granularity = st.radio("Group by", ["Month", "Year"], horizontal=True, key="trend_gran")
        if granularity == "Month":
            trend = df_valid.groupby("month").size().reset_index(name="contributions")
            fig = px.line(trend, x="month", y="contributions", labels={"month": "", "contributions": "Contributions"})
        else:
            trend = df_valid.groupby("year").size().reset_index(name="contributions")
            fig = px.line(trend, x="year", y="contributions", labels={"year": "", "contributions": "Contributions"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 2: Top speakers + Institution breakdown ---
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Top 20 speakers")
        speakers = (
            df[df["speaker"].notna() & (df["speaker"] != "N/A")]
            .groupby("speaker")
            .size()
            .nlargest(20)
            .reset_index(name="contributions")
        )
        fig = px.bar(speakers, x="contributions", y="speaker", orientation="h",
                     labels={"speaker": "", "contributions": "Contributions"})
        fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10), height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Institution breakdown")
        insts = (
            df[df["institution"].notna() & (df["institution"] != "N/A")]
            .groupby("institution")
            .size()
            .reset_index(name="contributions")
            .sort_values("contributions", ascending=False)
        )
        fig = px.pie(insts.head(15), values="contributions", names="institution",
                     hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10), height=500)
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 3: Contributions per meeting + PDF availability ---
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("Contributions per meeting")
        contribs_per_meeting = (
            df.groupby(["agenda", "meeting", "date_parsed"])
            .size()
            .reset_index(name="contributions")
            .sort_values("date_parsed")
        )
        fig = px.bar(contribs_per_meeting, x="date_parsed", y="contributions",
                     hover_data=["meeting"],
                     labels={"date_parsed": "", "contributions": "Contributions"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col6:
        st.subheader("PDF availability")
        has_pdf = (df["pdf"] != "no PDF") & (df["pdf"].str.len() > 0)
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
# PAGE: Speaker Profiles
# ====================================================================
elif page == "👤 Speakers":
    st.header("Speaker Profiles")

    speakers_list = sorted(
        df[df["speaker"].notna() & (df["speaker"] != "N/A")]["speaker"].unique()
    )
    if not speakers_list:
        st.info("No speaker data available.")
        st.stop()

    selected_speaker = st.selectbox("Select a speaker", speakers_list)
    speaker_df = df[df["speaker"] == selected_speaker].copy()

    # Stats
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

    # Contributions table
    st.subheader("Contributions")
    display = speaker_df[["date", "meeting", "contribution", "institution", "agenda", "pdf"]].rename(columns={
        "date": "Date", "meeting": "Meeting", "contribution": "Contribution",
        "institution": "Institution", "agenda": "Agenda", "pdf": "PDF",
    })
    st.dataframe(
        display,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="Download"),
        },
        use_container_width=True,
        hide_index=True,
    )

    # Timeline
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

    inst_list = sorted(
        df[df["institution"].notna() & (df["institution"] != "N/A")]["institution"].unique()
    )
    if not inst_list:
        st.info("No institution data available.")
        st.stop()

    selected_inst = st.selectbox("Select an institution", inst_list)
    inst_df = df[df["institution"] == selected_inst].copy()

    # Stats
    col1, col2, col3 = st.columns(3)
    col1.metric("Total contributions", len(inst_df))
    col2.metric("Meetings", inst_df["agenda"].nunique())
    col3.metric("Speakers", inst_df["speaker"].nunique())

    # Per-speaker breakdown
    st.subheader("Speakers from this institution")
    by_speaker = (
        inst_df[inst_df["speaker"] != "N/A"]
        .groupby("speaker")
        .size()
        .reset_index(name="contributions")
        .sort_values("contributions", ascending=False)
    )
    fig = px.bar(by_speaker, x="contributions", y="speaker", orientation="h",
                 labels={"speaker": "", "contributions": "Contributions"})
    fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # Contributions table
    st.subheader("All contributions")
    display = inst_df[["date", "meeting", "contribution", "speaker", "agenda", "pdf"]].rename(columns={
        "date": "Date", "meeting": "Meeting", "contribution": "Contribution",
        "speaker": "Speaker", "agenda": "Agenda", "pdf": "PDF",
    })
    st.dataframe(
        display,
        column_config={
            "Agenda": st.column_config.LinkColumn("Agenda", display_text="Open"),
            "PDF": st.column_config.LinkColumn("PDF", display_text="Download"),
        },
        use_container_width=True,
        hide_index=True,
    )

    # Activity timeline
    if inst_df["date_parsed"].notna().any():
        st.subheader("Activity timeline")
        timeline = inst_df.dropna(subset=["date_parsed"]).copy()
        timeline["month"] = timeline["date_parsed"].dt.to_period("M").dt.to_timestamp()
        monthly = timeline.groupby("month").size().reset_index(name="contributions")
        fig = px.line(monthly, x="month", y="contributions",
                      labels={"month": "", "contributions": "Contributions"}, markers=True)
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)
