"""
CYGNO Indico Dashboard — Streamlit app
Visualize, filter, and analyze CYGNO collaboration meetings.
"""

import os
import sqlite3
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from scraper import DB_PATH, init_db, get_meta, scrape_events

# --- Page config ---
st.set_page_config(
    page_title="CYGNO Meetings Dashboard",
    page_icon="🔬",
    layout="wide",
)

# --- Institution normalization map ---
INSTITUTION_MAP = {
    # GSSI & INFN LNGS
    "GSSI & INFN": "GSSI & INFN LNGS",
    "GSSI and INFN LNGS": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute / INFN": "GSSI & INFN LNGS",
    "Gran Sasso Science Institute / Istituto Nazionale di Fisica Nucleare LNGS": "GSSI & INFN LNGS",
    "Istituto Nazionale di Fisica Nucleare, GSSI": "GSSI & INFN LNGS",
    # INFN (generic)
    "Istituto Nazionale di Fisica Nucleare": "INFN",
    # INFN LNF
    "INFN - LNF": "INFN - LNF (Frascati)",
    "LNF": "INFN - LNF (Frascati)",
    "Laboratori Nazionali di Frascati": "INFN - LNF (Frascati)",
    # Sapienza & INFN Roma
    "INFN Roma": "Sapienza & INFN Roma",
    "RM1": "Sapienza & INFN Roma",
    "ROMA1": "Sapienza & INFN Roma",
    "Sapienza": "Sapienza & INFN Roma",
    "Sapienza & INFN Roma": "Sapienza & INFN Roma",
    "Sapienza Università di Roma": "Sapienza & INFN Roma",
    "Sapienza Università di Roma, INFN Roma1": "Sapienza & INFN Roma",
    "La Sapienza Università di Roma": "Sapienza & INFN Roma",
    # Roma Tre
    "ROMA3": "Università Roma Tre & INFN",
    "Roma Tre University, INFN Roma Tre": "Università Roma Tre & INFN",
    "Università Roma Tre": "Università Roma Tre & INFN",
    "Università degli Studi Roma Tre": "Università Roma Tre & INFN",
    # University of Coimbra / LIP
    "Universidade de Coimbra": "University of Coimbra",
    "LIBPhys-UC, Department of Physics, University of Coimbra": "University of Coimbra",
    "Laboratório de Instrumentação e Física Experimental de Partículas": "LIP",
    "Laboratory for Instrumentation, Biomedical Engineering and Radiation Physics": "LIP",
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
        # Filter out future events
        now = pd.Timestamp.now()
        df = df[df["date_parsed"] <= now].copy()
        # Normalize institutions
        df["institution"] = df["institution"].apply(normalize_institution)
        # Clean PDF column: replace "no PDF" and empty strings with None for proper link rendering
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


# --- Sidebar ---
st.sidebar.title("CYGNO Meetings")
st.sidebar.markdown("---")

# Update button
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

        col3, col4, col5 = st.columns(3)
        with col3:
            speaker_search = st.text_input("Speaker name")
        with col4:
            institutions = sorted(df["institution"].dropna().unique())
            inst_filter = st.multiselect("Institution", institutions)
        with col5:
            categories = sorted(df["category"].dropna().unique())
            cat_filter = st.multiselect("Category", categories)

        only_pdf = st.checkbox("Show only contributions with PDF", key="browser_pdf")

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

    display_df = filtered[["date", "category", "meeting", "contribution", "speaker", "institution", "agenda", "pdf"]].copy()
    display_df = display_df.rename(columns={
        "date": "Date",
        "category": "Category",
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
    df_valid["day_of_week"] = df_valid["date_parsed"].dt.day_name()
    df_valid["month_of_year"] = df_valid["date_parsed"].dt.month_name()
    df_valid["month_num"] = df_valid["date_parsed"].dt.month

    # --- Row 1: Meetings over time + Meetings by category ---
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
        st.subheader("Meetings by category")
        cat_counts = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("category")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        fig = px.pie(cat_counts, values="count", names="category", hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 2: Day of week + Month of year distributions ---
    col3, col4 = st.columns(2)

    with col3:
        st.subheader("Meetings by day of the week")
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        meetings_by_dow = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("day_of_week")
            .size()
            .reindex(day_order, fill_value=0)
            .reset_index(name="count")
        )
        meetings_by_dow.columns = ["day", "count"]
        fig = px.bar(meetings_by_dow, x="day", y="count",
                     labels={"day": "", "count": "Meetings"},
                     color="count", color_continuous_scale="Blues")
        fig.update_layout(margin=dict(t=10, b=10), showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        st.subheader("Meetings by month of the year")
        month_order = ["January", "February", "March", "April", "May", "June",
                       "July", "August", "September", "October", "November", "December"]
        meetings_by_moy = (
            df_valid.drop_duplicates(subset=["agenda"])
            .groupby("month_of_year")
            .size()
            .reindex(month_order, fill_value=0)
            .reset_index(name="count")
        )
        meetings_by_moy.columns = ["month_name", "count"]
        fig = px.bar(meetings_by_moy, x="month_name", y="count",
                     labels={"month_name": "", "count": "Meetings"},
                     color="count", color_continuous_scale="Oranges")
        fig.update_layout(margin=dict(t=10, b=10), showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 3: Activity heatmap (month x year) ---
    st.subheader("Activity heatmap (meetings per month/year)")
    meetings_unique = df_valid.drop_duplicates(subset=["agenda"]).copy()
    heatmap_data = (
        meetings_unique
        .groupby(["year", "month_num"])
        .size()
        .reset_index(name="count")
    )
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    heatmap_pivot = heatmap_data.pivot(index="year", columns="month_num", values="count").fillna(0)
    heatmap_pivot.columns = [month_labels[int(c) - 1] for c in heatmap_pivot.columns]
    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Month", y="Year", color="Meetings"),
        color_continuous_scale="YlOrRd",
        aspect="auto",
    )
    fig.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

    # --- Row 4: Activity trends + Contributions per meeting ---
    col5, col6 = st.columns(2)

    with col5:
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

    with col6:
        st.subheader("Contributions per meeting")
        contribs_per_meeting = (
            df_valid.groupby(["agenda", "meeting", "date_parsed"])
            .size()
            .reset_index(name="contributions")
            .sort_values("date_parsed")
        )
        fig = px.bar(contribs_per_meeting, x="date_parsed", y="contributions",
                     hover_data=["meeting"],
                     labels={"date_parsed": "", "contributions": "Contributions"})
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 5: Top speakers + Institution breakdown ---
    col7, col8 = st.columns(2)

    with col7:
        st.subheader("Top 20 speakers")
        speakers = (
            df[df["speaker"].notna() & (df["speaker"] != "N/A") & (df["speaker"] != "")]
            .groupby("speaker")
            .size()
            .nlargest(20)
            .reset_index(name="contributions")
        )
        fig = px.bar(speakers, x="contributions", y="speaker", orientation="h",
                     labels={"speaker": "", "contributions": "Contributions"})
        fig.update_layout(yaxis=dict(autorange="reversed"), margin=dict(t=10, b=10), height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col8:
        st.subheader("Institution breakdown")
        insts = (
            df[df["institution"].notna() & (df["institution"] != "N/A") & (df["institution"] != "")]
            .groupby("institution")
            .size()
            .reset_index(name="contributions")
            .sort_values("contributions", ascending=False)
        )
        fig = px.pie(insts.head(15), values="contributions", names="institution",
                     hole=0.3)
        fig.update_layout(margin=dict(t=10, b=10), height=500)
        st.plotly_chart(fig, use_container_width=True)

    # --- Row 6: PDF availability ---
    st.subheader("PDF availability")
    has_pdf = df["pdf"].notna()
    pdf_stats = pd.DataFrame({
        "status": ["Has PDF", "No PDF"],
        "count": [has_pdf.sum(), (~has_pdf).sum()],
    })
    col9, col10 = st.columns([1, 2])
    with col9:
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

    valid_speakers = df[df["speaker"].notna() & (df["speaker"] != "N/A") & (df["speaker"] != "")]

    if valid_speakers.empty:
        st.info("No speaker data available.")
        st.stop()

    # Default to the speaker with most contributions
    speaker_counts = valid_speakers.groupby("speaker").size().sort_values(ascending=False)
    speakers_list = speaker_counts.index.tolist()
    default_speaker = speakers_list[0]

    selected_speaker = st.selectbox("Select a speaker", speakers_list, index=0)
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
    only_pdf_speaker = st.checkbox("Show only contributions with PDF", key="speaker_pdf")
    display_speaker = speaker_df.copy()
    if only_pdf_speaker:
        display_speaker = display_speaker[display_speaker["pdf"].notna()]
    display = display_speaker[["date", "category", "meeting", "contribution", "institution", "agenda", "pdf"]].rename(columns={
        "date": "Date", "category": "Category", "meeting": "Meeting", "contribution": "Contribution",
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

    valid_insts = df[df["institution"].notna() & (df["institution"] != "N/A") & (df["institution"] != "")]
    if valid_insts.empty:
        st.info("No institution data available.")
        st.stop()

    # Default to institution with most contributions
    inst_counts = valid_insts.groupby("institution").size().sort_values(ascending=False)
    inst_list = inst_counts.index.tolist()

    selected_inst = st.selectbox("Select an institution", inst_list, index=0)
    inst_df = df[df["institution"] == selected_inst].copy()

    # Stats
    col1, col2, col3 = st.columns(3)
    col1.metric("Total contributions", len(inst_df))
    col2.metric("Meetings", inst_df["agenda"].nunique())
    col3.metric("Speakers", inst_df["speaker"].nunique())

    # Per-speaker breakdown
    st.subheader("Speakers from this institution")
    by_speaker = (
        inst_df[(inst_df["speaker"] != "N/A") & (inst_df["speaker"] != "")]
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
    only_pdf_inst = st.checkbox("Show only contributions with PDF", key="inst_pdf")
    display_inst = inst_df.copy()
    if only_pdf_inst:
        display_inst = display_inst[display_inst["pdf"].notna()]
    display = display_inst[["date", "category", "meeting", "contribution", "speaker", "agenda", "pdf"]].rename(columns={
        "date": "Date", "category": "Category", "meeting": "Meeting", "contribution": "Contribution",
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
