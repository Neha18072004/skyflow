"""SkyFlow dashboard: flight operations analytics on the published marts.* tables.

Reads ONLY the small mart tables (never the fact table), so it runs against the free-tier cloud
Postgres. Connection: st.secrets["DATABASE_URL"] -> env DASHBOARD_DB_URL / CLOUD_DATABASE_URL
-> the local warehouse.
"""

import os
from datetime import timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine

# ---------------------------------------------------------------------------------------------
# Brand palette (matches the Airflow task colours)
# ---------------------------------------------------------------------------------------------
NAVY, NAVY_2 = "#0F1B3D", "#1E3A5F"
AMBER = "#F5A524"
TEAL, TEAL_LIGHT = "#0F766E", "#99D5CD"
RED, RED_LIGHT = "#B4443C", "#E8B4AF"
VIOLET = "#5B4B8A"
SLATE_900, SLATE_700, SLATE_500, SLATE_300, SLATE_200 = "#1E293B", "#334155", "#64748B", "#CBD5E1", "#E2E8F0"
SURFACE, BACKGROUND = "#FFFFFF", "#F7F8FA"
CAUSE_COLORS = {
    "Late aircraft": NAVY_2, "Carrier": VIOLET, "Air traffic system (NAS)": SLATE_500,
    "Weather": AMBER, "Security": RED,
}
DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

st.set_page_config(page_title="SkyFlow · Flight operations analytics", page_icon="✈️", layout="wide")

st.markdown(
    f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
/* Streamlit's own component classes set Source Sans, so Inter needs !important to win; icon glyphs
   (Material Symbols) and code keep their fonts. */
.stApp *:not([data-testid="stIconMaterial"]):not(code):not(.material-symbols-rounded) {{
  font-family: 'Inter', sans-serif !important;
}}
.stApp code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace !important; }}
#MainMenu, footer, [data-testid="stAppDeployButton"], [data-testid="stDecoration"] {{ display: none; }}
header[data-testid="stHeader"] {{ background: transparent; height: 0; }}
.block-container {{ padding: 1.5rem 2.5rem 3rem; max-width: 1500px; }}
section[data-testid="stSidebar"] {{ background: {SURFACE}; border-right: 1px solid {SLATE_200}; }}

.sf-header {{
  background: linear-gradient(120deg, {NAVY} 0%, {NAVY_2} 100%); color: #fff;
  border-radius: 18px; padding: 1.4rem 1.8rem; margin-bottom: 1.4rem;
  display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: .8rem;
  box-shadow: 0 10px 30px rgba(15, 27, 61, .18);
}}
.sf-wordmark {{ font-size: 1.9rem; font-weight: 800; letter-spacing: -.02em; line-height: 1; }}
.sf-wordmark span {{ color: {AMBER}; }}
.sf-tagline {{ color: {SLATE_300}; font-size: .95rem; margin-top: .35rem; }}
.sf-pill {{
  background: rgba(255,255,255,.08); border: 1px solid rgba(255,255,255,.16); color: #E2E8F0;
  border-radius: 999px; padding: .4rem .9rem; font-size: .82rem; white-space: nowrap;
}}
.sf-pill b {{ color: {AMBER}; font-weight: 600; }}

.sf-card {{
  background: {SURFACE}; border: 1px solid {SLATE_200}; border-radius: 16px;
  padding: 1.15rem 1.3rem; box-shadow: 0 1px 2px rgba(15,23,42,.04), 0 6px 18px rgba(15,23,42,.05);
  height: 100%;
}}
.sf-kpi-label {{ color: {SLATE_500}; font-size: .78rem; font-weight: 600; text-transform: uppercase; letter-spacing: .06em; }}
.sf-kpi-value {{ color: {SLATE_900}; font-size: 2rem; font-weight: 700; letter-spacing: -.02em; margin: .3rem 0 .2rem; }}
.sf-delta {{ font-size: .82rem; font-weight: 600; }}
.sf-delta.good {{ color: {TEAL}; }} .sf-delta.bad {{ color: {RED}; }} .sf-delta.flat {{ color: {SLATE_500}; }}
.sf-delta small {{ color: {SLATE_500}; font-weight: 400; }}
.sf-accent {{ border-top: 3px solid {AMBER}; }}

.sf-section {{ color: {SLATE_900}; font-weight: 700; font-size: 1.05rem; margin: 1.6rem 0 .2rem; }}
.sf-caption {{ color: {SLATE_500}; font-size: .85rem; margin-bottom: .6rem; }}

.stTabs [data-baseweb="tab-list"] {{ gap: .4rem; border-bottom: 1px solid {SLATE_200}; }}
.stTabs [data-baseweb="tab"] {{ height: 2.8rem; padding: 0 1rem; font-weight: 600; color: {SLATE_500}; }}
.stTabs [aria-selected="true"] {{ color: {NAVY}; }}
.stTabs [data-baseweb="tab-highlight"] {{ background: {AMBER}; height: 3px; }}

div[data-testid="stPlotlyChart"], div[data-testid="stDataFrame"] {{
  background: {SURFACE}; border: 1px solid {SLATE_200}; border-radius: 16px; padding: .6rem .6rem .2rem;
  box-shadow: 0 1px 2px rgba(15,23,42,.04), 0 6px 18px rgba(15,23,42,.05);
}}

.sf-badge {{ display: inline-block; border-radius: 999px; padding: .15rem .6rem; font-size: .72rem; font-weight: 700; letter-spacing: .04em; }}
.sf-badge.pass {{ background: #E6F4F1; color: {TEAL}; }}
.sf-badge.warn {{ background: #FEF3DC; color: #9A6200; }}
.sf-badge.fail {{ background: #F8E3E1; color: {RED}; }}
.sf-badge.crit {{ background: {SLATE_200}; color: {SLATE_700}; }}
.sf-table-wrap {{ overflow-x: auto; }}
table.sf-table {{ width: 100% !important; table-layout: fixed; border-collapse: collapse; font-size: .88rem; margin: 0; display: table; border: none !important; }}
table.sf-table tr {{ border: none !important; }}
table.sf-table th, table.sf-table td {{ vertical-align: middle; }}
table.sf-table .sf-badge, table.sf-table code {{ white-space: nowrap; }}
table.sf-table .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
table.sf-table th, table.sf-table td {{ border-left: none !important; border-right: none !important; border-top: none !important; }}
table.sf-table th {{ text-align: left; color: {SLATE_500}; font-weight: 600; font-size: .75rem; text-transform: uppercase; letter-spacing: .05em; padding: .55rem .6rem; border-bottom: 1px solid {SLATE_200}; }}
table.sf-table td {{ padding: .55rem .6rem; border-bottom: 1px solid #F1F5F9; color: {SLATE_700}; }}
table.sf-table td code {{ background: #F1F5F9; color: {SLATE_900}; padding: .1rem .35rem; border-radius: 6px; font-size: .82rem; }}

.sf-flow {{ display: flex; align-items: center; flex-wrap: wrap; gap: .5rem; margin: .4rem 0 .8rem; }}
.sf-step {{ background: {NAVY}; color: #fff; border-radius: 10px; padding: .5rem .8rem; font-size: .84rem; font-weight: 600; }}
.sf-step.here {{ background: {AMBER}; color: {NAVY}; }}
.sf-arrow {{ color: {SLATE_300}; font-weight: 700; }}
.sf-empty {{ text-align: center; padding: 3rem 1rem; }}
.sf-empty h3 {{ color: {SLATE_900}; margin-bottom: .3rem; }} .sf-empty p {{ color: {SLATE_500}; }}
</style>
""",
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------------------------
def database_url() -> str:
    # Check first: touching st.secrets without a secrets.toml renders an error box in the app.
    url = st.secrets.get("DATABASE_URL") if st.secrets.load_if_toml_exists() else None
    url = (url or os.getenv("DASHBOARD_DB_URL") or os.getenv("CLOUD_DATABASE_URL")
           or "postgresql://skyflow:skyflow@localhost:5442/skyflow").strip().strip('"')
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg2://" + url[len(prefix):]
    return url


# Cached per URL: if the secret changes (e.g. it was missing on first boot), a new engine is
# created instead of reusing one that points at the old target.
@st.cache_resource
def engine(url: str):
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=3600, show_spinner="Loading flight operations data…")
def load_marts(url: str) -> dict[str, pd.DataFrame]:
    """Each mart is small (tens of thousands of rows), so load once and filter in memory: every
    filter change is then instant instead of a round trip to the cloud database."""
    with engine(url).connect() as conn:
        marts = {
            name: pd.read_sql(f"SELECT * FROM marts.{name}", conn)
            for name in ["daily_ops", "delay_heatmap", "route_daily", "dq_summary", "pipeline_summary"]
        }
    for name in ["daily_ops", "route_daily", "dq_summary"]:
        marts[name]["flight_date"] = pd.to_datetime(marts[name]["flight_date"]).dt.date
    return marts


try:
    data = load_marts(database_url())
except Exception as exc:  # show a friendly card instead of a stack trace
    st.markdown(
        f'<div class="sf-card sf-empty"><h3>Can\'t reach the SkyFlow database</h3>'
        f'<p>{type(exc).__name__}. Check the DATABASE_URL secret and that marts are published.</p></div>',
        unsafe_allow_html=True,
    )
    st.stop()

daily, heat, routes, dq, summary = (data[k] for k in
                                    ["daily_ops", "delay_heatmap", "route_daily", "dq_summary", "pipeline_summary"])
summary = summary.iloc[0]


# ---------------------------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------------------------
def style(fig: go.Figure, height: int = 340, legend: bool = False) -> go.Figure:
    fig.update_layout(
        height=height, margin=dict(l=8, r=8, t=12, b=8),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", color=SLATE_700, size=12),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=SLATE_200, font=dict(family="Inter", color=SLATE_900, size=12)),
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, bgcolor="rgba(0,0,0,0)", title=None),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor=SLATE_200, ticks="", tickfont=dict(color=SLATE_500))
    fig.update_yaxes(showgrid=True, gridcolor="#EEF1F5", zeroline=False, tickfont=dict(color=SLATE_500))
    return fig


def show(fig: go.Figure) -> None:
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def section(title: str, caption: str = "") -> None:
    st.markdown(f'<div class="sf-section">{title}</div>'
                + (f'<div class="sf-caption">{caption}</div>' if caption else ""), unsafe_allow_html=True)


def totals(df: pd.DataFrame) -> dict:
    flights, flown = int(df["flights"].sum()), int(df["flown"].sum())
    return {
        "flights": flights,
        "on_time": (flown - df["delayed_15"].sum()) / flown if flown else None,
        "avg_delay": df["arr_delay_sum"].sum() / flown if flown else None,
        "cancel": df["cancelled"].sum() / flights if flights else None,
    }


def kpi_card(label: str, value: str, delta: float | None, fmt: str, higher_is_good: bool | None,
             accent: bool = False) -> str:
    if delta is None:
        delta_html = '<span class="sf-delta flat">— <small>no prior period</small></span>'
    else:
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "■")
        tone = "flat" if higher_is_good is None or delta == 0 else (
            "good" if (delta > 0) == higher_is_good else "bad")
        delta_html = f'<span class="sf-delta {tone}">{arrow} {fmt.format(abs(delta))} <small>vs prior period</small></span>'
    return (f'<div class="sf-card{" sf-accent" if accent else ""}"><div class="sf-kpi-label">{label}</div>'
            f'<div class="sf-kpi-value">{value}</div>{delta_html}</div>')


def empty_state(message: str) -> None:
    st.markdown(f'<div class="sf-card sf-empty"><h3>No flights match these filters</h3><p>{message}</p></div>',
                unsafe_allow_html=True)


def pct(x) -> str:
    return "—" if x is None or pd.isna(x) else f"{x:.1%}"


# ---------------------------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------------------------
min_date, max_date = daily["flight_date"].min(), daily["flight_date"].max()
airline_names = (daily.groupby(["airline_code", "airline_name"])["flights"].sum()
                 .sort_values(ascending=False).reset_index())
airline_label = {r.airline_code: f"{r.airline_name} ({r.airline_code})" for r in airline_names.itertuples()}
origin_order = daily.groupby("origin")["flights"].sum().sort_values(ascending=False).index.tolist()

with st.sidebar:
    st.markdown(f'<div class="sf-wordmark" style="color:{NAVY};font-size:1.4rem">Sky<span>Flow</span></div>'
                f'<div class="sf-caption">Filters apply to every tab</div>', unsafe_allow_html=True)
    # Default to the latest 30 days so the KPI cards can compare against the 30 days before.
    picked = st.date_input("Date range", value=(max(min_date, max_date - timedelta(days=29)), max_date),
                           min_value=min_date, max_value=max_date)
    airlines = st.multiselect("Airline", list(airline_label), format_func=airline_label.get,
                              placeholder="All airlines")
    origins = st.multiselect("Origin airport", origin_order, placeholder="All airports (busiest first)")

# The date picker returns a single date while the user is mid-selection.
start, end = (picked if isinstance(picked, tuple) and len(picked) == 2
              else ((picked[0], picked[0]) if isinstance(picked, tuple) else (picked, picked)))


def apply_filters(df: pd.DataFrame, dates: bool = True, lo=None, hi=None) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    if dates:
        mask &= (df["flight_date"] >= (lo or start)) & (df["flight_date"] <= (hi or end))
    if airlines:
        mask &= df["airline_code"].isin(airlines)
    if origins:
        mask &= df["origin"].isin(origins)
    return df[mask]


cur = apply_filters(daily)
period_days = (end - start).days + 1
prev_end, prev_start = start - timedelta(days=1), start - timedelta(days=period_days)
prev = apply_filters(daily, lo=prev_start, hi=prev_end) if prev_start >= min_date else daily.iloc[0:0]

# ---------------------------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------------------------
st.markdown(
    f"""<div class="sf-header">
  <div><div class="sf-wordmark">Sky<span>Flow</span></div><div class="sf-tagline">Flight operations analytics</div></div>
  <div style="display:flex;gap:.5rem;flex-wrap:wrap">
    <div class="sf-pill">{start:%d %b %Y} – {end:%d %b %Y}</div>
    <div class="sf-pill"><b>{len(airlines) or "All"}</b> airlines · <b>{len(origins) or "All"}</b> airports</div>
    <div class="sf-pill">Data: US BTS On-Time Performance</div>
  </div>
</div>""",
    unsafe_allow_html=True,
)

tab_overview, tab_airlines, tab_patterns, tab_health = st.tabs(
    ["Overview", "Airlines & Routes", "Delay Patterns", "Pipeline Health"])

# ---------------------------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------------------------
with tab_overview:
    if cur.empty:
        empty_state("Widen the date range or clear the airline / airport filters.")
    else:
        now, before = totals(cur), (totals(prev) if not prev.empty else None)

        def change(key, relative=False):
            if before is None or before[key] is None or now[key] is None:
                return None
            return (now[key] - before[key]) / before[key] if relative else now[key] - before[key]

        cards = [
            kpi_card("Total flights", f"{now['flights']:,}", change("flights", relative=True), "{:.1%}", None),
            kpi_card("On-time arrivals", pct(now["on_time"]),
                     None if change("on_time") is None else change("on_time") * 100, "{:.1f} pts", True, accent=True),
            kpi_card("Avg arrival delay", f"{now['avg_delay']:+.1f} min" if now["avg_delay"] is not None else "—",
                     change("avg_delay"), "{:.1f} min", False),
            kpi_card("Cancellation rate", pct(now["cancel"]),
                     None if change("cancel") is None else change("cancel") * 100, "{:.2f} pts", False),
        ]
        for col, card in zip(st.columns(4, gap="medium"), cards, strict=True):
            col.markdown(card, unsafe_allow_html=True)

        section("Daily on-time performance",
                "Share of flown flights arriving less than 15 minutes late, with the 7-day rolling average.")
        by_day = cur.groupby("flight_date")[["flown", "delayed_15", "flights", "cancelled"]].sum()
        by_day["on_time"] = 1 - by_day["delayed_15"] / by_day["flown"].where(by_day["flown"] > 0)
        by_day["rolling"] = by_day["on_time"].rolling(7, min_periods=1).mean()
        fig = go.Figure()
        fig.add_bar(x=by_day.index, y=by_day["on_time"], name="Daily", marker_color=TEAL_LIGHT,
                    hovertemplate="%{x|%a %d %b}<br>On time: <b>%{y:.1%}</b><extra></extra>")
        fig.add_scatter(x=by_day.index, y=by_day["rolling"], name="7-day average", mode="lines",
                        line=dict(color=NAVY, width=3), hovertemplate="7-day avg: <b>%{y:.1%}</b><extra></extra>")
        lo = max(0.0, by_day["on_time"].min() - 0.08) if by_day["on_time"].notna().any() else 0
        fig.update_yaxes(tickformat=".0%", range=[lo, 1])
        fig.update_layout(bargap=0.25, hovermode="x unified")
        show(style(fig, 360, legend=True))

        left, right = st.columns(2, gap="medium")
        with left:
            section("What causes delays", "Share of reported delay minutes by cause (flights 15+ min late).")
            causes = pd.Series({
                "Late aircraft": cur["late_aircraft_delay_min"].sum(), "Carrier": cur["carrier_delay_min"].sum(),
                "Air traffic system (NAS)": cur["nas_delay_min"].sum(), "Weather": cur["weather_delay_min"].sum(),
                "Security": cur["security_delay_min"].sum(),
            }).sort_values()
            share = causes / causes.sum() if causes.sum() else causes
            fig = go.Figure(go.Bar(
                x=share.values, y=share.index, orientation="h",
                marker_color=[CAUSE_COLORS[c] for c in share.index],
                text=[f"{v:.0%}" for v in share.values], textposition="outside", cliponaxis=False,
                customdata=causes.values, hovertemplate="%{y}: <b>%{x:.1%}</b> (%{customdata:,.0f} min)<extra></extra>",
            ))
            fig.update_xaxes(visible=False, range=[0, share.max() * 1.18 if share.max() else 1])
            fig.update_yaxes(showgrid=False)
            show(style(fig, 300))
        with right:
            section("Cancellations per day", "Scheduled flights cancelled, by flight date.")
            fig = go.Figure(go.Bar(x=by_day.index, y=by_day["cancelled"], marker_color=AMBER,
                                   hovertemplate="%{x|%a %d %b}<br><b>%{y:,}</b> cancelled<extra></extra>"))
            show(style(fig, 300))

# ---------------------------------------------------------------------------------------------
# Airlines & Routes
# ---------------------------------------------------------------------------------------------
with tab_airlines:
    if cur.empty:
        empty_state("Widen the date range or clear the airline / airport filters.")
    else:
        section("Airline league table", "Click a column header to sort. Bars show on-time share.")
        league = cur.groupby(["airline_name", "airline_code"])[
            ["flights", "flown", "delayed_15", "cancelled", "arr_delay_sum"]].sum().reset_index()
        league["on_time"] = 1 - league["delayed_15"] / league["flown"].where(league["flown"] > 0)
        league["avg_delay"] = league["arr_delay_sum"] / league["flown"].where(league["flown"] > 0)
        league["cancel_rate"] = league["cancelled"] / league["flights"]
        league = league.sort_values("on_time", ascending=False)
        st.dataframe(
            league[["airline_name", "airline_code", "flights", "on_time", "avg_delay", "cancel_rate"]].assign(
                on_time=lambda d: d["on_time"] * 100, cancel_rate=lambda d: d["cancel_rate"] * 100),
            hide_index=True, use_container_width=True,
            column_config={
                "airline_name": st.column_config.TextColumn("Airline", width="medium"),
                "airline_code": st.column_config.TextColumn("Code", width="small"),
                "flights": st.column_config.NumberColumn("Flights", format="%d"),
                "on_time": st.column_config.ProgressColumn("On time", format="%.1f%%", min_value=0, max_value=100),
                "avg_delay": st.column_config.NumberColumn("Avg arr delay (min)", format="%.1f"),
                "cancel_rate": st.column_config.NumberColumn("Cancelled", format="%.2f%%"),
            },
        )

        section("10 most-delayed routes",
                "Among the 300 busiest routes, those with the highest share of flights arriving 15+ min late "
                "(minimum 30 flown flights in the selection).")
        r = apply_filters(routes)
        r = r.groupby(["origin", "dest"])[["flown", "delayed_15", "arr_delay_sum"]].sum().reset_index()
        r = r[r["flown"] >= 30]
        if r.empty:
            empty_state("No busy route has enough flights in this selection.")
        else:
            r["delay_rate"] = r["delayed_15"] / r["flown"]
            r["avg_delay"] = r["arr_delay_sum"] / r["flown"]
            worst = r.nlargest(10, "delay_rate").sort_values("delay_rate")
            worst["route"] = worst["origin"] + " → " + worst["dest"]
            fig = go.Figure(go.Bar(
                x=worst["delay_rate"], y=worst["route"], orientation="h", marker_color=RED,
                text=[f"{v:.0%}" for v in worst["delay_rate"]], textposition="outside", cliponaxis=False,
                customdata=worst[["flown", "avg_delay"]].values,
                hovertemplate="<b>%{y}</b><br>Delayed: %{x:.1%}<br>Flights: %{customdata[0]:,}"
                              "<br>Avg arr delay: %{customdata[1]:.1f} min<extra></extra>",
            ))
            fig.update_xaxes(visible=False, range=[0, worst["delay_rate"].max() * 1.15])
            fig.update_yaxes(showgrid=False)
            show(style(fig, 380))

# ---------------------------------------------------------------------------------------------
# Delay Patterns
# ---------------------------------------------------------------------------------------------
with tab_patterns:
    h = apply_filters(heat, dates=False)
    if h.empty:
        empty_state("Clear the airline / airport filters to see delay patterns.")
    else:
        section("When delays happen",
                "Share of flights arriving 15+ min late by scheduled departure hour and weekday. "
                "Covers the full loaded period; airline and airport filters apply.")
        grid = h.groupby(["day_of_week", "dep_hour"])[["flown", "delayed_15"]].sum().reset_index()
        grid["rate"] = grid["delayed_15"] / grid["flown"]
        # Blank out cells with < 30 flights (e.g. 3-4 AM): their rates are noise, not a pattern.
        grid.loc[grid["flown"] < 30, "rate"] = None
        z = grid.pivot(index="day_of_week", columns="dep_hour", values="rate").reindex(range(1, 8))
        n = grid.pivot(index="day_of_week", columns="dep_hour", values="flown").reindex(range(1, 8))
        fig = go.Figure(go.Heatmap(
            z=z.values, x=[f"{c:02d}:00" for c in z.columns], y=DOW, customdata=n.values,
            colorscale=[[0, "#E6F4F1"], [0.35, TEAL_LIGHT], [0.6, "#F8D58A"], [0.8, AMBER], [1, RED]],
            xgap=2, ygap=2, colorbar=dict(tickformat=".0%", outlinewidth=0, thickness=12, len=0.8),
            hovertemplate="%{y} %{x}<br>Delayed: <b>%{z:.1%}</b><br>Flights: %{customdata:,}<extra></extra>",
        ))
        fig.update_yaxes(autorange="reversed", showgrid=False)
        show(style(fig, 360))

        left, right = st.columns(2, gap="medium")
        with left:
            section("Delay rate through the day", "Later departures inherit delays that build up during the day.")
            by_hour = grid.groupby("dep_hour")[["flown", "delayed_15"]].sum()
            by_hour["rate"] = by_hour["delayed_15"] / by_hour["flown"]
            fig = go.Figure(go.Scatter(
                x=by_hour.index, y=by_hour["rate"], mode="lines+markers", line=dict(color=NAVY, width=3),
                marker=dict(size=6, color=AMBER), fill="tozeroy", fillcolor="rgba(15,27,61,0.06)",
                customdata=by_hour["flown"], hovertemplate="%{x}:00 — <b>%{y:.1%}</b> delayed<br>%{customdata:,} flights<extra></extra>",
            ))
            fig.update_yaxes(tickformat=".0%", rangemode="tozero")
            fig.update_xaxes(dtick=3, ticksuffix=":00")
            show(style(fig, 300))
        with right:
            section("Delay causes by airline", "Share of each airline's delay minutes (selected dates).")
            if cur.empty:
                empty_state("No flights in the selected dates.")
            else:
                cols = {"late_aircraft_delay_min": "Late aircraft", "carrier_delay_min": "Carrier",
                        "nas_delay_min": "Air traffic system (NAS)", "weather_delay_min": "Weather",
                        "security_delay_min": "Security"}
                by_al = cur.groupby("airline_code")[list(cols)].sum().rename(columns=cols)
                by_al = by_al.div(by_al.sum(axis=1).where(lambda s: s > 0), axis=0).dropna()
                by_al = by_al.sort_values("Late aircraft")
                fig = go.Figure()
                for cause in cols.values():
                    fig.add_bar(y=by_al.index, x=by_al[cause], name=cause, orientation="h",
                                marker_color=CAUSE_COLORS[cause],
                                hovertemplate="%{y} · " + cause + ": <b>%{x:.0%}</b><extra></extra>")
                fig.update_layout(barmode="stack", bargap=0.3)
                fig.update_xaxes(tickformat=".0%", range=[0, 1])
                fig.update_yaxes(showgrid=False)
                show(style(fig, max(300, 22 * len(by_al) + 80), legend=True))

# ---------------------------------------------------------------------------------------------
# Pipeline Health
# ---------------------------------------------------------------------------------------------
with tab_health:
    ok = int(summary["critical_failures"]) == 0
    status = (f'<span class="sf-badge {"pass" if ok else "fail"}">{"HEALTHY" if ok else "ATTENTION"}</span>')
    st.markdown(
        f'<div class="sf-card" style="margin-bottom:1rem"><div class="sf-kpi-label">Latest pipeline run</div>'
        f'<div style="display:flex;gap:1rem;align-items:center;flex-wrap:wrap;margin-top:.4rem">{status}'
        f'<span style="color:{SLATE_700}">{int(summary["partitions"])} daily partitions · '
        f'{summary["first_date"]:%d %b %Y} – {summary["last_date"]:%d %b %Y} · last load '
        f'{pd.Timestamp(summary["last_loaded_at"]):%d %b %Y %H:%M} UTC · marts published '
        f'{pd.Timestamp(summary["published_at"]):%d %b %Y %H:%M} UTC</span></div></div>',
        unsafe_allow_html=True,
    )
    stats = [
        ("Rows ingested", f'{int(summary["rows_ingested"]):,}', "raw BTS records"),
        ("Rows loaded", f'{int(summary["rows_loaded"]):,}', "passed every check"),
        ("Rows rejected", f'{int(summary["rows_rejected"]):,}', "quarantined with a reason"),
        ("DQ results", f'{int(summary["dq_results"]):,}',
         f'{int(summary["dq_checks"])} checks × {int(summary["partitions"])} days'),
    ]
    for col, (label, value, note) in zip(st.columns(4, gap="medium"), stats, strict=True):
        col.markdown(f'<div class="sf-card"><div class="sf-kpi-label">{label}</div>'
                     f'<div class="sf-kpi-value">{value}</div><span class="sf-delta flat">{note}</span></div>',
                     unsafe_allow_html=True)

    # Full width, stacked: the 6-column DQ table needs the room (side by side it wrapped mid-word).
    left, right = st.container(), st.container()
    with left:
        section(f"Data-quality checks ({dq['check_name'].nunique()})",
                "Latest result per day. Critical checks fail the run; warnings quarantine individual rows.")
        checks = dq.groupby(["stage", "check_name", "severity"]).agg(
            days=("passed", "size"), days_failed=("passed", lambda s: int((~s).sum())),
            failed_rows=("failed_rows", "sum")).reset_index()
        checks["order"] = checks["stage"].map({"validate": 0, "reconcile": 1})
        checks = checks.sort_values(["order", "severity", "check_name"])
        rows = []
        for c in checks.itertuples():
            # reject_rate can pass with rows flagged (under the 1% cap), so status is per day, not per row.
            kind = "pass" if c.days_failed == 0 else ("fail" if c.severity == "critical" else "warn")
            badge = f'<span class="sf-badge {kind}">{kind.upper()}</span>'
            rows.append(f"<tr><td>{badge}</td><td><code>{c.check_name}</code></td>"
                        f'<td><span class="sf-badge crit">{c.severity}</span></td><td>{c.stage}</td>'
                        f'<td class="num">{c.days - c.days_failed}/{c.days}</td>'
                        f'<td class="num">{c.failed_rows:,}</td></tr>')
        # Fixed column widths keep the table inside its card at any screen width.
        cols = "".join(f'<col style="width:{w}%">' for w in (11, 33, 14, 14, 14, 14))
        st.markdown(
            f'<div class="sf-card sf-table-wrap"><table class="sf-table"><colgroup>{cols}</colgroup>'
            "<thead><tr><th>Status</th><th>Check</th><th>Severity</th><th>Stage</th>"
            '<th class="num">Days passed</th><th class="num">Rows flagged</th></tr></thead>'
            f"<tbody>{''.join(rows)}</tbody></table></div>",
            unsafe_allow_html=True,
        )
    with right:
        section("How it works")
        steps = ["BTS on-time data", "Airflow ELT", "Postgres star schema", "Marts", "Neon (cloud)",
                 "This dashboard"]
        flow = '<span class="sf-arrow">→</span>'.join(
            f'<span class="sf-step{" here" if s == "This dashboard" else ""}">{s}</span>' for s in steps)
        st.markdown(
            f"""<div class="sf-card"><div class="sf-flow">{flow}</div>
<div style="color:{SLATE_700};font-size:.9rem;line-height:1.6">
<b>Daily batches.</b> Airflow processes one flight date per run: extract → validate → transform →
load dimensions → load facts → build ML features → reconcile → publish.<br>
<b>Idempotent.</b> Each day is replaced with a delete-insert in one transaction, so re-runs and
backfills never duplicate rows.<br>
<b>Quality gates.</b> {int(summary["dq_checks"])} checks per day. Bad rows are quarantined, and more
than 1% bad rows, duplicate keys or a reconciliation mismatch fail the run.<br>
<b>Lightweight cloud.</b> Only these pre-aggregated marts are published; the
{int(summary["rows_loaded"]):,}-row fact table stays in the warehouse.
</div></div>""",
            unsafe_allow_html=True,
        )
