"""Dashboard for the SL delay analytics.

Reads the small result tables that spark-analytics.py exported to
data/analytics/*.csv, so it needs neither Spark nor HDFS to run.

Usage:
    python scripts/dashboard.py              # then open http://127.0.0.1:8050
    python scripts/dashboard.py --port 8060
"""

import argparse
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data" / "analytics"

MODES = ["all", "bus", "metro", "train", "tram", "ferry"]
TOP = 15

# Colours: weekday/weekend are the two categorical slots, the map is one blue
# ramp from light to dark. Both sets pass the colour-blindness checks.
WEEKDAY, WEEKEND = "#2a78d6", "#eb6834"
DAY_COLOURS = {"weekday": WEEKDAY, "weekend": WEEKEND}
RAMP = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281"]
SURFACE, PAGE = "#fcfcfb", "#f9f9f7"
INK, INK_2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS = "#e1e0d9", "#c3c2b7"
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def load(folder):
    tables = {name: pd.read_csv(folder / f"{name}.csv", dtype={"route_id": str, "station_id": str,
                                                                "route_short_name": str})
              for name in ["lines", "stations", "hourly", "day_types", "histogram", "daily"]}
    tables["lines"]["label"] = tables["lines"].transport_mode.str.capitalize() + " " + \
        tables["lines"].route_short_name
    # station names repeat (Slussen is five stations), the modes tell them apart
    tables["stations"]["label"] = tables["stations"].station_name + " (" + tables["stations"].modes + ")"
    return tables


def style(figure, height):
    figure.update_layout(
        height=height, margin=dict(l=8, r=16, t=8, b=8),
        paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font=dict(family=FONT, size=13, color=INK_2),
        legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, title=None),
        hoverlabel=dict(font_family=FONT),
        barcornerradius=4,
    )
    figure.update_xaxes(gridcolor=GRID, linecolor=AXIS, zerolinecolor=AXIS, tickfont_color=MUTED)
    figure.update_yaxes(gridcolor=GRID, linecolor=AXIS, zerolinecolor=AXIS, tickfont_color=MUTED)
    return figure


def card(title, note, *children):
    return html.Div(className="card", children=[
        html.H2(title), html.P(note, className="note"), *children])


def tile(label, value_id):
    return html.Div(className="tile", children=[html.Div(label, className="tile-label"),
                                                html.Div(id=value_id, className="tile-value")])


def minutes(seconds):
    sign = "-" if seconds < 0 else ""
    seconds = round(abs(seconds))
    return f"{sign}{seconds // 60} min {seconds % 60:02d} s" if seconds >= 60 else f"{sign}{seconds} s"


def build(tables):
    daily = tables["daily"]
    period = (f"{daily.service_date.min()} to {daily.service_date.max()} · "
              f"{len(daily)} service days · "
              f"{(daily.day_type == 'weekday').sum()} weekdays, {(daily.day_type == 'weekend').sum()} weekend days")

    app = Dash(__name__, title="SL delay analytics",
               meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])
    app.index_string = INDEX.replace("{css}", CSS)
    app.layout = html.Div(className="page", children=[
        html.Header([
            html.H1("Stockholm public transport delays"),
            html.P(f"SL GTFS-RT trip updates from Trafiklab KoDa, {period}. "
                   "A stop is one vehicle stopping at one platform; late means 3 minutes or more."),
        ]),
        html.Div(className="filters", children=[
            html.Span("Transport mode", className="filter-label"),
            dcc.RadioItems(id="mode", value="all", inline=True, className="radio",
                           options=[{"label": m.capitalize(), "value": m} for m in MODES]),
        ]),
        html.Div(className="tiles", children=[
            tile("Stops analysed", "tile-stops"),
            tile("Average delay", "tile-avg"),
            tile("Stops 3+ min late", "tile-late"),
            tile("Weekend vs weekday average", "tile-weekend"),
        ]),
        card("When are delays worst?",
             "Average delay by scheduled hour (Stockholm time). Night hours have few stops, so they swing more.",
             dcc.Graph(id="hourly", config={"displayModeBar": False})),
        html.Div(className="row", children=[
            card("Lines with the highest average delay",
                 "Top lines with at least 100 stops per day.",
                 dcc.Graph(id="lines", config={"displayModeBar": False})),
            card("Stations most often late",
                 "Share of stops 3+ minutes late, stations with at least 50 stops per day. "
                 "Delays add up along a trip, so these mostly reflect the lines through them.",
                 dcc.Graph(id="stations", config={"displayModeBar": False})),
        ]),
        card("Where are stations late?",
             "Each dot is a station with at least 50 stops per day; darker means a larger share of late stops, "
             "bigger means more stops.",
             dcc.Graph(id="map", config={"displayModeBar": False, "scrollZoom": True})),
        html.Div(className="row", children=[
            card("How late are vehicles?",
                 "Share of stops per one-minute delay bin. Left of zero is early; the dashed line is the 3-minute mark.",
                 dcc.Graph(id="histogram", config={"displayModeBar": False})),
            card("Weekday vs weekend by mode",
                 "Average delay per transport mode.",
                 dcc.Graph(id="day-types", config={"displayModeBar": False})),
        ]),
        card("Busiest stations",
             "Most vehicle stops per day (vehicles, not passengers).",
             dash_table.DataTable(id="busiest", style_as_list_view=True, **TABLE_STYLE)),
    ])

    @app.callback(
        Output("tile-stops", "children"), Output("tile-avg", "children"),
        Output("tile-late", "children"), Output("tile-weekend", "children"),
        Output("hourly", "figure"), Output("lines", "figure"), Output("stations", "figure"),
        Output("map", "figure"), Output("histogram", "figure"), Output("day-types", "figure"),
        Output("busiest", "data"), Output("busiest", "columns"),
        Input("mode", "value"))
    def update(mode):
        stations = tables["stations"]
        if mode != "all":
            stations = stations[stations.modes.str.split(", ").apply(lambda modes: mode in modes)]
        return (*tiles(tables["day_types"], mode),
                hourly_figure(tables["hourly"], mode),
                lines_figure(tables["lines"], mode),
                stations_figure(stations),
                map_figure(stations),
                histogram_figure(tables["histogram"], mode),
                day_types_figure(tables["day_types"], mode),
                *busiest_table(stations))

    return app


def tiles(day_types, mode):
    rows = day_types[day_types.transport_mode == mode].set_index("day_type")
    stops = rows.stops.sum()
    avg = (rows.avg_delay_s * rows.stops).sum() / stops
    late = rows.late_stops.sum() / stops * 100
    gap = rows.avg_delay_s.get("weekend", float("nan")) - rows.avg_delay_s.get("weekday", float("nan"))
    gap_text = "n/a" if pd.isna(gap) else ("+" if gap >= 0 else "-") + minutes(abs(gap))
    return f"{stops:,}", minutes(avg), f"{late:.1f} %", gap_text


def hourly_figure(hourly, mode):
    rows = hourly[(hourly.transport_mode == mode) & (hourly.day_type != "all")].copy()
    rows["avg_delay_min"] = rows.avg_delay_s / 60
    figure = go.Figure()
    for day_type, colour in DAY_COLOURS.items():
        part = rows[rows.day_type == day_type].sort_values("hour_of_day")
        figure.add_scatter(
            x=part.hour_of_day, y=part.avg_delay_min, name=day_type.capitalize(), mode="lines+markers",
            line=dict(color=colour, width=2), marker=dict(size=8, line=dict(color=SURFACE, width=2)),
            customdata=part[["late_pct", "stops"]],
            hovertemplate="%{y:.2f} min average · %{customdata[0]:.1f} % late · %{customdata[1]:,} stops")
    figure.update_layout(hovermode="x unified")
    figure.update_xaxes(title="Scheduled hour of day", dtick=2, range=[-0.5, 23.5], zeroline=False)
    figure.update_yaxes(title="Average delay (min)", rangemode="tozero")
    return style(figure, 320)


def ranking_figure(rows, value, label, value_text, hover):
    rows = rows.iloc[::-1]  # plotly draws the first bar at the bottom
    figure = go.Figure(go.Bar(
        x=rows[value], y=rows[label], orientation="h", marker_color=WEEKDAY,
        text=rows[value].map(value_text), textposition="outside", cliponaxis=False,
        textfont_color=INK_2, customdata=rows[hover.columns], hovertemplate=hover.template))
    figure.update_layout(bargap=0.35, showlegend=False)
    # room on the right for the value labels
    figure.update_xaxes(showgrid=True, range=[0, rows[value].max() * 1.18])
    figure.update_yaxes(showgrid=False, tickfont_color=INK_2)
    return style(figure, 30 * max(len(rows), 3) + 40)


class Hover:
    def __init__(self, columns, template):
        self.columns, self.template = columns, template


def lines_figure(lines, mode):
    rows = lines[lines.rank_avg_delay.notna()]
    if mode != "all":
        rows = rows[rows.transport_mode == mode]
    rows = rows.sort_values("avg_delay_s", ascending=False).head(TOP).assign(avg_delay_min=lambda r: r.avg_delay_s / 60)
    figure = ranking_figure(rows, "avg_delay_min", "label", lambda m: f"{m:.1f} min", Hover(
        ["route_desc", "late_pct", "stops", "trips"],
        "<b>%{y}</b> %{customdata[0]}<br>%{x:.2f} min average<br>%{customdata[1]:.1f} % of stops late"
        "<br>%{customdata[2]:,} stops on %{customdata[3]:,} trips<extra></extra>"))
    figure.update_xaxes(title="Average delay (min)")
    return figure


def stations_figure(stations):
    rows = stations[stations.rank_late_pct.notna()].sort_values(["late_pct", "stops"], ascending=False).head(TOP)
    figure = ranking_figure(rows, "late_pct", "label", lambda p: f"{p:.0f} %", Hover(
        ["avg_delay_s", "stops_per_day", "lines"],
        "<b>%{y}</b><br>%{x:.1f} % of stops late<br>%{customdata[0]:.0f} s average"
        "<br>%{customdata[1]:.0f} stops per day on %{customdata[2]} lines<extra></extra>"))
    figure.update_xaxes(title="Stops 3+ minutes late (%)")
    return figure


def map_figure(stations):
    rows = stations[stations.rank_late_pct.notna()].sort_values("late_pct")  # latest-late drawn on top
    figure = px.scatter_map(
        rows, lat="station_lat", lon="station_lon", color="late_pct", size="stops_per_day", size_max=14,
        color_continuous_scale=RAMP, hover_name="station_name",
        hover_data={"modes": True, "late_pct": ":.1f", "avg_delay_s": ":.0f", "stops_per_day": ":.0f",
                    "station_lat": False, "station_lon": False},
        labels={"late_pct": "Late %", "avg_delay_s": "Average delay (s)", "stops_per_day": "Stops per day",
                "modes": "Modes"},
        center=dict(lat=59.33, lon=18.06), zoom=8.6, map_style="carto-positron")
    figure.update_layout(coloraxis_colorbar=dict(title="Late %", thickness=10, len=0.6))
    return style(figure, 520)


def histogram_figure(histogram, mode):
    rows = histogram if mode == "all" else histogram[histogram.transport_mode == mode]
    rows = rows.groupby(["day_type", "delay_minute"], as_index=False).stops.sum()
    rows["share"] = rows.stops / rows.groupby("day_type").stops.transform("sum") * 100
    rows = rows[rows.delay_minute.between(-10, 30)]
    rows["bin"] = rows.delay_minute.astype(str) + " to " + (rows.delay_minute + 1).astype(str) + " min"
    figure = go.Figure()
    for day_type, colour in DAY_COLOURS.items():
        part = rows[rows.day_type == day_type].sort_values("delay_minute")
        figure.add_scatter(
            x=part.delay_minute, y=part.share, name=day_type.capitalize(), mode="lines",
            line=dict(color=colour, width=2, shape="hv"), customdata=part[["bin"]],
            hovertemplate="%{customdata[0]}: %{y:.1f} % of stops<extra>" + day_type + "</extra>")
    figure.add_vline(x=3, line=dict(color=MUTED, width=1, dash="dash"))
    figure.update_layout(hovermode="x unified")
    figure.update_xaxes(title="Delay (min, cut at -10 and 30)", dtick=5)
    figure.update_yaxes(title="Share of stops (%)", rangemode="tozero")
    return style(figure, 320)


def day_types_figure(day_types, mode):
    rows = day_types[day_types.transport_mode != "all"] if mode == "all" else day_types[day_types.transport_mode == mode]
    rows = rows.assign(mode_label=rows.transport_mode.str.capitalize(), avg_delay_min=rows.avg_delay_s / 60)
    order = rows.groupby("mode_label").avg_delay_min.mean().sort_values(ascending=False).index.tolist()
    figure = go.Figure()
    for day_type, colour in DAY_COLOURS.items():
        part = rows[rows.day_type == day_type].set_index("mode_label").reindex(order)
        figure.add_bar(
            x=part.index, y=part.avg_delay_min, name=day_type.capitalize(), marker_color=colour,
            customdata=part[["late_pct", "stops_per_day"]],
            hovertemplate="%{y:.2f} min average · %{customdata[0]:.1f} % late · "
                          "%{customdata[1]:,.0f} stops per day<extra>" + day_type + "</extra>")
    figure.update_layout(barmode="group", bargap=0.35, bargroupgap=0.08)
    figure.update_yaxes(title="Average delay (min)", zeroline=True)
    return style(figure, 320)


def busiest_table(stations):
    rows = stations.sort_values("stops", ascending=False).head(TOP)
    columns = [("station_name", "Station"), ("modes", "Modes"), ("lines", "Lines"),
               ("stops_per_day", "Stops per day"), ("late_pct", "Late %"), ("avg_delay_s", "Average delay (s)")]
    rows = rows[[c for c, _ in columns]].round({"stops_per_day": 0, "avg_delay_s": 0})
    return rows.to_dict("records"), [{"id": c, "name": n} for c, n in columns]


TABLE_STYLE = dict(
    style_header={"fontWeight": "600", "color": INK, "backgroundColor": SURFACE, "borderBottom": f"1px solid {AXIS}"},
    style_cell={"fontFamily": FONT, "fontSize": "14px", "color": INK_2, "backgroundColor": SURFACE,
                "padding": "6px 10px", "textAlign": "left", "fontVariantNumeric": "tabular-nums",
                "borderBottom": f"1px solid {GRID}"},
    style_cell_conditional=[{"if": {"column_id": c}, "textAlign": "right"}
                            for c in ["lines", "stops_per_day", "late_pct", "avg_delay_s"]],
)

CSS = f"""
body {{ margin: 0; background: {PAGE}; color: {INK}; font-family: {FONT}; }}
.page {{ max-width: 1240px; margin: 0 auto; padding: 24px 16px 48px; }}
header h1 {{ font-size: 26px; margin: 0 0 4px; }}
header p {{ color: {INK_2}; margin: 0 0 16px; }}
.filters {{ display: flex; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }}
.filter-label {{ font-weight: 600; }}
.radio {{ display: flex; flex-wrap: wrap; row-gap: 6px; }}
.radio label {{ margin-right: 14px; color: {INK_2}; cursor: pointer; }}
.radio input {{ margin-right: 6px; }}
.tiles {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(200px, 100%), 1fr)); gap: 12px; margin-bottom: 12px; }}
.tile, .card {{ background: {SURFACE}; border: 1px solid rgba(11,11,11,0.10); border-radius: 8px; }}
.tile {{ padding: 14px 16px; }}
.tile-label {{ color: {INK_2}; font-size: 13px; }}
.tile-value {{ font-size: 26px; font-weight: 600; margin-top: 4px; }}
.card {{ padding: 16px; margin-bottom: 12px; min-width: 0; }}
.card h2 {{ font-size: 17px; margin: 0 0 2px; }}
.card .note {{ color: {INK_2}; font-size: 13px; margin: 0 0 8px; }}
.row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(min(480px, 100%), 1fr)); gap: 12px; }}
"""

INDEX = """<!DOCTYPE html>
<html>
  <head>
    {%metas%}
    <title>{%title%}</title>
    {%favicon%}
    {%css%}
    <style>{css}</style>
  </head>
  <body>
    {%app_entry%}
    <footer>{%config%}{%scripts%}{%renderer%}</footer>
  </body>
</html>"""


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data", type=Path, default=DATA, help=f"folder with the analytics CSVs (default {DATA})")
    parser.add_argument("--port", type=int, default=8050)
    args = parser.parse_args()
    build(load(args.data)).run(port=args.port, debug=False)


if __name__ == "__main__":
    main()
