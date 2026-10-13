"""Dash app showing the gold tables (data/gold/, see docker/hdfs.sh fetch-gold).

    python dashboard/app.py          then open http://127.0.0.1:8050
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dash_table, dcc, html

ROOT = Path(__file__).resolve().parent.parent
GOLD = ROOT / "data" / "gold"

# Fixed colour per mode (categorical slots in fixed order), so a filter never repaints a mode.
MODE_COLORS = {"Bus": "#2a78d6", "Metro": "#eb6834", "Commuter rail": "#1baf7a",
               "Tram": "#eda100", "Ferry": "#e87ba4", "Other": "#8a8985", "All": "#0b0b0b"}
DAY_COLORS = {"weekday": "#2a78d6", "weekend": "#eb6834"}
SEQUENTIAL = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#104281", "#0d366b"]
TEXT, MUTED, GRID = "#0b0b0b", "#52514e", "#e6e5e1"

METRICS = {
    "pct_delayed": "% arrivals > 3 min late",
    "mean_delay_s": "Mean delay (s)",
    "median_delay_s": "Median delay (s)",
    "p90_delay_s": "90th percentile delay (s)",
}


def load(name):
    folder = GOLD / name
    if not folder.exists():
        sys.exit(f"{folder} not found: run the Spark jobs and docker/hdfs.sh fetch-gold first")
    return pd.read_parquet(folder)


by_line, by_station, by_hour = load("by_line"), load("by_station"), load("by_hour")
by_day_type, by_date, histogram, qa = load("by_day_type"), load("by_date"), load("histogram"), load("qa")
MODES = ["All"] + sorted(by_line["mode"].unique(), key=list(MODE_COLORS).index)


def style(fig, height=480):
    fig.update_layout(template="plotly_white", height=height, font=dict(color=TEXT, size=13),
                      margin=dict(l=10, r=10, t=40, b=10), legend_title_text="",
                      hoverlabel=dict(bgcolor="white"))
    fig.update_xaxes(gridcolor=GRID, linecolor=GRID, title_font_color=MUTED)
    fig.update_yaxes(gridcolor=GRID, linecolor=GRID, title_font_color=MUTED)
    return fig


def table(df, page_size=15):
    return dash_table.DataTable(df.to_dict("records"), [{"name": c, "id": c} for c in df.columns],
                                page_size=page_size, sort_action="native",
                                style_cell={"fontFamily": "system-ui", "fontSize": 13, "padding": "4px 8px"},
                                style_header={"fontWeight": 600})


controls = html.Div(className="controls", children=[
    html.Label(["Mode", dcc.Dropdown(MODES, "All", id="mode", clearable=False)]),
    html.Label(["Metric", dcc.Dropdown([{"label": v, "value": k} for k, v in METRICS.items()],
                                       "pct_delayed", id="metric", clearable=False)]),
    html.Label(["Minimum events per line / station",
                dcc.Slider(0, 5000, 500, value=1000, id="min-events",
                           marks={i: str(i) for i in range(0, 5001, 1000)})]),
])

app = Dash(__name__, title="SL Delay Analytics")
app.layout = html.Div(className="page", children=[
    html.H1("Stockholm public transport delays"),
    html.P("SL real-time arrivals, service dates 2026-09-13 to 2026-09-26 (10 weekdays, 4 weekend days). "
           "Delay = observed arrival minus timetable.", className="sub"),
    controls,
    dcc.Tabs(id="tab", value="lines", children=[
        dcc.Tab(label="Lines", value="lines"),
        dcc.Tab(label="Stations", value="stations"),
        dcc.Tab(label="Hour of day", value="hours"),
        dcc.Tab(label="Weekday vs weekend", value="daytype"),
        dcc.Tab(label="Data quality", value="qa"),
    ]),
    html.Div(id="content"),
])


def lines_tab(mode, metric, min_events):
    df = by_line[by_line["n_events"] >= min_events]
    if mode != "All":
        df = df[df["mode"] == mode]
    df = df.nlargest(25, metric).iloc[::-1]
    df = df.assign(label=df["mode"] + " " + df["line"].astype(str))
    fig = px.bar(df, x=metric, y="label", color="mode", orientation="h", color_discrete_map=MODE_COLORS,
                 hover_data={"label": False, "line_name": True, "n_events": ":,", "n_trips": ":,",
                             "pct_delayed": True, "mean_delay_s": True, "median_delay_s": True},
                 labels={metric: METRICS[metric], "label": ""},
                 title=f"Top 25 lines by {METRICS[metric].lower()}")
    fig.update_yaxes(categoryorder="array", categoryarray=df["label"])
    return [dcc.Graph(figure=style(fig, 700))]


def stations_tab(mode, metric, min_events):
    df = by_station[(by_station["mode"] == mode) & (by_station["n_events"] >= min_events)].dropna(subset=["lat"])
    fig = px.scatter_map(df, lat="lat", lon="lon", color=metric, size="n_events", size_max=18,
                         color_continuous_scale=SEQUENTIAL, hover_name="station_name",
                         hover_data={"lat": False, "lon": False, "n_events": ":,", "n_delayed": ":,",
                                     "pct_delayed": True, "median_delay_s": True},
                         labels={metric: METRICS[metric]}, zoom=9.2, center={"lat": 59.33, "lon": 18.07},
                         map_style="carto-positron",
                         title=f"Stations, coloured by {METRICS[metric].lower()}, sized by number of arrivals")
    top = df.nlargest(20, metric)[["station_name", "n_events", "n_delayed", "pct_delayed",
                                   "mean_delay_s", "median_delay_s", "p90_delay_s"]]
    busiest = df.nlargest(20, "n_events")[["station_name", "n_events", "pct_delayed", "median_delay_s"]]
    return [dcc.Graph(figure=style(fig, 650)),
            html.Div(className="two", children=[
                html.Div([html.H3(f"Top 20 by {METRICS[metric].lower()}"), table(top, 20)]),
                html.Div([html.H3("Busiest 20 stations"), table(busiest, 20)]),
            ])]


def hours_tab(mode, metric):
    df = by_hour[by_hour["day_type"] == "All"]
    df = df[df["mode"] != "All"] if mode == "All" else df[df["mode"] == mode]
    fig = px.line(df, x="hour_local", y=metric, color="mode", markers=True, color_discrete_map=MODE_COLORS,
                  hover_data={"n_events": ":,"},
                  labels={metric: METRICS[metric], "hour_local": "Hour of day (Stockholm time)"},
                  title=f"{METRICS[metric]} by hour of day")
    fig.update_traces(line_width=2, marker_size=8)
    fig.update_xaxes(dtick=2)
    volume = px.bar(df, x="hour_local", y="n_events", color="mode", color_discrete_map=MODE_COLORS,
                    labels={"n_events": "Observed arrivals", "hour_local": "Hour of day (Stockholm time)"},
                    title="Number of observed arrivals by hour")
    volume.update_xaxes(dtick=2)
    return [dcc.Graph(figure=style(fig)), dcc.Graph(figure=style(volume, 360))]


def daytype_tab(mode, metric):
    hourly = by_hour[(by_hour["mode"] == mode) & (by_hour["day_type"] != "All")]
    fig = px.line(hourly, x="hour_local", y=metric, color="day_type", markers=True,
                  color_discrete_map=DAY_COLORS, hover_data={"n_events": ":,"},
                  labels={metric: METRICS[metric], "hour_local": "Hour of day (Stockholm time)"},
                  title=f"{METRICS[metric]} by hour, weekday vs weekend ({mode})")
    fig.update_traces(line_width=2, marker_size=8)
    fig.update_xaxes(dtick=2)

    hist = histogram[(histogram["mode"] == mode) & (histogram["day_type"] != "All")].copy()
    hist["share"] = 100 * hist["n_events"] / hist.groupby("day_type")["n_events"].transform("sum")
    dist = go.Figure([go.Scatter(x=g["bin_s"] / 60, y=g["share"], name=day, line_shape="hv",
                                 line=dict(color=DAY_COLORS[day], width=2),
                                 hovertemplate="%{x:.1f} min: %{y:.2f}% of arrivals<extra>" + day + "</extra>")
                      for day, g in hist.groupby("day_type")])
    dist.update_layout(title="Delay distribution (30 s bins, clipped to -5 / +20 min)",
                       xaxis_title="Delay (minutes)", yaxis_title="% of arrivals")

    summary = by_day_type[by_day_type["mode"] == mode].drop(columns="mode")
    per_mode = by_day_type[(by_day_type["day_type"] != "All") & (by_day_type["mode"] != "All")]
    bars = px.bar(per_mode, x="mode", y=metric, color="day_type", barmode="group",
                  color_discrete_map=DAY_COLORS, labels={metric: METRICS[metric], "mode": ""},
                  title=f"{METRICS[metric]} per mode")
    return [dcc.Graph(figure=style(fig)),
            html.Div(className="two", children=[dcc.Graph(figure=style(dist, 400)),
                                                dcc.Graph(figure=style(bars, 400))]),
            html.H3("Summary"), table(summary)]


def qa_tab(mode):
    daily = by_date[by_date["mode"] == mode]
    fig = px.bar(daily, x="service_date", y="n_events", color="day_type", color_discrete_map=DAY_COLORS,
                 labels={"n_events": "Observed arrivals", "service_date": ""},
                 title=f"Observed arrivals per service date ({mode})")
    return [html.H3("Pipeline row counts"), table(qa, 20), dcc.Graph(figure=style(fig, 380))]


@app.callback(Output("content", "children"),
              Input("tab", "value"), Input("mode", "value"), Input("metric", "value"), Input("min-events", "value"))
def render(tab, mode, metric, min_events):
    if tab == "lines":
        return lines_tab(mode, metric, min_events)
    if tab == "stations":
        return stations_tab(mode, metric, min_events)
    if tab == "hours":
        return hours_tab(mode, metric)
    if tab == "daytype":
        return daytype_tab(mode, metric)
    return qa_tab(mode)


app.index_string = app.index_string.replace("</head>", """<style>
body { margin: 0; background: #fcfcfb; color: #0b0b0b; font-family: system-ui, sans-serif; }
.page { max-width: 1200px; margin: 0 auto; padding: 16px; }
h1 { margin: 8px 0 4px; font-size: 26px; } .sub { color: #52514e; margin: 0 0 16px; }
h3 { font-size: 15px; margin: 16px 0 6px; }
.controls { display: grid; grid-template-columns: 200px 260px 1fr; gap: 16px; margin-bottom: 12px; }
.controls label { font-size: 13px; color: #52514e; display: flex; flex-direction: column; gap: 4px; }
.two { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
@media (max-width: 800px) { .controls, .two { grid-template-columns: 1fr; } }
</style></head>""")

if __name__ == "__main__":
    app.run(debug=False)
