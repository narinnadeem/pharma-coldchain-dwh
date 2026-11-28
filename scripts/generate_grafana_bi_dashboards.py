"""Writes the 3 BI dashboards as clean, executive-grade Grafana provisioning JSON.
Follows BI best practices: 3-second scannability, zero-baseline bar gauges (min=0),
semantic color discipline, zero text clipping, zero horizontal table scrollbars,
and full metric contracts embedded in panel info tooltips + governance footer.

Run from project root:  python scripts\\generate_grafana_bi_dashboards.py
"""
import json
import os
from build_bi_layer import TARGETS, METRICS, LIMITS_NOTE

DS = {"type": "grafana-postgresql-datasource", "uid": "warehouse-pg"}
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "grafana", "dashboards")
_id = [100]

# Colour-blind-safe, cohesive corporate palette
CH = {"Retail": "#2563EB", "Hospital": "#D97706", "E-commerce": "#059669"}
DASH = {"Retail": None, "Hospital": [8, 6], "E-commerce": [3, 5]}
CMIN = TARGETS["consumption_ratio_min_pct"]
UMAX = TARGETS["unfulfilled_share_max_pct"]
MMIN = TARGETS["gross_margin_min_pct"]

METRIC_DESC = {m[0]: f"{m[1]} | Grain: {m[2]} | Target: {m[4]} | Exclusions: {m[5]}" for m in METRICS}


def _panel(typ, title, x, y, w, h, targets=None, defaults=None, options=None, overrides=None, desc=""):
    _id[0] += 1
    p = {
        "id": _id[0],
        "type": typ,
        "title": title,
        "description": desc,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults or {}, "overrides": overrides or []},
        "options": options or {},
    }
    if targets:
        p["datasource"] = DS
        p["targets"] = [
            {
                "refId": chr(65 + i),
                "datasource": DS,
                "editorMode": "code",
                "format": f,
                "rawQuery": True,
                "rawSql": q,
            }
            for i, (q, f) in enumerate(targets)
        ]
    return p


def ov(name, props):
    return {
        "matcher": {"id": "byName", "options": name},
        "properties": [{"id": k, "value": v} for k, v in props.items()],
    }


def fixed(c):
    return {"mode": "fixed", "fixedColor": c}


def thresh_steps(steps):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}


# ---------------------------------------------------------------- building blocks
def text(title, md, x, y, w, h):
    return _panel("text", title, x, y, w, h, options={"mode": "markdown", "content": md})


def banner_stat(title, sql, x, y, w, h=3, size=15, bg_color="#1E3A8A", desc=""):
    return _panel(
        "stat",
        title,
        x,
        y,
        w,
        h,
        [(sql, "table")],
        {"unit": "none", "color": fixed(bg_color)},
        {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/", "values": False},
            "colorMode": "background",
            "graphMode": "none",
            "textMode": "value",
            "justifyMode": "center",
            "orientation": "auto",
            "text": {"valueSize": size},
        },
        desc=desc,
    )


def kpi(title, col, unit, dec, x, w=6, y=3, d_unit=None, d_dec=None, steps=None, base_color="#1E40AF", desc=""):
    sql = (
        f'SELECT {col} AS "Now", {col} - LAG({col}) OVER (ORDER BY refresh_no) AS "vs Prev Refresh" '
        f"FROM bi.refresh_history ORDER BY refresh_no DESC LIMIT 1"
    )
    defaults = {
        "unit": unit,
        "decimals": dec,
        "color": {"mode": "thresholds"} if steps else fixed(base_color),
    }
    if steps:
        defaults["thresholds"] = thresh_steps(steps)

    return _panel(
        "stat",
        title,
        x,
        y,
        w,
        3,
        [(sql, "table")],
        defaults,
        {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "background",
            "graphMode": "none",
            "textMode": "value_and_name",
            "orientation": "horizontal",
            "justifyMode": "center",
            "text": {"valueSize": 28, "titleSize": 12},
        },
        [
            ov(
                "vs Prev Refresh",
                {
                    "unit": d_unit or unit,
                    "decimals": d_dec if d_dec is not None else dec,
                    "color": fixed("#1E293B"),
                },
            )
        ],
        desc=desc,
    )


def ts(title, targets, x, y, w, h, unit="short", axis_label=None, min_=None, overrides=None, desc="", fill=16):
    custom = {
        "drawStyle": "line",
        "lineInterpolation": "smooth",
        "lineWidth": 2.5,
        "showPoints": "never",
        "pointSize": 4,
        "fillOpacity": fill,
        "gradientMode": "opacity",
    }
    if axis_label:
        custom["axisLabel"] = axis_label
    d = {"unit": unit, "custom": custom}
    if min_ is not None:
        d["min"] = min_
    return _panel(
        "timeseries",
        title,
        x,
        y,
        w,
        h,
        targets,
        d,
        {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi"},
        },
        overrides,
        desc,
    )


def bargauge(title, sql, x, y, w, h, unit="short", dec=0, min_=0, max_=None, color="#2563EB", desc=""):
    """Clean horizontal bar-gauge with explicit zero baseline (min=0) so bar lengths are honest."""
    defaults = {
        "unit": unit,
        "decimals": dec,
        "min": min_,
        "color": fixed(color),
    }
    if max_ is not None:
        defaults["max"] = max_
    return _panel(
        "bargauge",
        title,
        x,
        y,
        w,
        h,
        [(sql, "table")],
        defaults,
        {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
            "orientation": "horizontal",
            "displayMode": "gradient",
            "valueMode": "color",
            "namePlacement": "left",
            "showUnfilled": True,
            "minVizWidth": 8,
            "minVizHeight": 14,
            "text": {"valueSize": 13, "titleSize": 13},
        },
        desc=desc,
    )


def bars(title, sql, x, y, w, h, horizontal=False, unit="short", dec=None, overrides=None, legend=False, desc=""):
    d = {"unit": unit, "min": 0, "color": fixed("#2563EB"), "custom": {"fillOpacity": 88, "gradientMode": "none"}}
    if dec is not None:
        d["decimals"] = dec
    return _panel(
        "barchart",
        title,
        x,
        y,
        w,
        h,
        [(sql, "table")],
        d,
        {
            "orientation": "horizontal" if horizontal else "vertical",
            "showValue": "always",
            "stacking": "none",
            "legend": {"showLegend": legend, "displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "single"},
            "barWidth": 0.65,
            "groupWidth": 0.7,
            "xTickLabelRotation": 0,
        },
        overrides,
        desc,
    )


def table(title, sql, x, y, w, h, units=None, status_col=None, col_widths=None, desc=""):
    ovs = [ov(c, {"unit": u, "decimals": dec}) for c, (u, dec) in (units or {}).items()]
    for c, cw in (col_widths or {}).items():
        ovs.append(ov(c, {"custom.width": cw}))
    if status_col:
        ovs.append(
            ov(
                status_col,
                {
                    "custom.width": 130,
                    "custom.cellOptions": {"type": "color-background", "mode": "basic"},
                    "mappings": [
                        {
                            "type": "value",
                            "options": {
                                "\u25B2 On target": {"color": "dark-green", "index": 0},
                                "\u25BC Below target": {"color": "dark-red", "index": 1},
                                "\u25B2 Within limit": {"color": "dark-green", "index": 2},
                                "\u25BC Above limit": {"color": "dark-red", "index": 3},
                            },
                        }
                    ],
                },
            )
        )
    return _panel(
        "table",
        title,
        x,
        y,
        w,
        h,
        [(sql, "table")],
        {"unit": "locale"},
        {"showHeader": True, "cellHeight": "sm"},
        ovs,
        desc,
    )


def dash(uid, title, desc, panels):
    return {
        "uid": uid,
        "title": title,
        "description": desc,
        "tags": ["pharma", "bi"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 4,
        "refresh": "10s",
        "time": {"from": "2022-12-01T00:00:00.000Z", "to": "now+1y"},
        "editable": True,
        "graphTooltip": 1,
        "panels": panels,
        "templating": {"list": []},
        "annotations": {"list": []},
    }


REFRESH = (
    "SELECT 'Refresh #' || refresh_no || ' | ' || to_char(refreshed_at, 'YYYY-MM-DD HH24:MI') || ' UTC | Data to ' || "
    "to_char(GREATEST(sales_data_to, hospital_data_to), 'YYYY-MM-DD') AS refreshed FROM bi.refresh_history ORDER BY refresh_no DESC LIMIT 1"
)
LATEST = "(SELECT * FROM bi.refresh_history ORDER BY refresh_no DESC LIMIT 1)"


def header(question, decision, takeaway_sql):
    return [
        text("Purpose & Decision", f"**Q:** {question}  \n**Action:** {decision}", 0, 0, 7, 3),
        banner_stat("Key Takeaway (Auto-Updated on Refresh)", takeaway_sql, 7, 0, 11, 3, size=14, bg_color="#1E3A8A"),
        banner_stat("Pipeline Refresh Status", REFRESH, 18, 0, 6, 3, size=14, bg_color="#0F766E"),
    ]


def footer(metric_names):
    contracts = " | ".join(f"**{n}**: {METRIC_DESC[n]}" for n in metric_names if n in METRIC_DESC)
    md = f"**Governance & Data Lineage:** {LIMITS_NOTE}  \n**Metric Contracts (hover `(i)` on panels for details):** {contracts}"
    return text("Data Governance & Metric Contracts", md, 0, 29, 24, 3)


# ================================================================ BI 1
hm = "bi.hospital_monthly"
HOSP_AGG = (
    f"SELECT hospital_name, MAX(hospital_type) AS hospital_type, SUM(forecast_packs) AS f, SUM(consumed_packs) AS c, "
    f"SUM(unfulfilled_packs) AS u, ROUND(SUM(unfulfilled_packs)*100.0/NULLIF(SUM(forecast_packs),0),2) AS pct, "
    f"ROUND(AVG(avg_service_level_pct),1) AS sl FROM {hm} GROUP BY 1"
)
t1 = (
    f"SELECT 'Hospitals consumed ' || ROUND(k.consumption_ratio_pct,1) || '% of forecast (' || "
    f"CASE WHEN k.consumption_ratio_pct >= {CMIN} THEN 'on target' ELSE 'below {CMIN:g}% target' END || '); ' || "
    f"ROUND(k.unfulfilled_pct,2) || '% went unfulfilled (' || "
    f"CASE WHEN k.unfulfilled_pct <= {UMAX} THEN 'within' ELSE 'above' END || ' {UMAX:g}% limit). Largest gap: ' || "
    f"t.hospital_name || ' (' || TO_CHAR(t.u, 'FM999,999') || ' packs).' AS takeaway FROM {LATEST} k, "
    f"(SELECT hospital_name, SUM(unfulfilled_packs) AS u FROM {hm} GROUP BY 1 ORDER BY 2 DESC LIMIT 1) t"
)

p1 = header(
    "Are hospitals consuming forecasted stock, and where is unfulfilled demand concentrated?",
    "Prioritize replenishment for hospitals and medicines exceeding the 2.5% unfulfilled threshold.",
    t1,
) + [
    kpi(
        f"Consumption Ratio (Target >= {CMIN:g}%)",
        "consumption_ratio_pct",
        "percent",
        1,
        0,
        steps=[(None, "#B91C1C"), (CMIN, "#15803D")],
        desc=METRIC_DESC["Consumption ratio"],
    ),
    kpi(
        f"Unfulfilled Share (Limit <= {UMAX:g}%)",
        "unfulfilled_pct",
        "percent",
        2,
        6,
        steps=[(None, "#15803D"), (UMAX + 0.01, "#B91C1C")],
        desc=METRIC_DESC["Unfulfilled share of forecast"],
    ),
    kpi("Total Unfulfilled Demand (Packs)", "unfulfilled_packs", "short", 0, 12, base_color="#B45309"),
    banner_stat(
        f"Hospitals Above {UMAX:g}% Unfulfilled Limit",
        f"SELECT COUNT(*) FILTER (WHERE pct > {UMAX}) || ' of ' || COUNT(*) || ' Hospitals' AS above FROM ({HOSP_AGG}) x",
        18,
        3,
        6,
        h=3,
        size=24,
        bg_color="#991B1B",
    ),
    ts(
        "Monthly Forecast vs Actual Consumption (Packs)",
        [
            (
                f'SELECT month::timestamp AS time, SUM(forecast_packs) AS "Forecast", SUM(consumed_packs) AS "Consumed (Actual)" FROM {hm} GROUP BY 1 ORDER BY 1',
                "time_series",
            )
        ],
        0,
        6,
        12,
        8,
        "short",
        "Packs / Month",
        0,
        [
            ov("Forecast", {"color": fixed("#64748B"), "custom.lineStyle": {"fill": "dash", "dash": [8, 6]}, "custom.fillOpacity": 0}),
            ov("Consumed (Actual)", {"color": fixed("#2563EB"), "custom.fillOpacity": 18}),
        ],
        "Dashed = forecast demand; solid blue = actual hospital consumption.",
    ),
    ts(
        f"Monthly Unfulfilled Demand Share vs {UMAX:g}% Limit",
        [
            (
                f'SELECT month::timestamp AS time, ROUND(SUM(unfulfilled_packs)*100.0/NULLIF(SUM(forecast_packs),0),2) AS "Unfulfilled Share (%)" FROM {hm} GROUP BY 1 ORDER BY 1',
                "time_series",
            ),
            (
                f'SELECT month::timestamp AS time, {UMAX} AS "Assumed Limit ({UMAX:g}%)" FROM (SELECT DISTINCT month FROM {hm}) m ORDER BY 1',
                "time_series",
            ),
        ],
        12,
        6,
        12,
        8,
        "percent",
        "% of Forecast",
        0,
        [
            ov("Unfulfilled Share (%)", {"color": fixed("#EA580C"), "custom.fillOpacity": 20}),
            ov(
                f"Assumed Limit ({UMAX:g}%)",
                {"color": fixed("#DC2626"), "custom.lineStyle": {"fill": "dash", "dash": [8, 6]}, "custom.fillOpacity": 0},
            ),
        ],
    ),
    bargauge(
        "Unfulfilled Demand by Hospital (Packs, Zero-Baseline)",
        f'SELECT hospital_name AS "Hospital", u AS "Unfulfilled Packs" FROM ({HOSP_AGG}) x ORDER BY u DESC',
        0,
        14,
        12,
        8,
        unit="short",
        dec=0,
        min_=0,
        color="#EA580C",
    ),
    table(
        "Top 10 Medicines by Unfulfilled Demand",
        'SELECT generic_name AS "Medicine", therapeutic_class AS "Therapeutic Class", forecast_packs AS "Forecast", '
        'unfulfilled_packs AS "Unfulfilled", unfulfilled_pct AS "Unfulfilled (%)" FROM bi.hospital_medicine_gap ORDER BY unfulfilled_packs DESC LIMIT 10',
        12,
        14,
        12,
        8,
        {"Unfulfilled (%)": ("percent", 2)},
        col_widths={"Medicine": 170, "Therapeutic Class": 180},
    ),
    table(
        "Hospital Fulfillment Scorecard (All 12 Hospitals)",
        f'SELECT hospital_name AS "Hospital", hospital_type AS "Type", f AS "Forecast", c AS "Consumed", u AS "Unfulfilled", '
        f'pct AS "Unfulfilled (%)", sl AS "Service Level (%)", CASE WHEN pct > {UMAX} THEN \'\u25BC Above limit\' ELSE \'\u25B2 Within limit\' END AS "Status" '
        f"FROM ({HOSP_AGG}) x ORDER BY u DESC",
        0,
        22,
        24,
        7,
        {"Unfulfilled (%)": ("percent", 2), "Service Level (%)": ("percent", 1)},
        "Status",
        col_widths={"Hospital": 230, "Type": 190},
    ),
    footer(["Consumption ratio", "Unfulfilled share of forecast", "Change vs previous refresh"]),
]

# ================================================================ BI 2
cm = "bi.channel_monthly"
CH_AGG = (
    f"SELECT channel_group, SUM(net_sales) AS ns, SUM(gross_margin) AS gm, SUM(units_sold) AS u, SUM(transactions) AS tx, "
    f"ROUND(SUM(gross_margin)*100.0/NULLIF(SUM(net_sales),0),2) AS m, ROUND(SUM(net_sales)*100.0/SUM(SUM(net_sales)) OVER (),1) AS share FROM {cm} GROUP BY 1"
)
t2 = (
    f"SELECT a.channel_group || ' generates ' || a.share || '% of net sales. Overall gross margin is ' || ROUND(k.gross_margin_pct,2) || '% (' || "
    f"CASE WHEN k.gross_margin_pct >= {MMIN} THEN 'on target' ELSE 'below {MMIN:g}% target' END || '); margin spread across channels is only ' || "
    f"ROUND(b.spread,2) || ' pts - channel growth is driven by volume.' AS takeaway FROM {LATEST} k, "
    f"(SELECT channel_group, share FROM ({CH_AGG}) x ORDER BY ns DESC LIMIT 1) a, (SELECT MAX(m) - MIN(m) AS spread FROM ({CH_AGG}) y) b"
)
ch_ovs = [
    ov(
        k,
        {
            "color": fixed(c),
            "custom.fillOpacity": 15,
            **({"custom.lineStyle": {"fill": "dash", "dash": DASH[k]}} if DASH[k] else {}),
        },
    )
    for k, c in CH.items()
]

p2 = header(
    "Which sales channel drives revenue, and how do gross margins compare?",
    "Allocate commercial sales effort and review discounting across Retail, Hospital, and E-commerce.",
    t2,
) + [
    kpi("Total Net Sales (USD)", "net_sales", "currencyUSD", 1, 0, base_color="#1E40AF", desc=METRIC_DESC["Net sales"]),
    kpi("Total Gross Margin (USD)", "gross_margin", "currencyUSD", 1, 6, base_color="#0F766E"),
    kpi(
        f"Gross Margin % (Target >= {MMIN:g}%)",
        "gross_margin_pct",
        "percent",
        2,
        12,
        d_unit="suffix: pts",
        d_dec=2,
        steps=[(None, "#B91C1C"), (MMIN, "#15803D")],
        desc=METRIC_DESC["Gross margin %"],
    ),
    banner_stat(
        "Leading Channel (Revenue Share)",
        f"SELECT channel_group || ' (' || share || '%)' AS largest FROM ({CH_AGG}) x ORDER BY ns DESC LIMIT 1",
        18,
        3,
        6,
        h=3,
        size=24,
        bg_color="#3730A3",
    ),
    bars(
        "Net Sales vs Gross Margin by Channel Group (USD)",
        f'SELECT channel_group AS "Channel Group", ROUND(ns) AS "Net Sales", ROUND(gm) AS "Gross Margin" FROM ({CH_AGG}) x ORDER BY ns DESC',
        0,
        6,
        12,
        8,
        False,
        "currencyUSD",
        0,
        [ov("Net Sales", {"color": fixed("#2563EB")}), ov("Gross Margin", {"color": fixed("#10B981")})],
        True,
    ),
    table(
        "Channel Profitability Scorecard",
        f'SELECT channel_group AS "Channel Group", u AS "Packs Sold", ROUND(ns) AS "Net Sales (USD)", share AS "Revenue Share (%)", '
        f'm AS "Gross Margin (%)", CASE WHEN m >= {MMIN} THEN \'\u25B2 On target\' ELSE \'\u25BC Below target\' END AS "Status" FROM ({CH_AGG}) x ORDER BY ns DESC',
        12,
        6,
        12,
        8,
        {"Revenue Share (%)": ("percent", 1), "Gross Margin (%)": ("percent", 2)},
        "Status",
        col_widths={"Channel Group": 140},
    ),
    ts(
        "Monthly Net Sales Trend by Channel Group (USD)",
        [
            (
                f"SELECT month::timestamp AS time, channel_group AS metric, SUM(net_sales) AS value FROM {cm} GROUP BY 1,2 ORDER BY 1",
                "time_series",
            )
        ],
        0,
        14,
        12,
        8,
        "currencyUSD",
        "USD / Month",
        0,
        ch_ovs,
    ),
    ts(
        f"Monthly Gross Margin % Trend vs {MMIN:g}% Target",
        [
            (
                f"SELECT month::timestamp AS time, channel_group AS metric, ROUND(SUM(gross_margin)*100.0/NULLIF(SUM(net_sales),0),2) AS value FROM {cm} GROUP BY 1,2 ORDER BY 1",
                "time_series",
            ),
            (
                f'SELECT month::timestamp AS time, {MMIN} AS "Assumed Target ({MMIN:g}%)" FROM (SELECT DISTINCT month FROM {cm}) m ORDER BY 1',
                "time_series",
            ),
        ],
        12,
        14,
        12,
        8,
        "percent",
        "Margin %",
        15,
        ch_ovs
        + [
            ov(
                f"Assumed Target ({MMIN:g}%)",
                {"color": fixed("#DC2626"), "custom.lineStyle": {"fill": "dash", "dash": [8, 6]}, "custom.fillOpacity": 0},
            )
        ],
    ),
    table(
        "Channel Breakdown: Sales Channel x Pharmacy Type",
        f'SELECT channel_group AS "Channel Group", sales_channel AS "Sales Channel", pharmacy_type AS "Pharmacy Type", SUM(transactions) AS "Transactions", '
        f'SUM(units_sold) AS "Packs Sold", ROUND(SUM(net_sales)) AS "Net Sales (USD)", ROUND(SUM(gross_margin)) AS "Gross Margin (USD)", '
        f'ROUND(SUM(gross_margin)*100.0/NULLIF(SUM(net_sales),0),2) AS "Gross Margin (%)" FROM {cm} GROUP BY 1,2,3 ORDER BY 6 DESC',
        0,
        22,
        24,
        7,
        {"Gross Margin (%)": ("percent", 2)},
        col_widths={"Channel Group": 140, "Sales Channel": 170, "Pharmacy Type": 200},
    ),
    footer(["Net sales", "Gross margin %", "Channel group", "Change vs previous refresh"]),
]

# ================================================================ BI 3
mv = "bi.medicine_velocity"
t3 = (
    f"SELECT 'Top 10 fast-moving medicines account for ' || ROUND(SUM(units_sold) FILTER (WHERE movement = 'Fast (Top 10)') * 100.0 / SUM(units_sold), 1) || "
    f"'% of all packs sold. Fastest: ' || (SELECT generic_name FROM {mv} ORDER BY overall_rank LIMIT 1) || '; Slowest: ' || "
    f"(SELECT generic_name FROM {mv} ORDER BY overall_rank DESC LIMIT 1) || '.' AS takeaway FROM {mv}"
)

p3 = header(
    "Which medicines move fastest vs slowest across therapeutic classes?",
    "Protect safety stock for top-10 movers and minimize cold-storage spoilage on slow SKUs.",
    t3,
) + [
    kpi("Total Packs Sold", "units_sold", "short", 0, 0, base_color="#1E40AF"),
    kpi("Total Net Sales (USD)", "net_sales", "currencyUSD", 1, 6, base_color="#0F766E"),
    _panel(
        "stat",
        "Top 10 Fast Movers' Share of Volume",
        12,
        3,
        6,
        3,
        [
            (
                f'SELECT ROUND(SUM(units_sold) FILTER (WHERE movement = \'Fast (Top 10)\') * 100.0 / SUM(units_sold), 1) AS "Share of Packs" FROM {mv}',
                "table",
            )
        ],
        {"unit": "percent", "decimals": 1, "color": fixed("#3730A3")},
        {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "background",
            "graphMode": "none",
            "textMode": "value",
            "justifyMode": "center",
            "text": {"valueSize": 28},
        },
        desc=METRIC_DESC["Fast / slow medicine"],
    ),
    banner_stat(
        "Cold-Chain Flagged SKUs (ColdChainFlag = Y)",
        f"SELECT COUNT(*) FILTER (WHERE cold_chain_flag = 'Y') || ' of ' || COUNT(*) || ' Medicines' AS flagged FROM {mv}",
        18,
        3,
        6,
        h=3,
        size=24,
        bg_color="#0369A1",
    ),
    bargauge(
        "Top 10 Fast-Moving Medicines (Packs Sold, Min=0)",
        f'SELECT generic_name AS "Medicine", units_sold AS "Packs Sold" FROM {mv} WHERE movement = \'Fast (Top 10)\' ORDER BY overall_rank ASC',
        0,
        6,
        12,
        8,
        unit="short",
        dec=0,
        min_=0,
        color="#059669",
    ),
    bargauge(
        "Bottom 10 Slow-Moving Medicines (Packs Sold, Min=0)",
        f'SELECT generic_name AS "Medicine", units_sold AS "Packs Sold" FROM {mv} WHERE movement = \'Slow (Bottom 10)\' ORDER BY overall_rank DESC',
        12,
        6,
        12,
        8,
        unit="short",
        dec=0,
        min_=0,
        color="#D97706",
    ),
    bargauge(
        "Top 10 Therapeutic Classes by Packs Sold (Min=0)",
        f'SELECT therapeutic_class AS "Therapeutic Class", SUM(units_sold) AS "Packs Sold" FROM {mv} GROUP BY 1 ORDER BY 2 DESC LIMIT 10',
        0,
        14,
        12,
        8,
        unit="short",
        dec=0,
        min_=0,
        color="#2563EB",
    ),
    table(
        "Dedicated Cold-Chain Medicines (ColdChainFlag = Y)",
        f'SELECT overall_rank AS "Rank", generic_name AS "Medicine", therapeutic_class AS "Therapeutic Class", units_sold AS "Packs Sold", ROUND(net_sales) AS "Net Sales (USD)", movement AS "Movement" FROM {mv} WHERE cold_chain_flag = \'Y\' ORDER BY overall_rank',
        12,
        14,
        12,
        8,
        col_widths={"Rank": 65, "Medicine": 150, "Therapeutic Class": 170},
    ),
    table(
        "Fastest vs Slowest Medicine inside Each Therapeutic Class",
        f"""SELECT therapeutic_class AS "Therapeutic Class", generic_name AS "Medicine", rank_in_class AS "Class Rank", cold_chain_flag AS "Cold Chain", units_sold AS "Packs Sold", ROUND(net_sales) AS "Net Sales (USD)", movement AS "Movement" FROM {mv} m
            WHERE rank_in_class = 1 OR rank_in_class = (SELECT MAX(rank_in_class) FROM {mv} x WHERE x.therapeutic_class = m.therapeutic_class)
            ORDER BY therapeutic_class, rank_in_class""",
        0,
        22,
        24,
        7,
        col_widths={"Therapeutic Class": 240, "Medicine": 200, "Class Rank": 105, "Cold Chain": 105},
    ),
    footer(["Fast / slow medicine", "Change vs previous refresh"]),
]

os.makedirs(OUT, exist_ok=True)
for fn, d in [
    (
        "04_bi_hospital.json",
        dash("bi-hospital", "BI 1 - Hospital Forecast vs Actual and Unfulfilled Demand", "Supply planner view: forecast vs consumption and unfulfilled demand", p1),
    ),
    (
        "05_bi_channel.json",
        dash("bi-channel", "BI 2 - Channel Revenue Split and Gross Margin", "Commercial view: revenue and margin by channel group", p2),
    ),
    (
        "06_bi_velocity.json",
        dash("bi-velocity", "BI 3 - Fast vs Slow Moving Medicines by Therapeutic Class", "Inventory view: fast and slow movers", p3),
    ),
]:
    with open(os.path.join(OUT, fn), "w", encoding="utf-8") as f:
        json.dump(d, f, indent=1, ensure_ascii=False)
    print("wrote", fn, f"({len(d['panels'])} panels)")