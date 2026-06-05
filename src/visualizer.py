import os
from datetime import datetime, timezone, timedelta

import folium

from config import OUTPUT_DIR, MAP_FILENAME


def build_trajectory_map(
    pings_a: list[dict],
    pings_b: list[dict],
    collision_time: datetime,
    mmsi_a: int,
    mmsi_b: int,
    name_a: str,
    name_b: str,
    collision_lat: float,
    collision_lon: float,
) -> str:
    
    m = folium.Map(
        location=[collision_lat, collision_lon],
        zoom_start=12,
        tiles="CartoDB positron",
    )

    color_a = "#1D9E75"
    color_b = "#534AB7"

    # Helper to add one vessels trajectory
    def add_vessel(pings, color, mmsi, name, label_prefix):
        if not pings:
            return

        before = [p for p in pings if p["ts"] <= collision_time]
        after  = [p for p in pings if p["ts"] >  collision_time]

        coords_before = [(p["lat"], p["lon"]) for p in before]
        coords_after  = [(p["lat"], p["lon"]) for p in after]

        if len(coords_before) >= 2:
            folium.PolyLine(
                coords_before,
                color=color,
                weight=3,
                opacity=0.9,
                tooltip=f"{name} (before collision)",
            ).add_to(m)

        if len(coords_after) >= 2:
            folium.PolyLine(
                coords_after,
                color=color,
                weight=3,
                opacity=0.6,
                dash_array="8",
                tooltip=f"{name} (after collision)",
            ).add_to(m)

        for p in pings:
            folium.CircleMarker(
                location=(p["lat"], p["lon"]),
                radius=3,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                tooltip=f"{name} - {p['ts'].strftime('%H:%M:%S UTC')} - SOG: {p.get('sog', '?')} kn",
            ).add_to(m)

        if coords_before:
            folium.Marker(
                location=coords_before[0],
                tooltip=f"{label_prefix} START - {before[0]['ts'].strftime('%H:%M UTC')}",
                icon=folium.Icon(color="green", icon="play", prefix="fa"),
            ).add_to(m)

        all_coords = coords_before + coords_after
        if all_coords:
            folium.Marker(
                location=all_coords[-1],
                tooltip=f"{label_prefix} END",
                icon=folium.Icon(color="gray", icon="stop", prefix="fa"),
            ).add_to(m)

    add_vessel(pings_a, color_a, mmsi_a, name_a or f"MMSI {mmsi_a}", "Vessel A")
    add_vessel(pings_b, color_b, mmsi_b, name_b or f"MMSI {mmsi_b}", "Vessel B")

    folium.Marker(
        location=(collision_lat, collision_lon),
        tooltip=(
            f"COLLISION - {collision_time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
            f"{name_a or mmsi_a} x {name_b or mmsi_b}"
        ),
        icon=folium.Icon(color="red", icon="warning-sign", prefix="glyphicon"),
    ).add_to(m)

    legend_html = f"""
    <div style="position: fixed; bottom: 30px; left: 30px; z-index: 1000;
                background: white; padding: 12px 16px; border-radius: 6px;
                border: 1px solid #ccc; font-size: 13px; line-height: 1.8;">
        <b>Vessel trajectories</b><br>
        <span style="color:{color_a};">&#9644;</span>
        {name_a or f'MMSI {mmsi_a}'} (MMSI {mmsi_a})<br>
        <span style="color:{color_b};">&#9644;</span>
        {name_b or f'MMSI {mmsi_b}'} (MMSI {mmsi_b})<br>
        <span style="color:red;">&#9873;</span> Collision point<br>
        <hr style="margin:4px 0;">
        Solid line = before &nbsp; Dashed = after<br>
        Window: +-10 minutes around collision
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    out_path = os.path.join(OUTPUT_DIR, MAP_FILENAME)
    m.save(out_path)
    return out_path


def build_top5_maps(
    top5_rows: list,
    df_clean,
    extract_trajectory_fn,
    collision_time_winner: "datetime",
    output_dir: str = OUTPUT_DIR,
) -> list[str]:
 
    from pyspark.sql import functions as _F
    from datetime import timezone as _tz

    vessel_windows = []
    for r in top5_rows:
        for mmsi_col, ts_col in [("mmsi_a", "ts_a"), ("mmsi_b", "ts_b")]:
            mmsi = r[mmsi_col]
            event_unix = int(r[ts_col].replace(tzinfo=_tz.utc).timestamp())
            vessel_windows.append((mmsi, event_unix - 600, event_unix + 600))

    # Build one filter covering all mmsi/time windows
    from functools import reduce
    condition = reduce(
        lambda a, b: a | b,
        [
            (_F.col("mmsi") == mmsi) &
            (_F.col("ts_unix").between(start, end))
            for mmsi, start, end in vessel_windows
        ]
    )
    all_pings = (
        df_clean
        .filter(condition)
        .select("mmsi", "ts", "ts_unix", "lat", "lon", "sog", "cog")
        .collect()
    )

    from collections import defaultdict
    pings_by_mmsi = defaultdict(list)
    for row in all_pings:
        pings_by_mmsi[row["mmsi"]].append({
            "ts":  row["ts"].replace(tzinfo=_tz.utc),
            "lat": row["lat"],
            "lon": row["lon"],
            "sog": row["sog"],
            "cog": row["cog"],
            "ts_unix": row["ts_unix"],
        })
    for mmsi in pings_by_mmsi:
        pings_by_mmsi[mmsi].sort(key=lambda p: p["ts_unix"])

    paths = []
    for i, r in enumerate(top5_rows, 1):
        mmsi_a = r["mmsi_a"]
        mmsi_b = r["mmsi_b"]
        name_a = r["name_a"] or f"MMSI {mmsi_a}"
        name_b = r["name_b"] or f"MMSI {mmsi_b}"

        event_time = r["ts_a"].replace(tzinfo=_tz.utc)
        event_unix = int(event_time.timestamp())
        start_unix = event_unix - 600
        end_unix   = event_unix + 600

        pings_a = [p for p in pings_by_mmsi[mmsi_a]
                   if start_unix <= p["ts_unix"] <= end_unix]
        pings_b = [p for p in pings_by_mmsi[mmsi_b]
                   if start_unix <= p["ts_unix"] <= end_unix]

        col_lat = (r["lat_a"] + r["lat_b"]) / 2
        col_lon = (r["lon_a"] + r["lon_b"]) / 2

        filename = f"collision_trajectory_top{i}.html"
        if i == 1:
            primary_path = build_trajectory_map(
                pings_a, pings_b, event_time,
                mmsi_a, mmsi_b, name_a, name_b,
                col_lat, col_lon,
            )
            paths.append(primary_path)

        import folium
        m = folium.Map(location=[col_lat, col_lon], zoom_start=12,
                       tiles="CartoDB positron")
        color_a, color_b = "#1D9E75", "#534AB7"

        def _add(pings, color, name, label):
            if not pings:
                return
            before = [p for p in pings if p["ts"] <= event_time]
            after  = [p for p in pings if p["ts"] >  event_time]
            cb = [(p["lat"], p["lon"]) for p in before]
            ca = [(p["lat"], p["lon"]) for p in after]
            if len(cb) >= 2:
                folium.PolyLine(cb, color=color, weight=3, opacity=0.9,
                                tooltip=f"{name} (before)").add_to(m)
            if len(ca) >= 2:
                folium.PolyLine(ca, color=color, weight=3, opacity=0.6,
                                dash_array="8", tooltip=f"{name} (after)").add_to(m)
            for p in pings:
                folium.CircleMarker(
                    location=(p["lat"], p["lon"]), radius=3,
                    color=color, fill=True, fill_color=color, fill_opacity=0.8,
                    tooltip=f"{name} - {p['ts'].strftime('%H:%M:%S UTC')} - SOG: {p.get('sog','?')} kn"
                ).add_to(m)
            if cb:
                folium.Marker(cb[0], tooltip=f"{label} START",
                              icon=folium.Icon(color="green", icon="play", prefix="fa")).add_to(m)
            all_c = cb + ca
            if all_c:
                folium.Marker(all_c[-1], tooltip=f"{label} END",
                              icon=folium.Icon(color="gray", icon="stop", prefix="fa")).add_to(m)

        _add(pings_a, color_a, name_a, "Vessel A")
        _add(pings_b, color_b, name_b, "Vessel B")

        folium.Marker(
            location=(col_lat, col_lon),
            tooltip=f"Rank #{i} - {event_time.strftime('%Y-%m-%d %H:%M:%S UTC')}\n{name_a} x {name_b}",
            icon=folium.Icon(color="red", icon="warning-sign", prefix="glyphicon"),
        ).add_to(m)

        score = r["collision_score"] if "collision_score" in r.__fields__ else 0
        silence = r["silence_count"] if "silence_count" in r.__fields__ else 0
        dist_m = r["distance_nm"] * 1852

        legend_html = f"""
        <div style="position:fixed;bottom:30px;left:30px;z-index:1000;
                    background:white;padding:12px 16px;border-radius:6px;
                    border:1px solid #ccc;font-size:13px;line-height:1.8;">
            <b>Rank #{i} candidate</b><br>
            <span style="color:{color_a};">&#9644;</span> {name_a} (MMSI {mmsi_a})<br>
            <span style="color:{color_b};">&#9644;</span> {name_b} (MMSI {mmsi_b})<br>
            <span style="color:red;">&#9873;</span> Close approach point<br>
            <hr style="margin:4px 0;">
            Score: {score:.1f} &nbsp; Silence: {silence} &nbsp; Dist: {dist_m:.0f}m<br>
            Solid = before &nbsp; Dashed = after &nbsp; Window: +-10 min
        </div>
        """
        m.get_root().html.add_child(folium.Element(legend_html))

        out_path = os.path.join(output_dir, filename)
        m.save(out_path)
        paths.append(out_path)

    return paths


def write_report(
    mmsi_a: int,
    mmsi_b: int,
    name_a: str,
    name_b: str,
    collision_time: datetime,
    collision_lat: float,
    collision_lon: float,
    distance_nm: float,
    output_dir: str = OUTPUT_DIR,
) -> str:
    """Write a plain text collision report."""
    lines = [
        "=" * 60,
        "AIS COLLISION DETECTION REPORT",
        "=" * 60,
        "",
        "COLLISION EVENT",
        "-" * 40,
        f"Vessel A:    {name_a or 'Unknown'}",
        f"  MMSI:      {mmsi_a}",
        f"Vessel B:    {name_b or 'Unknown'}",
        f"  MMSI:      {mmsi_b}",
        "",
        f"Timestamp:   {collision_time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Latitude:    {collision_lat:.6f}",
        f"Longitude:   {collision_lon:.6f}",
        f"Distance:    {distance_nm * 1852:.1f} m ({distance_nm:.4f} nm)",
        "",
        "=" * 60,
    ]
    report = "\n".join(lines)
    print(report)

    path = os.path.join(output_dir, "collision_report.txt")
    with open(path, "w") as f:
        f.write(report)
    return path