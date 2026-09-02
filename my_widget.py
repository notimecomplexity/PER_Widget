import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo # to display widget
    import httpx # makes HTTP request to server; supports streaming responses
    import json # decodes each event's payload string into Python dict
    import threading # runs SSE listener in background
    from collections import defaultdict, deque # data structures for buffers
    import plotly.graph_objects as go # sketches line chart
    import numpy as np # manipulates arrays
    import math # mathematical functions (e.g. floor)

    return defaultdict, deque, go, httpx, json, mo, np, threading


@app.cell(hide_code=True)
def _():
    SENSOR_NAMES = {
        0: "pcm.wheelSpeeds.frontLeft",
        1: "pcm.pedals.accel",
        2: "pcm.coolingLoop.temp",
        3: "pcm.moc.motor.temp",
        4: "pcm.moc.motor.requestedTorque",
        5: "pcm.moc.motor.torqueFeedback",
        6: "pcm.vnav.posLla.latitude",
        7: "pcm.vnav.posLla.longitude",
        8: "pcm.vnav.velocityBody.x",
        9: "pcm.vnav.velocityBody.y",
        10: "pcm.vnav.compensatedAccel.y",
        11: "pcm.vnav.yawPitchRoll.yaw",
        12: "pcm.vnav.compensatedAngularRate.z",
        13: "bms.pack.voltage",
        14: "bms.pack.current",
        15: "bms.pack.power",
        16: "bms.stack.mma.temp.avg",
        17: "pdu.sensors.currPmp1",
        18: "pdu.sensors.currPmp2",
        19: "pdu.sensors.currFan1",
        20: "pdu.sensors.currFan2",
        21: "ludwig.steeringWheel.angle",
    }

    # IDs 22-37: rearLeft tireTemps, channels 0-15
    for n in range(16):
        SENSOR_NAMES[22 + n] = f"ludwig.tireTemps.rearLeft[{n}]"

    # IDs 38-53: rearRight tireTemps, channels 0-15
    for n in range(16):
        SENSOR_NAMES[38 + n] = f"ludwig.tireTemps.rearRight[{n}]"
    return


@app.cell(hide_code=True)
def _(mo):
    # Stores: {sensor_id: {"min": (ts, val), "max": (ts, val)}}
    get_records, set_records = mo.state({})
    return get_records, set_records


@app.cell(hide_code=True)
def _(defaultdict, deque, mo):
    get_buffers, set_buffers = mo.state(defaultdict(lambda: deque(maxlen=500)))
    return get_buffers, set_buffers


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <!-- This is the notebook's "memory" for incoming data.
    * mo.state(...) is marimo's version of a reactive variable — it gives you a getter function (get_buffers()) and a setter function (set_buffers(...)). Any cell that calls get_buffers() automatically re-runs whenever set_buffers(...) is called elsewhere. That's what makes the chart redraw when new data shows up.
    * The value being stored is a defaultdict(lambda: deque(maxlen=500)) — a dictionary where:
      * each key is a sensor id
      * each value is a deque (a list-like structure) holding up to 500 recent (timestamp, value) pairs for that sensor
      * defaultdict means: if you access a sensor id that doesn't exist yet, it automatically creates an empty deque for it instead of throwing a KeyError
      * maxlen=500 means once the deque hits 500 items, adding a new one automatically drops the oldest — this keeps memory bounded and gives you a "sliding window" of recent history instead of an ever-growing list -->
    """)
    return


@app.cell(hide_code=True)
def _(
    get_buffers,
    get_records,
    httpx,
    json,
    set_buffers,
    set_records,
    threading,
):
    def listen():
        # print("Listener thread starting...")
        try:
            with httpx.Client(timeout=None) as client:
                with client.stream("GET", "http://localhost:8081/stream") as response:
                    print(f"Connected, status: {response.status_code}")
                    for line in response.iter_lines():
                        if line.startswith("data:"):
                            payload = line[len("data:"):].strip()
                            if not payload:
                                continue
                            msg = json.loads(payload)
                            buffers = get_buffers()
                            records = get_records()

                            for ts, sensor_id, val in zip(msg["ts"], msg["id"], msg["v"]):
                                buffers[sensor_id].append((ts, val))

                                if sensor_id not in records:
                                    records[sensor_id] = {"min": (ts, val), "max": (ts, val)}
                                else:
                                    if val < records[sensor_id]["min"][1]:
                                        records[sensor_id]["min"] = (ts, val)
                                    if val > records[sensor_id]["max"][1]:
                                        records[sensor_id]["max"] = (ts, val)

                            set_buffers(buffers)
                            set_records(records)
        except Exception as e:
            print(f"Listener crashed: {type(e).__name__}: {e}")

    threading.Thread(target=listen, daemon=True).start()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <!-- This is the piece that actually talks to your FastAPI server.
    * httpx.Client(timeout=None) — creates an HTTP client with no timeout, since the SSE connection is meant to stay open indefinitely (a normal timeout would kill it after N seconds).
    * client.stream("GET", ".../stream") — opens the connection to your server's /stream endpoint but doesn't wait for it to "finish" (it never finishes) — instead it gives you a way to read data as it arrives.
    * sseclient.SSEClient(response.iter_lines()) — takes the raw streamed text and parses it into structured SSE event objects (splitting on data: ... lines, etc.), so you don't have to parse the wire format yourself.
    * for event in client_sse.events(): — loops forever, once per message the server sends (every ~50ms per the README).
    * msg = json.loads(event.data) — turns the JSON string like {"ts": [...], "id": [...], "v": [...]} into a real Python dict.
    * for ts, sensor_id, val in zip(msg["ts"], msg["id"], msg["v"]): — since each message can contain multiple samples (possibly from different sensors) packed into three parallel lists, zip walks through them together, pulling out one (timestamp, sensor_id, value) triple at a time.
    * buffers[sensor_id].append((ts, val)) — stores that sample in the correct sensor's deque.
    * set_buffers(buffers) — pushes the updated buffer back into marimo's state, which flags any dependent cells (like the chart) as needing to re-run.
    * threading.Thread(target=listen, daemon=True).start() — runs all of the above in a background thread, so it doesn't freeze the rest of the notebook. daemon=True means this thread won't prevent the program from exiting if you close the notebook.

    **Why a background thread specifically:** the SSE loop runs forever (for event in ... never naturally ends), so if it ran directly in a normal marimo cell, that cell would just hang forever and nothing else in the notebook could run. Putting it on its own thread lets it keep listening in the background while the rest of the notebook (UI, chart redraws) stays responsive. -->
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    refresh = mo.ui.refresh(default_interval="0.1s")
    refresh
    return (refresh,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <!-- Creates an interactive dropdown UI element.
    * options={name: sid ...} — builds a dict where the keys are the human-readable names (what the user sees and picks from) and the values are the numeric sensor ids (what your code actually uses).
    * value="moc.motor.requestedTorque" — sets which option is selected by default.
    * The last line, sensor_dropdown by itself, tells marimo to actually display the widget in the notebook output (just creating it doesn't render it — you have to "return" it as the cell's displayed value).
    * sensor_dropdown.value (used later) gives you whatever the user currently has selected — and since it's a marimo UI element, any cell that reads sensor_dropdown.value automatically reruns when the user changes the dropdown. -->
    """)
    return


@app.cell(hide_code=True)
def _():
    # # for debugging

    # sensor_dropdown = mo.ui.dropdown(
    #     options={name: sid for sid, name in SENSOR_NAMES.items()},
    #     value="pcm.moc.motor.requestedTorque",
    #     label="Sensor to plot",
    # )
    # sensor_dropdown
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <!-- This is a "clock" element — it doesn't display anything meaningful itself, but it changes its internal value on a timer (every 0.1 seconds here). Since marimo cells rerun whenever a variable they depend on changes, any cell that references refresh will get re-triggered every 100ms — this is what drives the "live" redrawing, since otherwise the chart cell would only rerun when its own inputs (like the dropdown) change, not when new background data arrives. -->
    """)
    return


@app.cell(hide_code=True)
def _():
    # # for debugging

    # refresh  # rerun this cell on each refresh tick

    # sensor_id = sensor_dropdown.value
    # buf = get_buffers()[sensor_id]

    # fig = go.Figure()
    # if buf:
    #     timestamps, values = zip(*buf)
    #     t_sec = [t / 1_000_000 for t in timestamps]
    #     fig.add_trace(go.Scatter(x=t_sec, y=values, mode="lines"))

    # label = SENSOR_NAMES.get(sensor_id, str(sensor_id))
    # fig.update_layout(title=f"Live: {label}", xaxis_title="Time (s)", yaxis_title="Value", height=400)
    # mo.ui.plotly(fig)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    <!-- Step by step:
    * refresh — just referencing this variable (without using it for anything) is enough to make marimo treat this cell as "depends on refresh," so it reruns every 0.1s.
    * sensor_id = sensor_dropdown.value — reads whichever sensor id the user currently has selected in the dropdown.
    * buf = get_buffers()[sensor_id] — pulls that sensor's deque of (timestamp, value) pairs out of the shared buffer.
    * fig = go.Figure() — starts a blank Plotly figure.
    * if buf: — only try to plot if there's actually data yet (avoids errors on the very first run, before any SSE messages have arrived).
    * timestamps, values = zip(*buf) — buf is a list of tuples like [(ts1, v1), (ts2, v2), ...]; zip(*buf) "unzips" it into two separate lists: all the timestamps together, all the values together.
    * t_sec = [t / 1_000_000 for t in timestamps] — converts timestamps from microseconds (as specified in the README) into seconds, so the x-axis reads in a human-friendly unit.
    * fig.add_trace(go.Scatter(x=t_sec, y=values, mode="lines")) — draws the actual line, plotting value over time.
    * fig.update_layout(...) — sets the chart title (using the readable sensor name), axis labels, and height.
    * mo.ui.plotly(fig) — the last line of a marimo cell is what gets displayed; this renders the Plotly figure as an interactive chart in the notebook. -->
    """)
    return


@app.cell(hide_code=True)
def _():
    # # for debugging
    # refresh
    # buffers = get_buffers()
    # mo.md(f"Sensors with data: {list(buffers.keys())} — counts: { {k: len(v) for k, v in buffers.items()} }")
    return


@app.cell(hide_code=True)
def _():
    # # old design

    # refresh

    # buffers1 = get_buffers()

    # def latest_values1(id_offset):
    #     vals = []
    #     for n in range(16):
    #         buf = buffers1.get(id_offset + n)
    #         vals.append(buf[-1][1] if buf else 0)
    #     return vals

    # rear_left1 = latest_values1(22)
    # rear_right1 = latest_values1(38)

    # angles1 = [n * (360 / 16) for n in range(16)]  # evenly spaced around the circle

    # fig1 = go.Figure()

    # fig1.add_trace(go.Barpolar(
    #     r=[1] * 16,                      # uniform slice length — color carries the info
    #     theta=angles1,
    #     width=[360 / 16] * 16,           # slice width, so they tile into a full circle
    #     marker=dict(
    #         color=rear_left1,
    #         colorscale="Hot",
    #         cmin=min(rear_left1 + rear_right1),
    #         cmax=max(rear_left1 + rear_right1),
    #         colorbar=dict(title="°C"),
    #     ),
    #     name="Rear Left",
    # ))

    # fig1.update_layout(
    #     title="Rear Left Tire — Channel Temperatures",
    #     polar=dict(
    #         radialaxis=dict(showticklabels=False, range=[0, 1]),
    #         angularaxis=dict(
    #             rotation=90,        # shifts angle 0 to point straight up
    #             direction="clockwise",  # optional: makes channels go clockwise from top, like a clock face
    #         ),
    #     ),
    #     height=400,
    # )

    # mo.ui.plotly(fig1)


    return


@app.cell(hide_code=True)
def _():
    # # for debugging: find all-time lowest / highest temps

    # refresh
    # records = get_records()

    # tire_records = {sid: r for sid, r in records.items() if sid in tire_ids}

    # if tire_records:
    #     coldest_sid = min(tire_records, key=lambda s: tire_records[s]["min"][1])
    #     hottest_sid = max(tire_records, key=lambda s: tire_records[s]["max"][1])

    #     cold_val = tire_records[coldest_sid]["min"][1]
    #     hot_val = tire_records[hottest_sid]["max"][1]

    #     mo.md(f"""
    #     **All-time coldest:** {cold_val:.1f}°C (channel {coldest_sid})

    #     **All-time hottest:** {hot_val:.1f}°C (channel {hottest_sid})
    #     """)
    #     print(cold_val, hot_val)
    # else:
    #     mo.md("No data yet")
    return


@app.cell(hide_code=True)
def _(get_buffers, go, mo, np, refresh):
    refresh

    buffers = get_buffers()
    def latest_values(id_offset):
        vals = []
        for n in range(16):
            buf = buffers.get(id_offset + n)
            vals.append(buf[-1][1] if buf else 0)
        return vals
    
    rear_left = latest_values(22)
    rear_right = latest_values(38)
    cmin, cmax = min(rear_left + rear_right), max(rear_left + rear_right)

    def interpolate_circular(values, num_points=720):
        # Repeat the first value at the end so interpolation wraps around smoothly
        values_wrapped = values + [values[0]]
        original_angles = np.linspace(0, 360, len(values_wrapped))
        target_angles = np.linspace(0, 360, num_points, endpoint=False)
        interpolated = np.interp(target_angles, original_angles, values_wrapped)
        return target_angles, interpolated
    
    angles, smooth_vals = interpolate_circular(rear_left, num_points=720)

    # Compute which original channel each interpolated angle corresponds to
    channel_indices = [int(a / 22.5) % 16 for a in angles]

    # Bundle both temp and channel into customdata (needs to be a 2D array: one row per point)
    custom = np.stack([smooth_vals, channel_indices], axis=-1)

    fig = go.Figure()
    fig.add_trace(go.Barpolar(
        r=[1] * len(angles),
        theta=angles,
        width=[360 / len(angles)] * len(angles),
        marker=dict(
            color=smooth_vals,
            colorscale="Hot",
            cmin=0,
            cmax=115,
            colorbar=dict(title="°C"),
            line=dict(width=0),  # removes the thin borders between slices, helps the blend look continuous
        ),
        customdata=custom,  # makes the actual temp value available to the template
        hovertemplate="Channel: %{customdata[1]}<br>Temp: %{customdata[0]:.1f}°C<extra></extra>",
        showlegend=False
    ))

    # Outer overheating ring — red where over threshold, invisible elsewhere
    overheat_colors = [
        "rgb(255,129,129)" if rear_left[ch] >= 100 else "rgba(0,0,0,0)"
        for ch in channel_indices
    ]

    fig.add_trace(go.Barpolar(
        r=[0.15] * len(angles),   # ring thickness, sitting outside r=1
        base=[1] * len(angles),   # starts the bar at r=1 instead of r=0
        theta=angles,
        width=[360 / len(angles) * 1.02] * len(angles),
        marker=dict(color=overheat_colors, line=dict(width=0)),
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        title=dict(
            text="<b>Rear Left Tire</b>",
            x=0.5,                          # centers horizontally (0 = left, 1 = right)
            xanchor="center",                 # anchors the text's center point at x, not its left edge
            font=dict(family="Times New Roman", size=20),
        ),
        polar=dict(
            radialaxis=dict(showticklabels=False, range=[0, 1.15]),
            angularaxis=dict(rotation=90, direction="clockwise", showticklabels=False),
        ),
        height=400,
    )

    # --- Live overheating alert ---
    overheating = []
    tire_ids = list(range(22, 54))
    for sid in tire_ids:
        buf = buffers.get(sid)
        if buf:
            latest_ts, latest_val = buf[-1]
            if latest_val >= 100:
                side = "Rear Left" if sid < 38 else "Rear Right"
                channel = sid - 22 if sid < 38 else sid - 38
                overheating.append((side, channel, latest_val))

    if overheating:
        lines = "\n".join(
            f"- 🔴 **{side} [{channel}]**: {val:.1f}°C" for side, channel, val in overheating
        )
        alert = f"### ⚠️ Overheating Alert\n{lines}"
    else:
        alert = "✅ All tire channels within normal range"

    # # uncomment to show specific channel/s overheating
    # print(alert)

    mo.ui.plotly(fig)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Notes during Office Hours:
    - High voltage
    - battery health, pack health, cell health inside pack, **state of charge**
    - current
    - max power: 80 kW
    - pack voltage 570 V (max), 532 V for a pack (8x17 cells)
    - nominal (3.7 V) for a cell
    - Temperature: VERY IMPORTANT, as temp increases, IR decreases, positive feedback loop

    Pack temperature, shown to driver, driver knows when to stop once reach certain temperature, 60 C
    if a thermistor goes wrong, then show red
    """)
    return


if __name__ == "__main__":
    app.run()
