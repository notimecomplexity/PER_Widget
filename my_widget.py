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
    import time # for flashing red
    import statistics # for calculating median

    return defaultdict, deque, go, httpx, json, mo, np, statistics, threading


@app.cell(hide_code=True)
def _():
    # dictionary of Sensor Names (pulled from GitHub README.md)

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

    # IDs 22-37: Rear Left Tire Motor Temperature, channels 0 - 15
    for n in range(16):
        SENSOR_NAMES[22 + n] = f"ludwig.tireTemps.rearLeft[{n}]"

    # NOTE: There is NO live data stream from the server for the following Sensor IDs.
    # IDs 38-53: Rear Right Tire Motor Temperature, channels 0 - 15
    for n in range(16):
        SENSOR_NAMES[38 + n] = f"ludwig.tireTemps.rearRight[{n}]"
    return


@app.cell(hide_code=True)
def _(defaultdict, deque, mo):
    # creating getter / setter functions to retrieve / update data

    get_buffers, set_buffers = mo.state(defaultdict(lambda: deque(maxlen=500)))
    return get_buffers, set_buffers


@app.cell(hide_code=True)
def _(get_buffers, httpx, json, set_buffers, threading):
    # listener function processes each new piece of data from the server

    def listen():
        try:
            with httpx.Client(timeout=None) as client:
                with client.stream("GET", "http://localhost:8081/stream") as response:
                    print(f"Status: {response.status_code} {"OK\nStandard response for successful HTTP requests." if response.status_code == 200 else ""}")
                    for line in response.iter_lines():
                        if line.startswith("data:"):
                            payload = line[len("data:"):].strip()
                            if not payload:
                                continue
                            msg = json.loads(payload)
                            buffers = get_buffers()

                            for ts, sensor_id, val in zip(msg["ts"], msg["id"], msg["v"]):
                                buffers[sensor_id].append((ts, val))

                            set_buffers(buffers)
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
    # refreshes every second

    refresh = mo.ui.refresh(default_interval="0.1s")

    mo.Html(f"<div style='display: none;'>{refresh}</div>") # hides refresh widget
    return (refresh,)


@app.cell(hide_code=True)
def _(mo):
    # Penn Electric Racing logo

    mo.image(src="https://kevinjchen.me/assets/images/per/logo.png", height=190).center()
    return


@app.cell(hide_code=True)
def _(get_buffers, go, mo, np, refresh, statistics):
    # 2D Heatmap of Rear Left Tire Rotor

    refresh

    bufs = get_buffers()

    def latest_vals(id_offset):
        vals = []
        oh = False
        for n in range(16):
            buf = bufs.get(id_offset + n)
            vals.append(buf[-1][1] if buf else 0) # average temperature
        if vals and max(vals) >= 80: # overheating check
            oh = True
        return vals, oh, statistics.mean(vals), statistics.median(vals), max(vals), min(vals)

    rear_left, overheating, rl_mean, rl_median, rl_maximum, rl_minimum = latest_vals(22)

    def interpolate_linear(values, num_points=320):
        original_x = np.linspace(0, 15, len(values))   # original channel positions: 0-15
        target_x = np.linspace(0, 15, num_points)        # many more points across the same range
        interpolated = np.interp(target_x, original_x, values)
        return target_x, interpolated

    x_smooth, smooth_vals = interpolate_linear(rear_left, num_points=320)

    rltm = go.Figure()

    rltm.add_trace(go.Heatmap(
        z=[smooth_vals],
        x=x_smooth,                     # 160 finely-spaced x positions
        y=[""],
        colorscale="Inferno",
        zmin=20,
        zmax=115,
        colorbar=dict(
            title=dict(text="°C", side="bottom", font=dict(family="Satoshi")),
            orientation="h",
            y=1,        # negative moves it below the plot area
            len=0.6,        # width of the colorbar, as a fraction of the plot width
            thickness=15,   # bar thickness in pixels
            dtick=10,
            tickfont=dict(family="Satoshi"),
        ),
        showscale=True,
        hovertemplate="Temp: %{z:.1f}°C<extra></extra>",
    ))

    rltm.update_layout(
        font=dict(family="Satoshi"),
        title=dict(
            text="<b>Rear Left Tire Rotor</b>",
            x=0.5,
        ),
        xaxis=dict(
            title=dict(
                text="Channel",   # ← change the text here
                standoff=50,               # ← distance between axis line and title (repositioning)
            ),
            tickmode="array",
            tickvals=list(range(16)),    # only show ticks at the original 16 channel positions
            ticktext=[str(n) for n in range(16)],
        ),
        height=250,
    )

    mo.vstack([
        mo.ui.plotly(rltm),
        mo.center(mo.md("""
        <div style="font-family: 'Satoshi', sans-serif; font-style: italic;">
        <span style="color: rgb(255, 65, 1); font-weight: bold;">Overheating</span> occurs when any one sensor reports a temperature reading of 80 °C or above.
        </div>
        """))
    ])
    return overheating, rear_left, rl_maximum, rl_mean, rl_median, rl_minimum


@app.cell(hide_code=True)
def _(mo, overheating, refresh, rl_maximum, rl_mean, rl_median, rl_minimum):
    # 2D Heatmap Statistics

    refresh

    def make_card(title, value, unit="°C"):
        if title == "Maximum" and overheating:
            color = "rgb(255, 65, 1)"
        else:
            color = "rgb(255, 255, 255)"
        return mo.Html(f"""
        <link href="https://api.fontshare.com/v2/css?f[]=satoshi@400,700&display=swap" rel="stylesheet">
        <div style="
            border: 1px solid #ddd;
            border-radius: 50%;
            overflow: hidden;
            font-family: 'Satoshi';
            width: 180px;
            height: 180px;
            text-align: center;
            background-image: linear-gradient(rgba(0,0,0,0.2), rgba(0,0,0,0.2)), url('https://pics.clipartpng.com/Tire_PNG_Clip_Art-2927.png');
            background-size: cover;
            background-position: center;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            color: white;
            text-shadow: 0 1px 3px rgba(0,0,0,0.8);
            -webkit-text-stroke: 0.5px black;
            color: {color};
        ">
            <div style="font-size: 20px; font-weight: bold; margin-bottom: 6px;">
                {title}
            </div>
            <div style="font-size: 20px; font-weight: bold;">
                {value}{unit}
            </div>
        </div>
        """)

    mo.hstack([
        make_card("Mean", round(rl_mean, 1)),
        make_card("Median", round(rl_median, 1)),
        make_card("Maximum", round(rl_maximum, 1)),
        make_card("Minimum", round(rl_minimum, 1)),
    ], gap=2.5, justify="center")
    return


@app.cell
def _(mo, overheating, rear_left, refresh):
    # Overheating Warning

    refresh

    oh_num = len([rear_left.index(hot) for hot in rear_left if hot >= 80])

    if overheating:
        alert = mo.Html(f"""
        <style>
        @keyframes flash {{
            0%, 100% {{ background-color: #990000; }}
            50% {{ background-color: #ffcccc; }}
        }}
        .flash-box {{
            animation: flash 1s infinite;
            color: white;
            font-weight: bold;
            font-size: 18px;
            text-align: center;
            padding: 16px;
            border-radius: 8px;
            width: 925px;
            margin: 0 auto;
            font-family: 'Satoshi';
        }}
        </style>
        <div class="flash-box">
            ⚠️ WARNING: {oh_num} CHANNEL{"S" if oh_num > 1 else ""} OVERHEATING ⚠️<br>PLEASE REDUCE LOAD
        </div>
        """)
    else:
        alert = mo.md("")

    alert
    return


if __name__ == "__main__":
    app.run()
