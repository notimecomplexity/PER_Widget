import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo # to display widget
    import httpx # makes HTTP request to server; supports streaming responses
    import json # decodes each event's payload string into Python dict
    import threading # runs SSE listener in background
    from collections import defaultdict, deque # data structures for buffers
    import plotly.graph_objects as go # sketches line chart 

    return defaultdict, deque, go, httpx, json, mo, threading


@app.cell
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
    return (SENSOR_NAMES,)


@app.cell
def _(defaultdict, deque, mo):
    get_buffers, set_buffers = mo.state(defaultdict(lambda: deque(maxlen=500)))
    return get_buffers, set_buffers


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This is the notebook's "memory" for incoming data.
    * mo.state(...) is marimo's version of a reactive variable — it gives you a getter function (get_buffers()) and a setter function (set_buffers(...)). Any cell that calls get_buffers() automatically re-runs whenever set_buffers(...) is called elsewhere. That's what makes the chart redraw when new data shows up.
    * The value being stored is a defaultdict(lambda: deque(maxlen=500)) — a dictionary where:
      * each key is a sensor id
      * each value is a deque (a list-like structure) holding up to 500 recent (timestamp, value) pairs for that sensor
      * defaultdict means: if you access a sensor id that doesn't exist yet, it automatically creates an empty deque for it instead of throwing a KeyError
      * maxlen=500 means once the deque hits 500 items, adding a new one automatically drops the oldest — this keeps memory bounded and gives you a "sliding window" of recent history instead of an ever-growing list
    """)
    return


@app.cell
def _(get_buffers, httpx, json, set_buffers, threading):
    def listen():
        print("Listener thread starting...")
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
    This is the piece that actually talks to your FastAPI server.
    * httpx.Client(timeout=None) — creates an HTTP client with no timeout, since the SSE connection is meant to stay open indefinitely (a normal timeout would kill it after N seconds).
    * client.stream("GET", ".../stream") — opens the connection to your server's /stream endpoint but doesn't wait for it to "finish" (it never finishes) — instead it gives you a way to read data as it arrives.
    * sseclient.SSEClient(response.iter_lines()) — takes the raw streamed text and parses it into structured SSE event objects (splitting on data: ... lines, etc.), so you don't have to parse the wire format yourself.
    * for event in client_sse.events(): — loops forever, once per message the server sends (every ~50ms per the README).
    * msg = json.loads(event.data) — turns the JSON string like {"ts": [...], "id": [...], "v": [...]} into a real Python dict.
    * for ts, sensor_id, val in zip(msg["ts"], msg["id"], msg["v"]): — since each message can contain multiple samples (possibly from different sensors) packed into three parallel lists, zip walks through them together, pulling out one (timestamp, sensor_id, value) triple at a time.
    * buffers[sensor_id].append((ts, val)) — stores that sample in the correct sensor's deque.
    * set_buffers(buffers) — pushes the updated buffer back into marimo's state, which flags any dependent cells (like the chart) as needing to re-run.
    * threading.Thread(target=listen, daemon=True).start() — runs all of the above in a background thread, so it doesn't freeze the rest of the notebook. daemon=True means this thread won't prevent the program from exiting if you close the notebook.

    **Why a background thread specifically:** the SSE loop runs forever (for event in ... never naturally ends), so if it ran directly in a normal marimo cell, that cell would just hang forever and nothing else in the notebook could run. Putting it on its own thread lets it keep listening in the background while the rest of the notebook (UI, chart redraws) stays responsive.
    """)
    return


@app.cell
def _(SENSOR_NAMES, mo):
    sensor_dropdown = mo.ui.dropdown(
        options={name: sid for sid, name in SENSOR_NAMES.items()},
        value="pcm.moc.motor.requestedTorque",
        label="Sensor to plot",
    )
    sensor_dropdown
    return (sensor_dropdown,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Creates an interactive dropdown UI element.
    * options={name: sid ...} — builds a dict where the keys are the human-readable names (what the user sees and picks from) and the values are the numeric sensor ids (what your code actually uses).
    * value="moc.motor.requestedTorque" — sets which option is selected by default.
    * The last line, sensor_dropdown by itself, tells marimo to actually display the widget in the notebook output (just creating it doesn't render it — you have to "return" it as the cell's displayed value).
    * sensor_dropdown.value (used later) gives you whatever the user currently has selected — and since it's a marimo UI element, any cell that reads sensor_dropdown.value automatically reruns when the user changes the dropdown.
    """)
    return


@app.cell
def _(mo):
    refresh = mo.ui.refresh(default_interval="0.1s")
    refresh
    return (refresh,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    This is a "clock" element — it doesn't display anything meaningful itself, but it changes its internal value on a timer (every 0.1 seconds here). Since marimo cells rerun whenever a variable they depend on changes, any cell that references refresh will get re-triggered every 100ms — this is what drives the "live" redrawing, since otherwise the chart cell would only rerun when its own inputs (like the dropdown) change, not when new background data arrives.
    """)
    return


@app.cell
def _(SENSOR_NAMES, get_buffers, go, mo, refresh, sensor_dropdown):
    refresh  # rerun this cell on each refresh tick

    sensor_id = sensor_dropdown.value
    buf = get_buffers()[sensor_id]

    fig = go.Figure()
    if buf:
        timestamps, values = zip(*buf)
        t_sec = [t / 1_000_000 for t in timestamps]
        fig.add_trace(go.Scatter(x=t_sec, y=values, mode="lines"))

    label = SENSOR_NAMES.get(sensor_id, str(sensor_id))
    fig.update_layout(title=f"Live: {label}", xaxis_title="Time (s)", yaxis_title="Value", height=400)
    mo.ui.plotly(fig)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Step by step:
    * refresh — just referencing this variable (without using it for anything) is enough to make marimo treat this cell as "depends on refresh," so it reruns every 0.1s.
    * sensor_id = sensor_dropdown.value — reads whichever sensor id the user currently has selected in the dropdown.
    * buf = get_buffers()[sensor_id] — pulls that sensor's deque of (timestamp, value) pairs out of the shared buffer.
    * fig = go.Figure() — starts a blank Plotly figure.
    * if buf: — only try to plot if there's actually data yet (avoids errors on the very first run, before any SSE messages have arrived).
    * timestamps, values = zip(*buf) — buf is a list of tuples like [(ts1, v1), (ts2, v2), ...]; zip(*buf) "unzips" it into two separate lists: all the timestamps together, all the values together.
    * t_sec = [t / 1_000_000 for t in timestamps] — converts timestamps from microseconds (as specified in the README) into seconds, so the x-axis reads in a human-friendly unit.
    * fig.add_trace(go.Scatter(x=t_sec, y=values, mode="lines")) — draws the actual line, plotting value over time.
    * fig.update_layout(...) — sets the chart title (using the readable sensor name), axis labels, and height.
    * mo.ui.plotly(fig) — the last line of a marimo cell is what gets displayed; this renders the Plotly figure as an interactive chart in the notebook.
    """)
    return


@app.cell(hide_code=True)
def _():
    # # for debugging
    # refresh
    # buffers = get_buffers()
    # mo.md(f"Sensors with data: {list(buffers.keys())} — counts: { {k: len(v) for k, v in buffers.items()} }")
    return


if __name__ == "__main__":
    app.run()
