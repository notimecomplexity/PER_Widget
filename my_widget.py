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

    return defaultdict, deque, go, httpx, json, mo, np, threading, time


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
    refresh
    return (refresh,)


@app.cell(hide_code=True)
def _(get_buffers, go, mo, np, refresh, time):
    refresh

    bufs = get_buffers()

    def latest_vals(id_offset):
        vals = []
        oh = False
        for n in range(8):
            buf_l = bufs.get(id_offset + n)
            buf_u = bufs.get(id_offset + 15 - n)
            vals.append((buf_l[-1][1] + buf_u[-1][1]) / 2 if buf_l and buf_u else 0) # average temperature
            if buf_l[-1][1] >= 80 or buf_u[-1][1] >= 80: # overheating check
                oh = True
        return vals, oh

    rear_left1, overheating = latest_vals(22)

    radial_values = rear_left1[::-1]

    def interpolate_radial(values, num_rings=40):
        original_radii = np.linspace(0, 1, len(values))
        target_radii = np.linspace(0, 1, num_rings)
        interpolated = np.interp(target_radii, original_radii, values)
        return target_radii, interpolated

    radii, smooth_radial_vals = interpolate_radial(radial_values, num_rings=40)

    rltm = go.Figure()

    ring_thickness = 1 / len(radii) * 1.02  # tiny overlap avoids gaps between rings

    for i in range(len(radii)):
        rltm.add_trace(go.Barpolar(
            r=[ring_thickness],
            base=[radii[i]],
            theta=[180],
            width=[360],
            marker=dict(
                color=[smooth_radial_vals[i]],

                colorscale="Hot",
                cmin=20,
                cmax=115,
                line=dict(width=0),
                colorbar=dict(title="°C") if i == 0 else None,  # prevents lag
            ),
            showlegend=False,
            hovertemplate=f"Temp: {smooth_radial_vals[i]:.1f}°C<extra></extra>"
        ))

    if overheating:
        flash_on = int(time.time() * 2) % 2 == 0  # toggles every 0.5s
        ring_color = "rgb(255,129,129)" if flash_on else "rgba(0,0,0,0)"

        rltm.add_trace(go.Barpolar(
            r=[0.1],
            base=[1.025],
            theta=[180],
            width=[360],
            marker=dict(color=ring_color, line=dict(width=0)),
            showlegend=False,
            hoverinfo="skip",
        ))

    rltm.update_layout(
        title=dict(text="<b>Rear Left Tire Motor</b>", x=0.5, xanchor="center"),
        polar=dict(
            radialaxis=dict(showticklabels=False, range=[0, 1.125]),
            angularaxis=dict(showticklabels=False),
        ),
        height=400,
        coloraxis=dict(colorscale="Hot", cmin=20, cmax=115, colorbar=dict(title="°C")),
        hovermode="closest",
    )

    mo.ui.plotly(rltm)
    return


@app.cell(hide_code=True)
def _():
    # refresh

    # buffers = get_buffers()
    # def latest_values(id_offset):
    #     vals = []
    #     for n in range(16):
    #         buf = buffers.get(id_offset + n)
    #         vals.append(buf[-1][1] if buf else 0)
    #     return vals

    # rear_left = latest_values(22)
    # rear_right = latest_values(38)
    # cmin, cmax = min(rear_left + rear_right), max(rear_left + rear_right)

    # def interpolate_circular(values, num_points=720):
    #     # Repeat the first value at the end so interpolation wraps around smoothly
    #     values_wrapped = values + [values[0]]
    #     original_angles = np.linspace(0, 360, len(values_wrapped))
    #     target_angles = np.linspace(0, 360, num_points, endpoint=False)
    #     interpolated = np.interp(target_angles, original_angles, values_wrapped)
    #     return target_angles, interpolated

    # angles, smooth_vals = interpolate_circular(rear_left, num_points=720)

    # # Compute which original channel each interpolated angle corresponds to
    # channel_indices = [int(a / 22.5) % 16 for a in angles]

    # # Bundle both temp and channel into customdata (needs to be a 2D array: one row per point)
    # custom = np.stack([smooth_vals, channel_indices], axis=-1)

    # fig = go.Figure()
    # fig.add_trace(go.Barpolar(
    #     r=[1] * len(angles),
    #     theta=angles,
    #     width=[360 / len(angles)] * len(angles),
    #     marker=dict(
    #         color=smooth_vals,
    #         colorscale="Hot",
    #         cmin=0,
    #         cmax=115,
    #         colorbar=dict(title="°C"),
    #         line=dict(width=0),  # removes the thin borders between slices, helps the blend look continuous
    #     ),
    #     customdata=custom,  # makes the actual temp value available to the template
    #     hovertemplate="Channel: %{customdata[1]}<br>Temp: %{customdata[0]:.1f}°C<extra></extra>",
    #     showlegend=False
    # ))

    # # Outer overheating ring — red where over threshold, invisible elsewhere
    # overheat_colors = [
    #     "rgb(255,129,129)" if rear_left[ch] >= 100 else "rgba(0,0,0,0)"
    #     for ch in channel_indices
    # ]

    # fig.add_trace(go.Barpolar(
    #     r=[0.15] * len(angles),   # ring thickness, sitting outside r=1
    #     base=[1] * len(angles),   # starts the bar at r=1 instead of r=0
    #     theta=angles,
    #     width=[360 / len(angles) * 1.02] * len(angles),
    #     marker=dict(color=overheat_colors, line=dict(width=0)),
    #     showlegend=False,
    #     hoverinfo="skip",
    # ))

    # fig.update_layout(
    #     title=dict(
    #         text="<b>Rear Left Tire</b>",
    #         x=0.5,                          # centers horizontally (0 = left, 1 = right)
    #         xanchor="center",                 # anchors the text's center point at x, not its left edge
    #         font=dict(family="Times New Roman", size=20),
    #     ),
    #     polar=dict(
    #         radialaxis=dict(showticklabels=False, range=[0, 1.15]),
    #         angularaxis=dict(rotation=90, direction="clockwise", showticklabels=False),
    #     ),
    #     height=400,
    # )

    # # --- Live overheating alert ---
    # overheating = []
    # tire_ids = list(range(22, 54))
    # for sid in tire_ids:
    #     buf = buffers.get(sid)
    #     if buf:
    #         latest_ts, latest_val = buf[-1]
    #         if latest_val >= 100:
    #             side = "Rear Left" if sid < 38 else "Rear Right"
    #             channel = sid - 22 if sid < 38 else sid - 38
    #             overheating.append((side, channel, latest_val))

    # if overheating:
    #     lines = "\n".join(
    #         f"- 🔴 **{side} [{channel}]**: {val:.1f}°C" for side, channel, val in overheating
    #     )
    #     alert = f"### ⚠️ Overheating Alert\n{lines}"
    # else:
    #     alert = "✅ All tire channels within normal range"

    # # # uncomment to show specific channel/s overheating
    # # print(alert)

    # mo.ui.plotly(fig)
    return


if __name__ == "__main__":
    app.run()
