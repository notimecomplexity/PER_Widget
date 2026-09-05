# 2D Heatmap of Rear Left Tire Rotor

A marimo-based widget built for the Penn Electric Racing (P.E.R.) Software Challenge.

## Setup

1. Create a virtual environment:
```bash
   python -m venv .venv
```

2. Activate it:
   - Mac/Linux: `source .venv/bin/activate`
   - Windows: `.venv\Scripts\activate`

3. Install dependencies:
```bash
   pip install -r requirements.txt
```

4. Run the widget:
```bash
   marimo run my_widget.py
```
   (or `marimo edit my_widget.py` if you want to view/edit it in the notebook interface)

## Files
- `my_widget.py` — the widget code
- `requirements.txt` — Python dependencies

## Notes
The widget is a **2D Heatmap** visualisation of **REV11's Rear Left Tire Rotor**. Although **REV11** also had sensors on the **Rear Right Tire**, the `server` did not return any corresponding sensor data, and there are no corresponding sensors on the **Front Tires**. Luckily, this code is easily adjustable once sensor data for the other three tires are available. 
