
from __future__ import annotations

import os
import io
from pathlib import Path

from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import pandas as pd

from analytics import (
    load_events_csv,
    compute_funnel_summary,
    compute_daily_series,
    compute_lift_table,
    detect_anomalies,
)

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
CURRENT_DATA_PATH = DATA_DIR / "current.csv"
SAMPLE_DATA_PATH = DATA_DIR / "sample_events.csv"

UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")

def get_active_df() -> pd.DataFrame:
    if CURRENT_DATA_PATH.exists():
        return load_events_csv(CURRENT_DATA_PATH)
    return load_events_csv(SAMPLE_DATA_PATH)

def apply_filters(df: pd.DataFrame):
    variant = request.args.get("variant", "all")
    channel = request.args.get("channel", "all")
    segment = request.args.get("segment", "all")
    start_date = request.args.get("start_date", "")
    end_date = request.args.get("end_date", "")

    fdf = df.copy()
    if variant != "all":
        fdf = fdf[fdf["variant"] == variant]
    if channel != "all":
        fdf = fdf[fdf["channel"] == channel]
    if segment != "all":
        fdf = fdf[fdf["segment"] == segment]
    if start_date:
        fdf = fdf[fdf["date"] >= start_date]
    if end_date:
        fdf = fdf[fdf["date"] <= end_date]

    selected = dict(variant=variant, channel=channel, segment=segment, start_date=start_date, end_date=end_date)
    return fdf, selected

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/load-sample")
def load_sample():
    if CURRENT_DATA_PATH.exists():
        CURRENT_DATA_PATH.unlink()
    flash("Loaded sample dataset.", "success")
    return redirect(url_for("dashboard"))

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        flash("No file uploaded.", "error")
        return redirect(url_for("index"))

    f = request.files["file"]
    if f.filename.strip() == "":
        flash("Please choose a CSV file.", "error")
        return redirect(url_for("index"))

    if not f.filename.lower().endswith(".csv"):
        flash("Only .csv files are supported.", "error")
        return redirect(url_for("index"))

    save_path = UPLOADS_DIR / f.filename
    f.save(str(save_path))

    try:
        df = load_events_csv(save_path)
    except Exception as e:
        flash(f"Could not read CSV: {e}", "error")
        return redirect(url_for("index"))

    df.to_csv(CURRENT_DATA_PATH, index=False)
    flash("Uploaded dataset loaded successfully.", "success")
    return redirect(url_for("dashboard"))

@app.route("/dashboard")
def dashboard():
    df = get_active_df()
    fdf, selected = apply_filters(df)

    funnel = compute_funnel_summary(fdf)
    lift = compute_lift_table(fdf)
    daily = compute_daily_series(fdf)
    anomalies = detect_anomalies(daily, metric="approve_rate_over_impression")

    variants = ["all"] + sorted(df["variant"].unique().tolist())
    channels = ["all"] + sorted(df["channel"].unique().tolist())
    segments = ["all"] + sorted(df["segment"].unique().tolist())

    return render_template(
        "dashboard.html",
        funnel=funnel,
        lift=lift.to_dict(orient="records"),
        anomalies=anomalies.to_dict(orient="records"),
        daily=daily.to_dict(orient="list"),
        variants=variants,
        channels=channels,
        segments=segments,
        selected=selected,
    )

@app.route("/export/funnel.csv")
def export_funnel():
    df = get_active_df()
    fdf, _ = apply_filters(df)
    funnel = compute_funnel_summary(fdf)

    out = pd.DataFrame([
        {"step": "impression", "users": funnel["counts"]["impression"]},
        {"step": "click", "users": funnel["counts"]["click"]},
        {"step": "apply", "users": funnel["counts"]["apply"]},
        {"step": "approve", "users": funnel["counts"]["approve"]},
    ])

    buf = io.StringIO()
    out.to_csv(buf, index=False)
    buf.seek(0)

    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="funnel_summary.csv",
    )

@app.route("/export/lift.csv")
def export_lift():
    df = get_active_df()
    fdf, _ = apply_filters(df)
    lift = compute_lift_table(fdf)

    buf = io.StringIO()
    lift.to_csv(buf, index=False)
    buf.seek(0)

    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="lift_summary.csv",
    )

if __name__ == "__main__":
    app.run(debug=True)
