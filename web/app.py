"""
 Flask-static-site is a skeleton Python/Flask application ready to
 be deployed as a static website. It is released under an MIT Licence.
 This file is the only Python script, and controls the entire app.
 Feel free to explore and adapt it to your own needs.
"""

import sys
from datetime import datetime, timedelta

from flask import Flask, render_template, render_template_string
from app.charts.model import *

DEBUG = True

app = Flask(__name__)
app.config.from_object(__name__)

# === URL Routes === #

@app.route('/')
def index():
    """
    SELECT * FROM `chartentry`
    LEFT JOIN track ON track.id = chartentry.track_id
    LEFT JOIN trackartists ON track.id = trackartists.track_id
    LEFT JOIN artist ON artist.id = trackartists.id
"""

    chart_id = 1
    region_id = 2
    date = (datetime.now() - timedelta(days=3)).replace(minute=0, hour=0, second=0, microsecond=0)
    tracks = (ChartEntry
        .select(ChartEntry, Track, TrackArtists, Artist)
        .join(Track)
        .join(TrackArtists)
        .join(Artist)
        .group_by(Track)
        .where(
            ChartEntry.date == date,
            ChartEntry.chart_id == chart_id,
            ChartEntry.region_id == region_id
        )
        
    )

    chart = (
        Chart
        .get(Chart.id == chart_id)
    )
    
    region = (
        Region
        .get(Region.id == region_id)
    )

    streams = (
        ChartEntry
        .select(fn.SUM(ChartEntry.streams).over(order_by=[ChartEntry.track_id]).alias('total'))
    )
    
    for s in streams:
        print(s)

    return render_template('index.html', tracks=tracks, chart=chart, region=region, date=date)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)