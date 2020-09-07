import csv
import json
from datetime import date, datetime, timedelta
from charts.model import Chart, Region

def populate_tables():
    # Populate charts
    charts = {
        "Spotify Top 200 Daily": "regional_daily",
        "Spotify Viral 50 Daily": "viral_daily",
        "Spotify Top 200 Weekly": "regional_weekly",
        "Spotify Viral 50 Weekly": "viral_weekly",
        "iTunes Top 100": "itunes100",
    }

    for k, chart in charts.items():
        try:
            idx = Chart.get(name=k)
        except:
            print("Added Chart", k, chart)
            idx = Chart.insert(name=k, url=chart).execute()

    # Populate regions
    with open("./spotify_regions.json", "r") as f:
        regions = json.load(f)
        for k, v in regions.items():
            try:
                idx = Region.get(country_code=k)
            except:
                idx = Region.insert(country_code=k, name=v).execute()

    regions = get_itunes_regions()
    for k, v in regions.items():
        try:
            idx = Region.get(country_code=k)
        except:
            idx = Region.insert(country_code=k, name=v).execute()


def get_itunes_regions():
    with open("./itunes_regions.json", "r") as f:
        regions = json.load(f)
    return regions


def get_chart_ids():
    chart_ids = {}
    for c in Chart.select():
        chart_ids[c.url] = c.id
    return chart_ids

def get_region_ids():
    region_ids = {}
    for r in Region.select():
        region_ids[r.country_code] = r.id
    return region_ids


