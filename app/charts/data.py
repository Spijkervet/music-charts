import csv
import json
from datetime import date, datetime, timedelta
from charts.model import Chart, Region, Vendor

def get_add_vendor(name):
    try:
        vendor_id = Vendor.get(name=name)
    except:
        print("Added Vendor", name)
        vendor_id = Vendor.insert(name=name).execute()
    return vendor_id

def populate_tables():
    # Populate charts
    with open("./charts.json", "r") as f:
        charts = json.load(f)
    
    for chart in charts:
        try:
            idx = Chart.get(name=chart["name"])
        except:
            print("Added Chart", chart["name"])
            vendor_id = get_add_vendor(chart["vendor"])
            idx = Chart.insert(name=chart["name"], identifier=chart["identifier"], interval=chart["interval"], vendor_id=vendor_id).execute()

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
        chart_ids["{}_{}".format(c.identifier, c.interval)] = c.id
    return chart_ids

def get_region_ids():
    region_ids = {}
    for r in Region.select():
        region_ids[r.country_code] = r.id
    return region_ids


