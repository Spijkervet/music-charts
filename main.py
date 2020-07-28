from peewee import *
import datetime
import json
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from get_data import get_spotify_chart
from model import *


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def populate_tables():
    # Populate charts
    charts = {"Top 200": "regional", "Viral 50": "viral"}
    intervals = ["daily", "weekly"]
    chart_ids = {}
    for k, chart in charts.items():
        for interval in intervals:
            try:
                idx = Chart.get(name=k, interval=interval)
            except:
                print("Added Chart", k, chart, interval)
                idx = Chart.insert(name=k, url=chart, interval=interval).execute()

            mapping_id = "{}_{}".format(chart, interval)
            chart_ids[mapping_id] = idx

    # Populate regions
    region_ids = {}
    with open("./spotify_regions.json", "r") as f:
        spotify_regions = json.load(f)
        for k, v in spotify_regions.items():
            try:
                idx = Region.get(country_code=k)
            except:
                idx = Region.insert(country_code=k, name=v).execute()
            region_ids[k] = idx
    return chart_ids, region_ids


def add_track(name, spotify_id):
    try:
        track_id = Track.get(spotify_id=spotify_id)
    except:
        track_id = Track.create(name=name, spotify_id=spotify_id)
    return track_id


def add_blacklist(url):
    try:
        bid = Blacklist.get(url=url)
    except:
        bid = Blacklist.create(url=url)
    return bid


def add_artists(data):
    data = {t.artist: t for t in data}
    existing_ids = set([r.name for r in Artist.select(Artist.name)])

    add_ids = set(data.keys()) - existing_ids
    data = [{"name": t.artist} for k, t in data.items() if t.artist in add_ids]
    print("Adding {} artists".format(len(data)))
    with db.atomic():
        ids = Artist.insert_many(data).on_conflict("ignore").execute()
    return ids


def add_tracks(data):
    tracks = {t.spotify_id: t for t in data}
    existing_ids = set([r.spotify_id for r in Track.select(Track.spotify_id)])

    add_ids = set(tracks.keys()) - existing_ids
    tracks = [
        {
            "name": t.name,
            "artist_id": Artist.get(name=t.artist),
            "spotify_id": t.spotify_id,
        }
        for k, t in tracks.items()
        if t.spotify_id in add_ids
    ]
    print("Adding {} tracks".format(len(tracks)))
    with db.atomic():
        Track.insert_many(tracks).on_conflict("ignore").execute()


def add_chart_entry(date, position, streams, chart_id, region_id, track_id):
    # try:
    #     chart_entry_id = ChartEntry.get(date=date, chart_id=chart_id, region_id=region_id, track_id=track_id)
    # except:

    chart_entry_id = ChartEntry.create(
        date=date,
        position=position,
        streams=streams,
        chart_id=chart_id,
        region_id=region_id,
        track_id=track_id,
    )
    return chart_entry_id


def add_chart_entries(data):
    existing_tracks = {
        r.spotify_id: r.id for r in Track.select(Track.id, Track.spotify_id)
    }
    data = [
        {
            "date": d.date,
            "position": d.position,
            "streams": d.streams,
            "chart_id": chart_ids["{}_{}".format(d.chart, d.interval)],
            "region_id": region_ids[d.region],
            "track_id": existing_tracks[d.spotify_id],
        }
        for d in data
        if d.spotify_id is not None
    ]
    print("Adding {} chart entries".format(len(data)))
    with db.atomic():
        ChartEntry.insert_many(data).on_conflict("ignore").execute()


if __name__ == "__main__":
    chart_ids, region_ids = populate_tables()

    from utils import get_dates
    from datetime import date

    start_date = date(2017, 1, 1)
    daily_dates = get_dates(start_date)
    charts = list(
        ChartEntry.select(ChartEntry.date, ChartEntry.chart_id, ChartEntry.region_id)
        .distinct()
        .dicts()
    )
    composite_keys = set()
    for c in tqdm(charts):
        composite_keys.add("{}_{}_{}".format(c["chart_id"], c["region_id"], c["date"]))

    blacklist_urls = [b.url for b in Blacklist.select(Blacklist.url)]
    
    urls = []
    charts = Chart.select()
    regions = Region.select()
    for c in charts:
        for r in regions:
            for d in daily_dates:
                if c.interval == "daily":
                    composite_key = "{}_{}_{}".format(
                        chart_ids["{}_{}".format(c.url, c.interval)],
                        region_ids[r.country_code],
                        d,
                    )
                    if composite_key not in composite_keys:
                        url = "https://spotifycharts.com/{}/{}/{}/{}/download".format(
                            c.url, r.country_code, c.interval, d
                        )

                        if url not in blacklist_urls:
                            urls.append((url, c.url, r.country_code, c.interval, d))

    reqs = 0
    datas = []
    print("Scraping {} URLs".format(len(urls)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {
            executor.submit(get_spotify_chart, url, 60): url for url in urls
        }
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    datas.extend(data)
                else:
                    add_blacklist(url[0])

                if len(datas) > (200 * 200):
                    id_name = {}
                    new_ids = []
                    add_artists(datas)
                    add_tracks(datas)
                    add_chart_entries(datas)
                    datas = []
            except Exception as e:
                print("Exception", e)
            reqs += 1
            print(reqs, "/", len(urls))
