import concurrent.futures
import json
from datetime import date
import time
from peewee import *
from tqdm import tqdm
from get_data import (
    get_spotify_chart,
    get_kworb_chart,
    build_urls,
    get_chart_ids,
    get_region_ids,
)
from model import *
from utils import get_dates


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
    t0 = time.time()
    artists = []
    added = []
    for d in data:
        if d.artist not in added:
            added.append(d.artist)
            artists.append({"name": d.artist})

    with db.atomic():
        Artist.insert_many(artists).on_conflict("ignore").execute()
    print("Artists", time.time() - t0)


def add_tracks(data):
    t0 = time.time()
    tracks = []
    added = []
    artist_name_id = {a.name: a.id for a in Artist.select(Artist.name, Artist.id)}
    for d in data:
        key = d.artist + d.name
        if key not in added:
            added.append(key)
            tracks.append({"name": d.name, "artist_id": artist_name_id[d.artist], "spotify_id": d.spotify_id})

    with db.atomic():
        Track.insert_many(tracks).on_conflict("ignore").execute()
    print("Tracks", time.time() - t0)


def add_chart_entries(data):
    t0 = time.time()
    chart_ids = get_chart_ids()
    region_ids = get_region_ids()

    data = [
        {
            "date": d.date,
            "position": d.position,
            "streams": d.streams,
            "chart_id": chart_ids[d.chart],
            "region_id": region_ids[d.region],
            "track_id": "{}_{}".format(d.artist, d.name),
        }
        for d in data
    ]
    with db.atomic():
        ChartEntry.insert_many(data).execute()
    print("Charts", time.time() - t0)


if __name__ == "__main__":
    populate_tables()
    dates = get_dates(date(2013, 8, 13))
    urls = []
    for r, _ in get_itunes_regions().items():
        for d in dates:
            if r == "us":
                r = ""
            urls.append(
                "https://kworb.net/pop{}/archive/{}.html".format(
                    r, str(d).replace("-", "")
                )
            )

    f = get_kworb_chart
    reqs = 0
    datas = []

    ## Spotify
    # urls = build_urls()
    # print("Scraping {} URLs".format(len(urls)))
    # f = get_spotify_chart
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_url = {executor.submit(f, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            # try:
            url = future_to_url[future]
            data = future.result()
            if data:
                datas.extend(data)
            else:
                add_blacklist(url[0])

            if len(datas) > (200 * 200):
                add_artists(datas)
                add_tracks(datas)
                add_chart_entries(datas)
                datas = []
            # except Exception as e:
            #     print(e)
            #     pass
            reqs += 1
            print(reqs, "/", len(urls))

        # add remainder
        add_artists(datas)
        add_tracks(datas)
        add_chart_entries(datas)
