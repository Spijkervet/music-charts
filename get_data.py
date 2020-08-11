import requests
import csv
from datetime import date, datetime, timedelta
from model import *
from utils import get_dates
from tqdm import tqdm
from bs4 import BeautifulSoup
from response_model import Spotify, Kworb

def get_spotify_chart(url):
    data = []
    url, chart, region, chart_type, date = url
    with requests.Session() as s:
        r = s.get(url)
        if r.status_code == 200:
            r = r.content.decode("utf-8")
            reader = csv.reader(r.splitlines(), delimiter=",")
            next(reader)
            next(reader)
            for row in reader:
                if len(row) == 1:
                    return None
                elif len(row) == 4:
                    position, name, artist, spotify_id = row
                    streams = None
                else:
                    position, name, artist, streams, spotify_id = row
                spotify_id = spotify_id.split("/")[-1]
                s = Spotify(
                    chart,
                    region,
                    chart_type,
                    date,
                    position,
                    name,
                    artist,
                    streams,
                    spotify_id,
                )
                data.append(s)
    return data


def get_kworb_chart(url):
    timezone_hours = 4 # EDT - UTC = +4
    chart = "itunes100"
    data = None
    with requests.Session() as s:
        r = s.get(url)
        if r.status_code == 200:
            data = []
            region = url.split("/")[3]
            if region == "pop":
                region = "us"
            else:
                region = region[3:]

            if region == "uk":
                region = "gb"

            soup = BeautifulSoup(r.content, "html.parser")
            recent_date = str(soup.find("b")).split("|")[1].replace("Last update: ", "").replace("EDT", "").strip()
            recent_date = datetime.strptime(recent_date, "%Y-%m-%d %H:%M:%S")
            recent_date = recent_date + timedelta(hours=timezone_hours)

            table = soup.find("table")
            table_body = table.find("tbody")
            table_header = table.find("thead")
            col_names = table_header.find_all("th")
            for idx, c in enumerate(col_names):
                col_names[idx] = c.text

            rows = table_body.find_all("tr")
            for row in rows:
                cols = row.find_all("td")
                cols = [ele.text.strip() for ele in cols]
                assert len(cols) == len(col_names), "Columns does not correspond with header"

                position = int(cols[0])
                if position <= 100:
                    for idx, streams in enumerate(cols):
                        if idx > 1:
                            hours_after_recent = col_names[idx]
                            if hours_after_recent != "???":
                                if hours_after_recent == "Now":
                                    hours_after_recent = 0

                                date = recent_date - timedelta(hours=float(hours_after_recent)) # hours ago
                                artist = cols[1].split(" - ")[0]
                                name = cols[1].split(" - ")[1]
                                k = Kworb(chart, region, date, position, name, artist, streams)
                                data.append(k)
    return data

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

def build_urls():

    charts = Chart.select()
    regions = Region.select()

    composite_charts = list(
        ChartEntry.select(ChartEntry.date, ChartEntry.chart_id, ChartEntry.region_id)
        .distinct()
        .dicts()
    )
    composite_keys = set()
    for c in tqdm(composite_charts):
        composite_keys.add("{}_{}_{}".format(c["chart_id"], c["region_id"], c["date"]))

    chart_ids = get_chart_ids()
    region_ids = get_region_ids()
    urls = []
    daily_dates = get_dates(date(2020, 1, 1))
    weekly_dates = [d for d in get_dates(date(2020, 1, 1)) if d.isoweekday() == 4]
    blacklist_urls = [b.url for b in Blacklist.select(Blacklist.url)]
    dates = None
    for c in charts:
        if "_" not in c.url: # TODO
            continue
        for r in regions:
            interval = c.url.split("_")[1]
            if interval == "daily":
                dates = daily_dates
            elif interval == "weekly":
                dates = weekly_dates
            for d in dates:
                composite_key = "{}_{}_{}".format(
                    chart_ids["{}".format(c.url)],
                    region_ids[r.country_code],
                    d,
                )
                if composite_key not in composite_keys:
                    url = "https://spotifycharts.com/{}/{}/{}/{}/download".format(
                        c.url.split("_")[0], r.country_code, interval, d
                    )

                    if url not in blacklist_urls:
                        urls.append((url, c.url, r.country_code, interval, d))
    return urls
