from peewee import *
import datetime
import json
import pandas as pd
from tqdm import tqdm
import concurrent.futures
from get_data import get_spotify_chart


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


db = SqliteDatabase("spotify.db")


class BaseModel(Model):
    class Meta:
        database = db


class Chart(BaseModel):
    id = AutoField()
    name = CharField()
    url = CharField()
    interval = CharField()
    genre = CharField(null=True)


class Region(BaseModel):
    id = AutoField()
    country_code = CharField(unique=True)
    name = CharField()


class Artist(BaseModel):
    id = AutoField()
    name = CharField()


class Track(BaseModel):
    id = AutoField()
    name = CharField()
    spotify_id = CharField(index=True, unique=True)
    # artist_id = ForeignKeyField(Artist, backref="artists")


class ChartEntry(BaseModel):
    id = AutoField()
    date = DateField()
    position = IntegerField()
    streams = IntegerField()
    chart_id = ForeignKeyField(Chart)
    region_id = ForeignKeyField(Region)
    track_id = ForeignKeyField(Track)


db.connect()
db.create_tables([Chart, Region, Track, Artist, ChartEntry])


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

def add_tracks(tracks):
    with db.atomic():
        track_ids = Track.insert_many(tracks).on_conflict("ignore").execute()
    return track_ids

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

if __name__ == "__main__":
    chart_ids, region_ids = populate_tables()

    from utils import get_dates
    from datetime import date

    start_date = date(2017, 1, 1)
    dates = get_dates(start_date)

    urls = []
    charts = Chart.select()
    regions = Region.select()
    for c in charts:
        for r in regions:
            for d in dates:
                url = "https://spotifycharts.com/{}/{}/{}/{}/download".format(
                    c.url, r.country_code, c.interval, d
                )
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

                if len(datas) > 200 * 200:
                    id_name = {}
                    new_ids = []
                    for d in datas:
                        id_name[d.spotify_id] = d.name
                        new_ids.append(d.spotify_id)

                    existing_ids = set([r.spotify_id for r in Track.select(Track.spotify_id)])
                    add_ids = set(new_ids) - existing_ids
                    tracks = [{"name": id_name[spotify_id], "spotify_id": spotify_id} for spotify_id in add_ids]
                    track_ids = add_tracks(tracks)
                    print("Added {} tracks".format(len(tracks)))
                    datas = []
                    # chart_id = chart_ids["{}_{}".format(d.chart, d.interval)]
                    # region_id = region_ids[d.region]
                    # chart_entry_id = add_chart_entry(
                    #     d.date, d.position, d.streams, chart_id, region_id, track_id
                    # )
            except Exception as e:
                print("Exception", e)
            reqs += 1
            print(reqs, "/", len(urls))

    exit(0)

    print("Reading historical data from csv file")
    DATA = "test.csv"  # "./datasets/spotify/historical_chart.csv"
    spotify_df = pd.read_csv(DATA, index_col=0)
    spotify_df["date"] = pd.to_datetime(spotify_df["date"]).dt.date
    spotify_df = spotify_df.dropna()

    print("Adding unique tracks")
    tracks_df = spotify_df.drop_duplicates(subset="spotify_id", keep="first")
    for idx, row in tqdm(tracks_df.iterrows(), total=tracks_df.shape[0]):
        try:
            track = Track.create(spotify_id=row["spotify_id"], name=row["track_name"],)
        except:
            pass

    print("Mapping database IDs to vendor identifier")
    track_ids = Track.select().where(
        Track.spotify_id.in_(tracks_df["spotify_id"].to_list())
    )
    id_spotify = {}
    for q in track_ids:
        id_spotify[q.spotify_id] = q.id

    # create chart mapping ID
    spotify_df["chart_mapping_id"] = (
        spotify_df["chart"].map(str) + "_" + spotify_df["chart_type"]
    )
    spotify_df["region_id"] = spotify_df["region"].map(region_ids)
    spotify_df["chart_id"] = spotify_df["chart_mapping_id"].map(chart_ids)
    spotify_df["track_id"] = spotify_df["spotify_id"].map(id_spotify)
    spotify_df = spotify_df[
        ["date", "position", "streams", "chart_id", "region_id", "track_id"]
    ]

    print("Building list queries")
    spotify_df = spotify_df.to_dict("records")

    print("Adding Chart Entries")
    N = 50000
    for b in batch(spotify_df, N):
        print("Adding {} rows".format(N))
        with db.atomic():
            ChartEntry.insert_many(b).execute()
