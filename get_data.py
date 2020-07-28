import requests
import csv
from spotify import Spotify


def get_spotify_chart(url, timeout):
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
