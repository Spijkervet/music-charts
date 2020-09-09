
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from charts.model import *

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


entries = HistoricalEntry().select(HistoricalEntry.spotify_id).distinct()

bulk = []
results = []
for e in entries:
    bulk.append(e.spotify_id)
    if len(bulk) == 100:
        processed = False
        while processed is False:
            features = spotify.audio_features(bulk)
            fts = []
            for f in features:
                if f is None:
                    continue
                
                d = {}
                d["spotify_id"] = f["id"]
                d["danceability"] = f["danceability"]
                d["energy"] = f["energy"]
                d["key"] = f["key"]
                d["loudness"] = f["loudness"]
                d["mode"] = f["mode"]
                d["speechiness"] = f["speechiness"]
                d["acousticness"] = f["acousticness"]
                d["instrumentalness"] = f["instrumentalness"]
                d["liveness"] = f["liveness"]
                d["valence"] = f["valence"]
                d["tempo"] = f["tempo"]
                d["duration_ms"] = f["duration_ms"]
                d["time_signature"] = f["time_signature"]

                fts.append(d)

            with db.atomic():
                AudioFeatures.insert_many(fts).execute()
                processed = True
        bulk = []
