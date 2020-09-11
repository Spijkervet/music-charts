
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from collections import defaultdict
from charts.model import *

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


query = """
    SELECT DISTINCT `spotify_id`
    FROM `historicalentry`
    WHERE NOT EXISTS 
    (
    SELECT 1 
        FROM `track`
        WHERE `track`.`spotify_id`=`historicalentry`.`spotify_id`
    );
"""


cursor = db.execute_sql(query)
entries = cursor.fetchall()
print(len(entries))

MAX = 50
bulk = []
results = []
for e in entries:
    
    if len(e[0]) <= 1:
        continue

    bulk.append(e[0])
    if len(bulk) == MAX:
        
        tracks = spotify.tracks(bulk)
        tracks = tracks["tracks"]

        """
        'album', 'artists', 'available_markets', 'disc_number', 'duration_ms', 'explicit', 'external_ids', 'external_urls', 'href', 'id', 'is_local', 'name', 'popularity', 'preview_url', 'track_number', 'type', 'uri']
        """
        ts = []
        artists = []
        track_artist = defaultdict(list)
        for t in tracks:
            for a in t["artists"]:
                artists.append({
                    "name": a["name"],
                    "spotify_id": a["id"]
                })

            ts.append({
                "name": t["name"],
                "duration_ms": t["duration_ms"],
                "explicit": t["explicit"],
                "track_number": t["track_number"],
                "preview_url": t["preview_url"],
                "spotify_id": t["id"]
            })
        
        with db.atomic():
            Artist.insert_many(artists).on_conflict("ignore").execute()
    
        with db.atomic():
            Track.insert_many(ts).on_conflict("ignore").execute()
        
        # match track with artists
        track_artists = []
        for t in tracks:
            for a in t["artists"]:
                try:
                    track_id = Track.get(Track.spotify_id == t["id"])
                    artist_id = Artist.get(Artist.spotify_id == a["id"])

                    track_artists.append({
                        "track_id": track_id,
                        "artist_id": artist_id
                    })
                except Exception as e:
                    pass

        with db.atomic():
            TrackArtists.insert_many(track_artists).execute()
            
        bulk = []
