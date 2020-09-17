
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from collections import defaultdict
from charts.model import db
from spotify_utils import add_tracks

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
        add_tracks(tracks)
        bulk = []
