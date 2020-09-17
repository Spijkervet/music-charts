
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from collections import defaultdict
from charts.model import db, StagingPlaylistTracks
from spotify_utils import add_tracks

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


query = """
    SELECT DISTINCT `track_spotify_id`
    FROM `stagingplaylisttracks`
    WHERE NOT EXISTS 
    (
    SELECT 1 
        FROM `track`
        WHERE `track`.`spotify_id`=`stagingplaylisttracks`.`track_spotify_id`
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


# staging playlisttracks

query = """
    INSERT INTO `playlisttracks` (`track_id`, `position`, `added_at`, `playlist_spotify_id`)
    SELECT `track`.id, `stagingplaylisttracks`.`position`, `stagingplaylisttracks`.added_at, `stagingplaylisttracks`.`playlist_spotify_id`  FROM `stagingplaylisttracks`
    LEFT JOIN `track` ON `track`.`spotify_id`=`stagingplaylisttracks`.`track_spotify_id`
    WHERE `track`.`id` IS NOT NULL
"""
cursor = db.execute_sql(query)

StagingPlaylistTracks.delete().execute()