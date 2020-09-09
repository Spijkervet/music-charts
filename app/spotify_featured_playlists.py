
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from charts.model import *

with open("./spotify_users.json") as f:
    users = json.load(f)

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


for region in Region(Region.id).select():
    try:
        playlists = spotify.featured_playlists(country=region.country_code)
    except Exception as e:
        print(e)
        continue
    
    playlists = playlists["playlists"]
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))

            playlist_details = spotify.playlist(playlist["id"])

            user = playlist["owner"]
            SpotifyUsers.insert(spotify_id=user["id"]).on_conflict("ignore").execute()
            
            # select playlist_id and snapshot_id, which means the pl wasn't updated
            try:
                playlist_id = SpotifyPlaylist().get(
                    spotify_id=playlist_details["id"],
                    snapshot_id=playlist_details["snapshot_id"]
                )
            except:
                playlist_id = None

            if not playlist_id:
                playlist_id = SpotifyPlaylist.insert(
                    spotify_id=playlist_details["id"],
                    snapshot_id=playlist_details["snapshot_id"],
                    name=playlist_details["name"],
                    followers=playlist_details["followers"]["total"],
                    featured=True,
                    region_id=region.id,
                    user_spotify_id=user["id"]
                ).execute()

                ## playlist tracks
                tracks = playlist_details["tracks"]
                ds = []
                while tracks:
                    for idx, track in enumerate(tracks["items"]):
                        # if it still exists on Spotify, add it:
                        if track["track"]:
                            ds.append({
                                "track_id": track["track"]["id"],
                                "position": idx,
                                "added_at": track["added_at"],
                                "playlist_spotify_id": playlist_id
                            })
                    if tracks["next"]:
                        tracks = spotify.next(tracks)
                    else:
                        tracks = None
                
                with db.atomic():
                    PlaylistTracks.insert_many(ds).execute()

        if playlists['next']:
            playlists = spotify.next(playlists)
        else:
            playlists = None