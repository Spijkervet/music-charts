
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

from charts.model import *

with open("./spotify_users.json") as f:
    users = json.load(f)

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

for user in users:
    playlists = spotify.user_playlists(user["spotify_id"])
    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))

            playlist_details = spotify.playlist(playlist["id"])

            SpotifyUsers.insert(spotify_id=user["spotify_id"]).on_conflict("ignore").execute()
            
            # select playlist_id and snapshot_id, which means the pl wasn't updated
            try:
                playlist_id = SpotifyPlaylist().get(
                    spotify_id=playlist_details["id"],
                    # snapshot_id=playlist_details["snapshot_id"]
                )
            except:
                playlist_id = None

            if not playlist_id:
                playlist_id = SpotifyPlaylist.insert(
                    spotify_id=playlist_details["id"],
                    snapshot_id=playlist_details["snapshot_id"],
                    name=playlist_details["name"],
                    followers=playlist_details["followers"]["total"],
                    featured=False,
                    region_id=None,
                    user_spotify_id=user["spotify_id"]
                ).execute()

                ## playlist tracks
                tracks = playlist_details["tracks"]
                ds = []
                while tracks:
                    for idx, track in enumerate(tracks["items"]):
                        # if it still exists on Spotify, add it:
                        if track["track"]:
                            ds.append({
                                "track_spotify_id": track["track"]["id"],
                                "position": idx,
                                "added_at": track["added_at"],
                                "playlist_spotify_id": playlist_id
                            })
                    if tracks["next"]:
                        tracks = spotify.next(tracks)
                    else:
                        tracks = None
                
                with db.atomic():
                    StagingPlaylistTracks.insert_many(ds).execute()

        if playlists['next']:
            playlists = spotify.next(playlists)
        else:
            playlists = None