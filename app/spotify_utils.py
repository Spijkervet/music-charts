from collections import defaultdict
from charts.model import *

def match_track_artists(tracks):
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
        

def add_track(track):
    artists = []
    for a in track["artists"]:
        artists.append({
            "name": a["name"],
            "spotify_id": a["id"]
        })

    with db.atomic():
        Artist.insert_many(artists).on_conflict("ignore").execute()

        track_id = Track.insert(
            name=track["name"],
            duration_ms=track["duration_ms"],
            explicit=track["explicit"],
            track_number=track["track_number"],
            preview_url=track["preview_url"],
            spotify_id=track["id"]
        ).on_conflict("ignore").execute()

    match_track_artists([track])
    return track_id

def add_tracks(tracks):
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
    
    match_track_artists(tracks)
