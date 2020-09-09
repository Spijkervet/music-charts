
import time
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from tqdm import tqdm
from charts.model import *

spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())


entries = HistoricalEntry().select(HistoricalEntry.spotify_id).distinct()

delete = ["codestring", "code_version", "echoprintstring", "echoprint_version", "synchstring", "synch_version", "rhythmstring", "rhythm_version"]
for e in tqdm(entries):
    data = spotify.audio_analysis(e.spotify_id)
    analyzer_version = data["meta"]["analyzer_version"]
    timestamp = data["meta"]["timestamp"]

    del data["meta"]
    for d in delete:
        if d in data["track"]:
            del data["track"][d]
    
    AudioAnalysis.insert(
        spotify_id=e.spotify_id,
        num_samples=data["track"]["num_samples"],
        duration=data["track"]["duration"],
        analysis_sample_rate=data["track"]["analysis_sample_rate"],
        analysis_channels=data["track"]["analysis_channels"],
        end_of_fade_in=data["track"]["end_of_fade_in"],
        start_of_fade_out=data["track"]["start_of_fade_out"],
        loudness=data["track"]["loudness"],
        tempo=data["track"]["tempo"],
        tempo_confidence=data["track"]["tempo_confidence"],
        time_signature=data["track"]["time_signature"],
        time_signature_confidence=data["track"]["time_signature_confidence"],
        key=data["track"]["key"],
        key_confidence=data["track"]["key_confidence"],
        mode=data["track"]["mode"],
        mode_confidence=data["track"]["mode_confidence"],
        bars=data["bars"],
        beats=data["beats"],
        sections=data["sections"],
        segments=data["segments"],
        tatums=data["tatums"],
        analyzer_version=analyzer_version,
        timestamp=timestamp,
    ).execute()