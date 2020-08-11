
class Kworb:
    def __init__(self, chart, region, date, position, name, artist, streams):
        self.chart = chart
        self.region = region
        self.date = date
        self.position = position
        self.name = name
        self.artist = artist
        self.streams = streams
        self.spotify_id = None

class Spotify:

    def __init__(self, chart, region, interval, date, position, name, artist, streams, spotify_id):
        self.chart = chart
        self.region = region
        self.interval = interval
        self.date = date
        self.position = position
        self.name = name
        self.artist = artist
        self.streams = streams
        self.spotify_id = spotify_id