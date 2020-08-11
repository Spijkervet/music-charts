from peewee import *


db = SqliteDatabase("spotify.db")


class BaseModel(Model):
    class Meta:
        database = db


class Chart(BaseModel):
    id = AutoField()
    name = CharField()
    url = CharField()
    genre = CharField(null=True)


class Region(BaseModel):
    id = AutoField()
    country_code = CharField(unique=True)
    name = CharField()


class Artist(BaseModel):
    id = AutoField()
    name = CharField(unique=True)
    spotify_id = CharField(null=True)


class Track(BaseModel):
    id = AutoField()
    name = CharField()
    artist_id = ForeignKeyField(Artist)
    spotify_id = CharField(index=True, unique=True, null=True)


class ChartEntry(BaseModel):
    id = AutoField()
    date = DateTimeField()
    position = IntegerField()
    streams = IntegerField()
    chart_id = ForeignKeyField(Chart)
    region_id = ForeignKeyField(Region)
    track_id = CharField() # ForeignKeyField(Track)

class Blacklist(BaseModel):
    id = AutoField()
    url = CharField()

db.connect()
db.create_tables([Chart, Region, Track, Artist, ChartEntry, Blacklist])

