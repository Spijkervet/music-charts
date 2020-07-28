from peewee import *


db = SqliteDatabase("spotify.db")


class BaseModel(Model):
    class Meta:
        database = db


class Chart(BaseModel):
    id = AutoField()
    name = CharField()
    url = CharField()
    interval = CharField()
    genre = CharField(null=True)


class Region(BaseModel):
    id = AutoField()
    country_code = CharField(unique=True)
    name = CharField()


class Artist(BaseModel):
    id = AutoField()
    name = CharField()
    spotify_id = CharField(index=True, null=True)


class Track(BaseModel):
    id = AutoField()
    name = CharField()
    spotify_id = CharField(index=True, unique=True)
    artist_id = ForeignKeyField(Artist)


class ChartEntry(BaseModel):
    id = AutoField()
    date = DateField()
    position = IntegerField()
    streams = IntegerField()
    chart_id = ForeignKeyField(Chart)
    region_id = ForeignKeyField(Region)
    track_id = ForeignKeyField(Track)

class Blacklist(BaseModel):
    id = AutoField()
    url = CharField()

db.connect()
db.create_tables([Chart, Region, Track, Artist, ChartEntry, Blacklist])

