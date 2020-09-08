import pymysql
from peewee import *

conn = pymysql.connect(host='charts_mysql', user='root', password='root')
conn.cursor().execute('CREATE DATABASE IF NOT EXISTS charts')
conn.close()

# db = SqliteDatabase("listings.db")
# Connect to a MySQL database on network.
db = MySQLDatabase('charts', user='root', password='root',
                         host='charts_mysql', port=3306)


class BaseModel(Model):
    class Meta:
        database = db

class Vendor(BaseModel):
    id = AutoField()
    name = CharField(unique=True)

class Chart(BaseModel):
    id = AutoField()
    name = CharField()
    identifier = CharField()
    interval = CharField()
    genre = CharField(null=True)
    vendor_id = ForeignKeyField(Vendor, backref='chart')


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
    track_id = CharField() # ForeignKeyField(T


class HistoricalEntry(BaseModel):
    id = AutoField()
    date = DateTimeField()
    position = IntegerField()
    name = CharField()
    artist = CharField()
    streams = IntegerField()
    spotify_id = CharField(null=True)
    chart_id = ForeignKeyField(Chart)
    region_id = ForeignKeyField(Region)

class Blacklist(BaseModel):
    id = AutoField()
    url = CharField()

db.connect()
db.create_tables([Chart, Region, Track, Artist, ChartEntry, HistoricalEntry, Blacklist, Vendor])

