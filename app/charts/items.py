# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ChartsItem(scrapy.Item):
    # define the fields for your item here like:
    chart_id = scrapy.Field()
    region_id = scrapy.Field()
    date = scrapy.Field()
    position = scrapy.Field()
    name = scrapy.Field()
    artist = scrapy.Field()
    streams = scrapy.Field()
    spotify_id = scrapy.Field()
