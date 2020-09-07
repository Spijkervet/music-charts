import scrapy
import csv
from datetime import date, datetime, timedelta

from charts.data import populate_tables, get_chart_ids, get_region_ids, get_itunes_regions
from charts.items import ChartsItem
from charts.model import Chart, Region, ChartEntry, Blacklist
from charts.utils import get_dates

class SpotifySpider(scrapy.Spider):
    name = "spotify"

    def start_requests(self):

        populate_tables()

        self.chart_ids = get_chart_ids()
        self.region_ids = get_region_ids()

        urls = self.build_urls()

        for d in urls:
            yield scrapy.Request(url=d["url"], callback=self.parse, meta=d)


    def parse(self, response):
        chart = response.meta["chart"]
        region = response.meta["region"]
        date = response.meta["date"]
        reader = csv.reader(response.text.splitlines(), delimiter=",")
        next(reader)
        next(reader)
        for row in reader:
            if len(row) == 1:
                return None
            elif len(row) == 4:
                position, name, artist, spotify_id = row
                streams = None
            else:
                position, name, artist, streams, spotify_id = row
            
            spotify_id = spotify_id.split("/")[-1]

            yield ChartsItem(
                chart_id=self.chart_ids[chart],
                region_id=self.region_ids[region],
                date=date,
                position=position,
                name=name,
                artist=artist,
                streams=streams,
                spotify_id=spotify_id,
            )

        
    def build_urls(self):
        charts = Chart.select()
        regions = Region.select()

        composite_charts = list(
            ChartEntry.select(ChartEntry.date, ChartEntry.chart_id, ChartEntry.region_id)
            .distinct()
            .dicts()
        )
        composite_keys = set()
        for c in composite_charts:
            composite_keys.add("{}_{}_{}".format(c["chart_id"], c["region_id"], c["date"]))

        chart_ids = get_chart_ids()
        region_ids = get_region_ids()
        urls = []
        daily_dates = get_dates(date(2020, 1, 1))
        weekly_dates = [d for d in get_dates(date(2020, 1, 1)) if d.isoweekday() == 4]
        blacklist_urls = [b.url for b in Blacklist.select(Blacklist.url)]
        dates = None
        for c in charts:
            if "_" not in c.url: # TODO
                continue
            for r in regions:
                interval = c.url.split("_")[1]
                if interval == "daily":
                    dates = daily_dates
                elif interval == "weekly":
                    dates = weekly_dates
                for d in dates:
                    composite_key = "{}_{}_{}".format(
                        chart_ids["{}".format(c.url)],
                        region_ids[r.country_code],
                        d,
                    )
                    if composite_key not in composite_keys:
                        url = "https://spotifycharts.com/{}/{}/{}/{}/download".format(
                            c.url.split("_")[0], r.country_code, interval, d
                        )

                        if url not in blacklist_urls:
                            urls.append({"url": url, "chart": c.url, "region": r.country_code, "interval": interval, "date": d})
        return urls


