import scrapy
import csv
from datetime import date, datetime, timedelta
from charts.data import populate_tables, get_chart_ids, get_region_ids, get_itunes_regions
from charts.items import ChartsItem
from charts.model import Chart, Region, ChartEntry, Blacklist, Vendor, HistoricalEntry
from charts.utils import get_dates

class SpotifySpider(scrapy.Spider):
    name = "spotify"

    def start_requests(self):

        populate_tables()

        self.chart_ids = get_chart_ids()
        self.region_ids = get_region_ids()

        urls = self.build_urls()

        print("URLs", len(urls))
        for d in urls:
            yield scrapy.Request(url=d["url"], callback=self.parse, meta=d)


    def parse(self, response):
        identifier = response.meta["identifier"]
        interval = response.meta["interval"]
        region = response.meta["region"]
        date = response.meta["date"]
        chart = "{}_{}".format(identifier, interval)

        rows = response.selector.xpath('//table[contains(@class, "chart-table")]/tbody/tr')
        for row in rows:
            spotify_id = row.xpath("(.//td)[1]/a/@href").get()
            position = row.xpath("(.//td)[2]/text()").get()
            name = row.xpath("(.//td)[4]/strong/text()").get()


            artist = row.xpath("(.//td)[4]/span/text()").get()
            if artist:
                artist = artist.replace("by ", "", 1).strip()

            streams = row.xpath("(.//td)[5]/text()").get()
            if streams:
                streams = int(streams.replace(",", ""))

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
        charts = (Chart
            .select()
            .join(Vendor)
        )

        regions = Region.select()

        composite_charts = list(
            HistoricalEntry
            .select(HistoricalEntry.date, HistoricalEntry.chart_id, HistoricalEntry.region_id)
            .distinct()
            .dicts()
        )

        composite_keys = set()
        for c in composite_charts:
            composite_keys.add("{}_{}_{}".format(c["chart_id"], c["region_id"], c["date"].date()))

        urls = []
        daily_dates = get_dates(date(2020, 1, 1))
        weekly_dates = [d for d in get_dates(date(2016, 12, 23)) if d.isoweekday() == 5]
        blacklist_urls = [b.url for b in Blacklist.select(Blacklist.url)]
        dates = None
        for c in charts:
            if c.vendor_id.name != "Spotify":
                continue

            for r in regions:
                if c.interval == "daily":
                    dates = daily_dates
                elif c.interval == "weekly":
                    dates = weekly_dates

                for d in dates:
                    composite_key = "{}_{}_{}".format(
                        c.id,
                        r.id,
                        d,
                    )
                    if composite_key not in composite_keys:
                        if c.interval == "daily":
                            url = "https://spotifycharts.com/{}/{}/{}/{}".format(
                                c.identifier, r.country_code, c.interval, d
                            )
                        elif c.interval == "weekly":
                            url = "https://spotifycharts.com/{}/{}/{}/{}--{}".format(
                                c.identifier, r.country_code, c.interval, d, d + timedelta(days=7)
                            )
                        else:
                            continue

                        if url not in blacklist_urls:
                            urls.append({"url": url, "identifier": c.identifier, "region": r.country_code, "interval": c.interval, "date": d})
        return urls


