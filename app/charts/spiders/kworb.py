import scrapy
from charts.utils import get_dates
from datetime import date
import json
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta
from charts.items import ChartsItem
from charts.data import populate_tables, get_chart_ids, get_region_ids, get_itunes_regions

class KworbSpider(scrapy.Spider):
    name = "kworb"

    timezone_hours = 4 # EDT - UTC = +4
    chart = "itunes100"


    def start_requests(self):

        populate_tables()

        self.chart_ids = get_chart_ids()
        self.region_ids = get_region_ids()

        dates = get_dates(date(2013, 8, 13))
        urls = []
        for r, _ in get_itunes_regions().items():
            for d in dates:
                if r == "us":
                    r = ""
                urls.append(
                    "https://kworb.net/pop{}/archive/{}.html".format(
                        r, str(d).replace("-", "")
                    )
                )

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        url = response.url

        region = url.split("/")[3]
        if region == "pop":
            region = "us"
        else:
            region = region[3:]

        if region == "uk":
            region = "gb"

        soup = BeautifulSoup(response.text, "html.parser")
        recent_date = str(soup.find("b")).split("|")[1].replace("Last update: ", "").replace("EDT", "").strip()
        recent_date = datetime.strptime(recent_date, "%Y-%m-%d %H:%M:%S")
        recent_date = recent_date + timedelta(hours=self.timezone_hours)

        table = soup.find("table")
        table_body = table.find("tbody")
        table_header = table.find("thead")
        col_names = table_header.find_all("th")
        for idx, c in enumerate(col_names):
            col_names[idx] = c.text

        rows = table_body.find_all("tr")
        for row in rows:
            cols = row.find_all("td")
            cols = [ele.text.strip() for ele in cols]
            assert len(cols) == len(col_names), "Columns does not correspond with header"

            position = int(cols[0])
            if position <= 100:
                for idx, streams in enumerate(cols):
                    if idx > 1:
                        hours_after_recent = col_names[idx]
                        if hours_after_recent != "???":
                            if hours_after_recent == "Now":
                                hours_after_recent = 0

                            date = recent_date - timedelta(hours=float(hours_after_recent)) # hours ago
                            artist = cols[1].split(" - ")[0]
                            name = cols[1].split(" - ")[1]


                            yield ChartsItem(
                                chart_id=self.chart_ids[self.chart],
                                region_id=self.region_ids[region],
                                date=date,
                                position=position,
                                name=name,
                                artist=artist,
                                streams=streams,
                                spotify_id=None
                            )
