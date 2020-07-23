import os
import csv
import pandas as pd
import requests
from datetime import datetime, date, timedelta
import concurrent.futures
import urllib.request


def get_spotify_chart(url, timeout):
    data = []
    url, chart, region, chart_type, date = url
    with requests.Session() as s:
        r = s.get(url)
        if r.status_code == 200:
            r = r.content.decode("utf-8")
            reader = csv.reader(r.splitlines(), delimiter=",")
            next(reader)
            next(reader)
            for row in reader:
                if len(row) == 1:
                    return None

                position, track_name, artist, streams, spotify_id = row
                spotify_id = spotify_id.split("/")[-1]
                data.append(
                    [
                        chart,
                        region,
                        chart_type,
                        date,
                        position,
                        track_name,
                        artist,
                        streams,
                        spotify_id,
                    ]
                )
    return data


if __name__ == "__main__":
    DATA_DIR = "./datasets/spotify"
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    CHART = os.path.join(DATA_DIR, "historical_chart.csv")
    BLACKLIST = os.path.join(DATA_DIR, "blacklist.csv")

    start_date = date(2017, 1, 1)
    end_date = datetime.now().date()
    delta = end_date - start_date
    dates = [start_date + timedelta(days=day) for day in range(delta.days + 1)]

    regions = {
        "us": "United States",
        "gb": "United Kingdom",
        "ad": "Andorra",
        "ar": "Argentina",
        "at": "Austria",
        "au": "Australia",
        "be": "Belgium",
        "bg": "Bulgaria",
        "bo": "Bolivia",
        "br": "Brazil",
        "ca": "Canada",
        "ch": "Switzerland",
        "cl": "Chile",
        "co": "Colombia",
        "cr": "Costa Rica",
        "cy": "Cyprus",
        "cz": "Czech Republic",
        "de": "Germany",
        "dk": "Denmark",
        "do": "Dominican Republic",
        "ec": "Ecuador",
        "ee": "Estonia",
        "es": "Spain",
        "fi": "Finland",
        "fr": "France",
        "gr": "Greece",
        "gt": "Guatemala",
        "hk": "Hong Kong",
        "hn": "Honduras",
        "hu": "Hungary",
        "id": "Indonesia",
        "ie": "Ireland",
        "il": "Israel",
        "in": "India",
        "is": "Iceland",
        "it": "Italy",
        "jp": "Japan",
        "lt": "Lithuania",
        "lu": "Luxembourg",
        "lv": "Latvia",
        "mc": "Monaco",
        "mt": "Malta",
        "mx": "Mexico",
        "my": "Malaysia",
        "ni": "Nicaragua",
        "nl": "Netherlands",
        "no": "Norway",
        "nz": "New Zealand",
        "pa": "Panama",
        "pe": "Peru",
        "ph": "Philippines",
        "pl": "Poland",
        "pt": "Portugal",
        "py": "Paraguay",
        "ro": "Romania",
        "se": "Sweden",
        "sg": "Singapore",
        "sk": "Slovakia",
        "sv": "El Salvador",
        "th": "Thailand",
        "tr": "Turkey",
        "tw": "Taiwan",
        "uy": "Uruguay",
        "vn": "Vietnam",
        "za": "South Africa",
    }

    charts = ["regional"]
    chart_type = "daily"

    print("Reading stream file")
    columns = [
        "chart",
        "region",
        "chart_type",
        "date",
        "position",
        "track_name",
        "artist",
        "streams",
        "spotify_id",
    ]

    if os.path.exists(CHART):
        _spotify_df = pd.read_csv(CHART, index_col=0)
        print(_spotify_df)
        _spotify_df["date"] = pd.to_datetime(_spotify_df["date"]).dt.date
        spotify_df = _spotify_df[
            ["chart", "region", "chart_type", "date"]
        ].drop_duplicates()
    else:
        _spotify_df = pd.DataFrame([], columns=columns)
        spotify_df = _spotify_df

    # construct dataframe with all possible values for charts/regions/dates/etc.
    print("Constructing dataframe with all charts/regions/dates/etc.")
    spotify_df_2 = []
    for chart in charts:
        for region in regions.keys():
            for date in dates:
                spotify_df_2.append([chart, region, chart_type, date])

    spotify_df_2 = pd.DataFrame(
        data=spotify_df_2, columns=["chart", "region", "chart_type", "date"]
    )
    diff_df = pd.concat([spotify_df, spotify_df_2]).drop_duplicates(keep=False)
    diff_df = diff_df.sort_values(by=["date"])

    print("Constructing urls")
    spotify_urls = []

    if os.path.exists(BLACKLIST):
        blacklist = pd.read_csv(BLACKLIST, names=["data_id"]).drop_duplicates()
        blacklist = blacklist["data_id"].to_list()
    else:
        blacklist = []

    for idx, row in diff_df.iterrows():
        data_id = "{}-{}-{}-{}".format(row["chart"], row["region"], row["chart_type"], row["date"])

        daily_url = "https://spotifycharts.com/{}/{}/{}/{}/download".format(
            row["chart"], row["region"], row["chart_type"], row["date"]
        )
        spotify_urls.append(
            [daily_url, row["chart"], row["region"], row["chart_type"], row["date"]]
        )

    all_data = []
    # We can use a with statement to ensure threads are cleaned up promptly
    reqs = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {
                executor.submit(get_spotify_chart, url, 60): url for url in spotify_urls
        }
        print("Starting to fetch data")
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data_id = "{}-{}-{}-{}".format(url[1], url[2], url[3], url[4])
                data = future.result()
                if data:
                    all_data.extend(data)
                elif data_id not in blacklist:
                    print("Added to blacklist:", data_id)
                    blacklist.append(data_id)
                print(reqs, "/", len(spotify_urls))
            except Exception as exc:
                print("%r generated an exception: %s" % (url, exc))
            reqs += 1

    tmp_df = pd.DataFrame(
        data=all_data,
        columns=[
            "chart",
            "region",
            "chart_type",
            "date",
            "position",
            "track_name",
            "artist",
            "streams",
            "spotify_id",
        ],
    )

    print("Constructing full dataframe")
    _spotify_df = _spotify_df.append(tmp_df)
    _spotify_df = _spotify_df.sort_values(by=["chart", "region", "chart_type", "date"])
    _spotify_df.to_csv(CHART)

    print("Writing blacklist")
    pd.DataFrame(data=blacklist).to_csv(BLACKLIST, index=False, header=None)
    print("Done")


