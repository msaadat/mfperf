import requests
import pandas as pd
import datetime
import json
import re
import os
import zipfile
import io


def fetch_scrips(start_date):
    # end_date = datetime.date.fromisoformat("2022-01-21")
    end_date = datetime.date.today()
    data = {"date": ""}
    url = "https://dps.psx.com.pk/historical"

    df_list = []
    while start_date < end_date:
        data["date"] = start_date.isoformat()
        a = requests.post(url, data=data)
        try:
            df = pd.read_html(a.content)[0]
            if not df.empty:
                df.columns = [a for a in df.columns.to_flat_index()]
                df = df[["SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "LDCP", "VOLUME"]]
                df["close_date"] = data["date"]
                df_list.append(df)
        except:
            pass
        start_date = start_date + datetime.timedelta(days=1)

    if df_list != []:
        df = pd.concat(df_list)
    else:
        df = pd.DataFrame()

    return df


def fetch_index(start_date):
    end_date = datetime.date.today()
    indexes = {"KSE 100": "1", "KMI 30": "3"}
    data = {
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        "indexid": "1",
        "term": "1",
        "term_table": "index_rates",
    }
    url = "https://www.khistocks.com/ajax/archive/"
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
    }

    df_list = []

    for key, i in indexes.items():
        data["indexid"] = i
        a = requests.post(url, data=data, headers=headers)
        js = json.loads(a.content)
        df = pd.DataFrame(js["data"])

        if not df.empty:
            df.drop("sDate", axis=1, inplace=True)
            df["sIndex"] = key
            df.columns = [
                "index_date",
                "index",
                "open",
                "high",
                "low",
                "close",
                "turnover",
                "mcap",
                "tradedval",
            ]
            for i in df.columns[2:]:
                df[i] = pd.to_numeric(df[i].str.replace(",", ""))
            df = df.sort_values(by=["index_date"])
            df_list.append(df)

    if df_list != []:
        df = pd.concat(df_list)
    else:
        df = pd.DataFrame()

    return df

def fetch_co_info():
    url = "https://dps.psx.com.pk/download/text/listed_cmp.lst.Z"
    r = requests.get(url)
    zdata = io.BytesIO(r.content)
    zfile = zipfile.ZipFile(zdata)
    data = io.BytesIO(zfile.read('listed_cmp.lst'))
    df = pd.read_csv(data, sep='|', header=None).dropna(axis=1)
    df.columns = ['symbol', 'co_name', 'sector_id','sector_name','shares']

    zfile.close()
    return df

if __name__ == "__main__":
    start_date = datetime.date(2023, 8, 4)
    # df = fetch_scrips(start_date)
    # print(df)
    df = fetch_co_info()
    print(df)
