import pandas as pd
import datetime
import json
import zipfile
import io

import net_utils

def fetch_scrips_single(dt):
    data = {"date": ""}
    url = "https://dps.psx.com.pk/historical"

    data["date"] = dt.date().isoformat()
    a = net_utils.post(url, data=data)
    try:
        df = pd.read_html(a.content)[0]
        if not df.empty:
            df.columns = [a for a in df.columns.to_flat_index()]
            df = df[["SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "LDCP", "VOLUME"]]
            df["close_date"] = data["date"]
    except:
        df = pd.DataFrame()

    return df

def fetch_scrips(start_date=None, dt_list=None):
    end_date = datetime.datetime.today()
    # end_date = datetime.datetime(2023, 10, 24)

    df_list = []

    if start_date:
        while start_date < end_date:
            df_list.append(fetch_scrips_single(start_date))
            start_date = start_date + datetime.timedelta(days=1)

    else:
        for i in dt_list:
            df_list.append(fetch_scrips_single(i))

    if df_list != []:
        df = pd.concat(df_list)
    else:
        df = pd.DataFrame()

    return df


def fetch_index(start_date):
    end_date = datetime.date.today()
    indexes = {"KSE 100": "1", "KMI 30": "3"}
    data = {
        "from": start_date.date().isoformat(),
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
        a = net_utils.post(url, data=data, headers=headers)
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
    r = net_utils.get(url)
    zdata = io.BytesIO(r.content)
    zfile = zipfile.ZipFile(zdata)
    data = io.BytesIO(zfile.read('listed_cmp.lst'))
    df = pd.read_csv(data, sep='|', header=None).dropna(axis=1)
    df.columns = ['symbol', 'co_name', 'sector_id','sector_name','shares']

    zfile.close()
    return df

if __name__ == "__main__":
    start_date = datetime.datetime(2023, 10, 20)
    # df = fetch_scrips(start_date)
    # print(df)
    dt_list = ["2021-11-07",
        "2021-10-19",
        "2021-10-17",
        "2021-10-03",
        "2018-12-16",
        "2021-11-14",
        "2021-09-26",
        "2021-10-10"]
    dt_list = [datetime.datetime.fromisoformat(x) for x in dt_list]
    df = fetch_scrips(dt_list=dt_list)
    # print(df)
    df.to_csv('scrips.csv')
