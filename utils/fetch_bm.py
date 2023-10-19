import pandas as pd
import pathlib
import datetime

import sys

from bs4 import BeautifulSoup
import openpyxl as xl
from openpyxl.utils.dataframe import dataframe_to_rows

sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import db_benchmarks
import net_utils


def fetch_index_weights():
    xl_file_path = pathlib.Path("C:/Users/Mian Muhammad Sadaat/Dropbox/Work/AWT RC/Reports/") / "PSX Index weights.xlsx"
    wb = xl.Workbook()

    urls = {"KSE100":"https://dps.psx.com.pk/indices/KSE100",
            "KMI30":"https://dps.psx.com.pk/indices/KMI30"}

    for k, i in urls.items():
        r = net_utils.get(i)
        print(f'Fetching {k} weights')
        soup = BeautifulSoup(r.content, 'html.parser')
        tbl = soup.find('table')
        tag_divs = soup.find_all('div', {'class': 'tag'})
        for i in tag_divs:
            i.decompose()

        df = pd.read_html(str(tbl))[0]
        df['IDX WTG (%)'] = pd.to_numeric(df['IDX WTG (%)'].str.strip('%')).div(100)

        ws = wb.create_sheet(k)

        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)

    wb.save(xl_file_path)


db = db_benchmarks.BMDatabase()

start_date = datetime.datetime.fromisoformat(db.get_latest_fi_date()) + datetime.timedelta(days=1)
# # start_date = datetime.date(2023, 7, 1)
db.update_attached(start_date)
db.merge_attached(start_date)
db.path_db_attach.unlink()

db.close()

fetch_index_weights()