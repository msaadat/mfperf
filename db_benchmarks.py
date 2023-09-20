import pathlib
import datetime
import numpy as np
import sqlite3
import pandas as pd

import psx


class BMDatabase:
    path_db_main = pathlib.Path(__file__).parent.resolve() / "benchmarks.db"
    path_db_attach = pathlib.Path(__file__).parent.resolve() / "benchmarks_attach.db"

    db_cache = {}

    def __init__(self):
        self.conn = self.connect()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def pd_read_sql_cached(self, qry, parse_dates=None):
        if qry in self.db_cache:
            return self.db_cache[qry].copy()
        else:
            df = pd.read_sql(qry, self.conn, parse_dates=parse_dates)
            self.db_cache[qry] = df.copy()
            return df

    def connect(self):
        conn = sqlite3.connect(self.path_db_main)
        return conn

    def connect_attach(self):
        conn = sqlite3.connect(self.path_db_attach)
        return conn

    def close(self):
        self.conn.commit()
        self.conn.close()

    def get_schema(self):
        schema = ""
        cursor = self.conn.cursor()
        cursor.execute("select sql from sqlite_master")
        for r in cursor.fetchall():
            schema = schema + r[0] + "; \n"
        cursor.close()
        return schema

    def make_attached_db(self):
        if self.path_db_attach.exists():
            self.path_db_attach.unlink()

        schema = self.get_schema()

        conn_attach = self.connect_attach()
        conn_attach.executescript(schema)
        conn_attach.commit()

        return conn_attach

    def get_latest_index_date(self):
        qry = "SELECT index_date FROM psx_indexes ORDER BY index_date DESC LIMIT 1;"
        res = self.conn.execute(qry)
        return res.fetchone()[0]

    def __fetch_index(self, start_date):
        df = psx.fetch_index(start_date)
        idx_ids = {"KSE 100": "1", "KMI 30": "3"}
        if df.shape[0] > 0:
            df = df[["index_date", "index", "close"]]
            df["index"] = df["index"].apply(lambda x: idx_ids[x])
            # df["index_date"] = pd.to_datetime(df["index_date"], errors="coerce")
            df.columns = ["index_date", "bm_id", "close"]

        return df

    def __fetch_scrips(self, start_date):
        df = psx.fetch_scrips(start_date)
        if df.shape[0] > 0:
            df.columns = [x.lower() for x in df.columns]
            df = df[["close_date", "symbol", "close", "ldcp", "volume"]]
            # df.columns = ["index_date", "bm_id", "close"]

        return df

    def update_attached(self, start_date):
        conn_attach = self.make_attached_db()

        print("Getting Indexes...")
        df = self.__fetch_index(start_date)
        df.to_sql("psx_indexes", conn_attach, if_exists="append", index=False)

        print("Getting Scrips...")
        df = self.__fetch_scrips(start_date)
        df.to_sql("psx_scrips", conn_attach, if_exists="append", index=False)

        print("\nDone.")

        conn_attach.close()

    def merge_attached(self, cutoff_date):
        self.conn.execute(f"DELETE FROM psx_indexes WHERE index_date>='{cutoff_date}';")
        self.conn.execute(f"DELETE FROM psx_scrips WHERE close_date>='{cutoff_date}';")
        self.conn.execute(f"ATTACH DATABASE '{self.path_db_attach}' AS new_db;")
        self.conn.execute("INSERT INTO psx_indexes SELECT * FROM new_db.psx_indexes;")
        self.conn.execute("INSERT INTO psx_scrips SELECT * FROM new_db.psx_scrips;")
        self.conn.commit()
        self.conn.execute("DETACH DATABASE new_db;")

    def fetch_scrips(self, start_date):
        return self.__fetch_scrips(start_date)

    def update_psx_sectors(self):
        df = psx.fetch_co_info()
        df = df[["sector_id", "sector_name"]]
        df = df.drop_duplicates()
        df.to_sql("psx_sectors", self.conn, if_exists="replace", index=False)

    def update_psx_coinfo(self):
        df = psx.fetch_co_info()
        df = df.drop(["sector_name"], axis=1)
        df.to_sql("psx_co_info", self.conn, if_exists="replace", index=False)

    def get_index_data(self, start_date, end_date, index_id=None):
        qry = f"SELECT * FROM psx_indexes WHERE index_date>='{start_date}' AND index_date<='{end_date}'"
        qry = qry + f" AND bm_id='{index_id}'" if index_id else ''
        qry = qry + " ORDER BY index_date ASC"
        df = self.pd_read_sql_cached(qry)
        return df

    def get_index_stddev(self, start_date, end_date, index_id=None):
        df = self.get_index_data(start_date, end_date, index_id)
        df['return'] = df['close'].pct_change()
        return df['return'].std(ddof=0)
    
    def get_index_daily_return(self, start_date, end_date, index_id):
        df = self.get_index_data(start_date, end_date, index_id)
        df['return'] = df['close'].pct_change()
        return df[['index_date', 'return']].dropna()
    
    def get_index_correl(self, start_date, end_date, index_id, symbol):
        idx_returns = self.get_index_daily_return(start_date, end_date, index_id)
        scrip_returns = self.get_scrip_daily_return(symbol, start_date, end_date)
        df = pd.merge(idx_returns, scrip_returns, how='left', left_on='index_date', right_on='close_date')
        df = df.fillna(0)

        return df[['return','ln_change']].corr().iloc[0,1]
    
    def get_scrip_return(self, start_date, end_date, symbol=None):
        if symbol == None:
            qry = f"SELECT * FROM psx_scrips WHERE close_date>'{start_date}' AND close_date<='{end_date}'"
        else:
            qry = f"SELECT * FROM psx_scrips WHERE symbol='{symbol}' AND close_date>'{start_date}' AND close_date<='{end_date}'"

        df = self.pd_read_sql_cached(qry)
        if df.empty:
            return 0
        
        df["ln_change"] = np.log(df["close"] / df["ldcp"])
        ret = np.exp(df.groupby("symbol")["ln_change"].sum().fillna(0)) - 1

        if symbol:
            return ret[symbol]

        return ret

    def get_scrip_stddev(self, symbol, start_date, end_date):
        qry = f"SELECT * FROM psx_scrips WHERE symbol='{symbol}' AND close_date>'{start_date}' AND close_date<='{end_date}'"
        df = self.pd_read_sql_cached(qry)
        df["ln_change"] = np.log(df["close"] / df["ldcp"])
        ret = df["ln_change"].std(ddof=0)

        return ret
    
    def get_scrip_daily_return(self, symbol, start_date, end_date):
        qry = f"SELECT * FROM psx_scrips WHERE symbol='{symbol}' AND close_date>'{start_date}' AND close_date<='{end_date}'"
        df = self.pd_read_sql_cached(qry)
        df["ln_change"] = np.log(df["close"] / df["ldcp"])
        ret = df[["close_date", "ln_change"]]

        return ret

    def get_scrip_correl(self, symbol1, symbol2, start_date, end_date):
        if symbol1 == symbol2:
            return 1.0
        
        qry = f"SELECT * FROM psx_scrips WHERE symbol in {(symbol1, symbol2)} AND close_date>'{start_date}' AND close_date<='{end_date}'"
        df = self.pd_read_sql_cached(qry)
        df["ln_change"] = np.log(df["close"] / df["ldcp"])
        df = df[['close_date','symbol','ln_change']]
        df = df.pivot(columns=['symbol'], index=['close_date'])
        df = df.fillna(0)
        ret = df.corr().iloc[0,1]
        return ret

    def get_scrip_avg_volume(self, symbol, start_date, end_date):
        qry = f"SELECT avg(volume) FROM psx_scrips WHERE symbol='{symbol}' AND close_date>'{start_date}' AND close_date<='{end_date}'"
        ret = self.conn.execute(qry)

        return ret.fetchone()[0]

    def get_scrip_traded_days(self, symbol, start_date, end_date):
        qry = f"SELECT count(symbol) FROM psx_scrips WHERE symbol='{symbol}' AND close_date>'{start_date}' AND close_date<='{end_date}'"
        ret = self.conn.execute(qry)

        return ret.fetchone()[0]
    
    def get_scrip_info(self, symbol):
        qry = f"SELECT * FROM psx_co_info WHERE symbol='{symbol}'"
        ret = self.conn.execute(qry)

        return ret.fetchone()

    def get_scrip_data(self, symbol, start_date=None, end_date=None, last=False):
        qry = f"SELECT * FROM psx_scrips WHERE symbol='{symbol}'"

        if end_date:
            qry = qry + f" AND close_date<='{end_date}'"

        if last:
            qry = qry + " ORDER BY close_date DESC LIMIT 1"
        else:
            if start_date:
                qry = qry + f" AND close_date>'{start_date}'"

        df = self.pd_read_sql_cached(qry)

        return df
    

if __name__ == "__main__":
    # start_date = datetime.date(2023, 8, 1)
    with BMDatabase() as db:
        # start_date = datetime.date.fromisoformat(db.get_latest_index_date()) + datetime.timedelta(days=1)
        # # start_date = datetime.date(2023, 7, 1)
        # db.update_attached(start_date)
        # db.merge_attached(start_date)
        # db.path_db_attach.unlink()
        
        # df = db.get_scrip_traded_days("MEBL", "2022-07-31", "2023-07-31")
        df = db.get_scrip_daily_return("HBL", "2023-08-01", "2023-08-31")
        df2 = db.get_index_correl("2023-08-01", "2023-08-31", 1, "HBL")
        print(df2)
        # db.update_psx_sectors()
        # df.to_excel("df.xlsx")
