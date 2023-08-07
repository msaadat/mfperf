import pathlib
import datetime
import sqlite3
import pandas as pd

import psx

class BMDatabase():
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
        cursor.execute('select sql from sqlite_master')
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
    

    def __fetch_index(self, start_date):
        df = psx.fetch_index(start_date)
        idx_ids = {"KSE 100": "1", "KMI 30": "3"}
        if df.shape[0] > 0:
            df = df[['index_date','index','close']]
            df['index'] = df['index'].apply(lambda x: idx_ids[x])
            # df["index_date"] = pd.to_datetime(df["index_date"], errors="coerce")
            df.columns = ["index_date", "bm_id", "close"]
            
        return df

    def __fetch_scrips(self, start_date):
        df = psx.fetch_scrips(start_date)
        if df.shape[0] > 0:
            df.columns = [x.lower() for x in df.columns]
            df = df[['close_date','symbol','close', 'ldcp', 'volume']]
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

if __name__ == '__main__':
    start_date = datetime.date(2023, 8, 4)
    with BMDatabase() as db:
        db.update_attached(start_date)
        db.merge_attached(start_date)
        db.path_db_attach.unlink()
        
