from datetime import datetime, timedelta
import sqlite3
import time
import pathlib
import concurrent.futures

import pandas as pd

import mufap


class MFDatabase():
    path_db_main = pathlib.Path(__file__).parent.resolve() / "mufap.db"
    path_db_attach = pathlib.Path(__file__).parent.resolve() / "mufap_attach.db"

    db_cache = {}

    def __init__(self):
        self.conn = self.connect()
        # self.conn_attach = self.connect_attach()

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


    def get_latest_nav_date(self):
        qry = """
        SELECT nav_date, date_count FROM (SELECT nav_date, COUNT(nav_date) AS date_count
        FROM navs
        GROUP BY nav_date
        ORDER BY nav_date DESC
        LIMIT 10) WHERE date_count>=300;
        """
        res = self.conn.execute(qry)
        return res.fetchone()[0]


    def get_fund_id(self, fund_name):
        cur = self.conn.cursor()
        res = cur.execute(f"SELECT fund_id FROM funds WHERE fund_name='{fund_name}'")
        return res.fetchone()[0]


    def get_fund_backward(self, fund_id):
        cur = self.conn.cursor()
        res = cur.execute(f"SELECT backward FROM funds WHERE fund_id='{fund_id}'")
        return res.fetchone()[0]


    # def db_create(self):
    #     conn = sqlite3.connect("data.db")
    #     cursor = conn.cursor()
    #     with open("schema.sql", "r") as file:
    #         schema = file.read()
    #     cursor.executescript(schema)
    #     conn.commit()
    #     conn.close()


    def get_fundlist(self, with_cats=False, with_amcs=False):
        df_funds = self.pd_read_sql_cached("SELECT * from funds", parse_dates=["inception"])

        if with_cats:
            df_funds = pd.merge(df_funds, self.get_cat_list(), on="cat_id", how="left")

        if with_amcs:
            df_funds = pd.merge(df_funds, self.get_amc_list(), on="amc_id", how="left")

        return df_funds


    def get_cat_list(self, amc_id=""):
        qry = "SELECT DISTINCT categories.* FROM categories INNER JOIN funds ON categories.cat_id = funds.cat_id"

        if amc_id != "" and amc_id != "0":
            qry = qry + f" WHERE funds.amc_id='{amc_id}'"

        df = self.pd_read_sql_cached(qry)
        return df


    def get_amc_list(self, all=False):
        qry = "SELECT DISTINCT amcs.* FROM amcs INNER JOIN funds ON amcs.amc_id = funds.amc_id"

        if all:
            qry = "SELECT * from amcs"

        df = self.pd_read_sql_cached(qry)
        return df


    def update_fundlist(self, rebuild=False):
        fund_list = mufap.mufap_funds_list()
        fund_list.columns = [
            "fund_id",
            "fund_name",
            "cat_id",
            "inception",
            "amc_id",
            "backward",
            "mufap_tab",
        ]

        if rebuild:
            cur = self.conn.cursor()
            cur.execute(f"drop table if exists funds")
            cur.execute(f"drop table if exists amcs")
            cur.execute(f"drop table if exists categories")

            fund_list.to_sql("funds", self.conn, if_exists="append", index=False)

            amc_list = mufap.mufap_amc_list()
            amc_list.to_sql("amcs", self.conn, if_exists="append", index=False)

            cat_list = mufap.mufap_category_list()
            cat_list.to_sql("categories", self.conn, if_exists="append", index=False)
        else:
            df_funds = self.get_fundlist()
            fund_list["exists"] = fund_list["fund_id"].isin(df_funds["fund_id"])
            df_new = fund_list[fund_list["exists"] == False].drop(columns="exists")
            if not df_new.empty:
                df_new.to_sql("funds", self.conn, if_exists="append", index=False)
    

    def update_attached(self, start_date):
        df_funds = self.get_fundlist()

        conn_attach = self.make_attached_db()

        n = df_funds.shape[0]
        i = 0

        print("Getting NAVs...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(lambda x: self.__fetch_navs(start_date, x), row):idx for (idx, row) in df_funds.iterrows()
            }

            for future in concurrent.futures.as_completed(futures):
                id = futures[future]
                try:
                    df = future.result()
                    if not df.empty:
                        # df["backward"] = df["fund_id"].apply(lambda x: self.get_fund_backward(x))
                        backward = df_funds[df_funds["fund_id"]==df["fund_id"].iloc[0]]["backward"].iloc[0]
                    
                        if backward == 1:
                            df["nav_date"] = df["nav_date"] - timedelta(days=1)
                            df = df[df["nav_date"] >= start_date]
                        
                        df.to_sql("navs", conn_attach, if_exists="append", index=False)
                except Exception as e:
                    print(f"Error occurred for item {df_funds.iloc[id]}: {e}")
                
                i += 1
                print(f"Updating {i} / {n}", end="\r")
        

        print("\nGetting Payouts...")
        df_funds_novps = df_funds[df_funds['mufap_tab']!='vps']

        n = df_funds_novps.shape[0]
        i = 1

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(lambda x: self.__fetch_payouts(start_date, x), row):idx for (idx, row) in df_funds_novps.iterrows()
            }

            for future in concurrent.futures.as_completed(futures):
                id = futures[future]
                try:
                    df = future.result()
                    if not df.empty:
                        backward = df_funds[df_funds["fund_id"]==df["fund_id"].iloc[0]]["backward"].iloc[0]
                        if backward == 1:
                            df["payout_date"] = df["payout_date"] - timedelta(days=1)
                            df = df[df["payout_date"] >= start_date]
                        
                        df.to_sql("payouts", conn_attach, if_exists="append", index=False)
                        
                except Exception as e:
                    print(f"Error occurred for item {df_funds.iloc[id]}: {e}")
                
                i += 1
                print(f"Updating {i} / {n}", end="\r")

        print("\nDone.")

        conn_attach.close()

    def merge_attached(self, cutoff_date):
        self.conn.execute(f"DELETE FROM navs WHERE nav_date>='{cutoff_date}';")
        self.conn.execute(f"DELETE FROM payouts WHERE payout_date>='{cutoff_date}';")
        self.conn.execute(f"ATTACH DATABASE '{self.path_db_attach}' AS new_db;")
        self.conn.execute("INSERT INTO navs SELECT * FROM new_db.navs;")
        self.conn.execute("INSERT INTO payouts SELECT * FROM new_db.payouts;")
        self.conn.commit()
        self.conn.execute("DETACH DATABASE new_db;")

    def __fetch_navs(self, start_date, row):
        # print(f"Getting {row['fund_id']}")
        df = mufap.mufap_fund_navs(start_date, row["fund_id"], mufap_tab=row["mufap_tab"])
        
        if df.shape[0] > 0:
            df.columns = ["fund_id", "nav", "nav_date"]
            df["fund_id"] = row["fund_id"]
            
        return df


    def __fetch_payouts(self, start_date, row):
        df = mufap.mufap_fund_payouts(start_date, row["fund_id"], row["cat_id"])
        if df.shape[0] > 0:
            df.columns = ["fund_id", "payout", "exnav", "payout_date"]
            df["fund_id"] = row["fund_id"]

        return df


    def compute_return(self, df_funds, op_date, end_date):
        df_navs_op = self.pd_read_sql_cached(f"SELECT * from navs WHERE nav_date='{op_date}'")
        
        df_navs_cl = self.pd_read_sql_cached(f"SELECT * from navs WHERE nav_date='{end_date}'")

        df_funds["annualize"] = df_funds["category"].apply(mufap.mufap_cat_annualize)

        df_navs_op = df_navs_op[df_navs_op["nav"] != 0]
        df_navs_cl = df_navs_cl[df_navs_cl["nav"] != 0]

        # add missing op navs
        df_funds_missing = pd.merge(df_funds, df_navs_op, on='fund_id', how='left')
        df_funds_missing = df_funds_missing[df_funds_missing["nav"].isna() & (df_funds_missing["inception"]<=op_date)]
        df_funds_missing = df_funds_missing.merge(df_navs_cl, on='fund_id', how='left')
        df_funds_missing = df_funds_missing[~df_funds_missing["nav_y"].isna()]
        
        df_funds_missing_eq = df_funds_missing[~df_funds_missing["annualize"]]
        
        if not df_funds_missing_eq.empty:
            df_funds_missing_eq = self.get_navs_last(df_funds_missing_eq['fund_id'], op_date)
            df_navs_op = pd.concat([df_navs_op, df_funds_missing_eq])

        df_funds_missing_fi = df_funds_missing[df_funds_missing["annualize"]]
        if not df_funds_missing_fi.empty:
            df_funds_missing_fi["nav"] = df_funds_missing_fi['fund_id'].apply(lambda x: self.get_nav_interpolated(x, op_date))
            df_funds_missing_fi['nav_date'] = op_date
            df_funds_missing_fi = df_funds_missing_fi[['fund_id', 'nav_date','nav']]
            df_navs_op = pd.concat([df_navs_op, df_funds_missing_fi])

        del df_navs_op["nav_date"]
        del df_navs_cl["nav_date"]

        if not df_navs_cl.empty:
            df = df_navs_op.merge(df_navs_cl, on="fund_id", how="inner")
            df = df[(df["nav_x"] != 0) & (df["nav_y"] != 0)]

            df_payouts = pd.read_sql(
                f"SELECT * FROM payouts WHERE payout_date>'{op_date}' AND payout_date<='{end_date}'",
                self.conn,
            )
            if df_payouts.empty:
                df_payouts["div_factor"] = 1
            else:
                df_payouts["div_factor"] = (
                    df_payouts["exnav"] + df_payouts["payout"]
                ) / df_payouts["exnav"]
                df_payouts = df_payouts[["fund_id", "div_factor"]]
                df_payouts = df_payouts.groupby("fund_id").prod()

            df = pd.merge(df, df_payouts, on="fund_id", how="left")
            df["div_factor"] = df["div_factor"].fillna(1)
            df["return"] = df["nav_y"] / df["nav_x"] * df["div_factor"] - 1

            return df

        else:
            return pd.DataFrame()

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

    def __annualize_return(self, ret, delta_days):
        r = 0
        if delta_days > 365:
            r = (ret + 1) ** (365 / delta_days) - 1
        else:
            r = ret * 365 / delta_days
        return r

    def annualize_retun(self, df, ret_col, delta_days):
        df["annualize"] = df["category"].apply(mufap.mufap_cat_annualize)

        df[ret_col] = df.apply(
            lambda x: self.__annualize_return(x[ret_col], delta_days)
            if x["annualize"]
            else x[ret_col],
            axis=1,
        )
        return df
    
    def get_nav_interpolated(self, fund_id, nav_date):
        df_nav_after = self.pd_read_sql_cached(f"SELECT * from navs WHERE fund_id='{fund_id}'" 
                                               f" AND nav_date>'{nav_date}' AND nav!=0 ORDER BY nav_date ASC LIMIT 1", parse_dates=['nav_date'])

        if df_nav_after.empty:
            return None
        
        df_nav_before = self.pd_read_sql_cached(f"SELECT * from navs WHERE fund_id='{fund_id}'"
                                                f" AND nav_date<'{nav_date}' AND nav!=0 ORDER BY nav_date DESC LIMIT 1", parse_dates=['nav_date'])

        if df_nav_before.empty:
            return None
        
        df = pd.concat([df_nav_before, df_nav_after])

        df_payouts = pd.read_sql(
                f"SELECT * FROM payouts WHERE fund_id='{fund_id}' "
                f"AND payout_date>'{df['nav_date'].min()}' AND payout_date<='{df['nav_date'].max()}'",
                self.conn, parse_dates=['payout_date']
            )
        if not df_payouts.empty:
            df_payouts['full_nav'] = df_payouts['exnav'] + df_payouts['payout']
            df = df.merge(df_payouts[['payout_date', 'full_nav']], how='left', left_on='nav_date', right_on='payout_date')
            df['full_nav'] = df['full_nav'].fillna(df['nav'])
            df = df.drop(['nav','payout_date'], axis=1)
            df = df.rename(columns={'full_nav':'nav'})

        df = df.drop(['fund_id'], axis=1)
        df = df.set_index('nav_date').asfreq('D').interpolate()
        nav = df.loc[nav_date]
        return nav['nav']
    
    def get_navs_last(self, df_fund_ids, nav_date):
        fund_ids = str(df_fund_ids.tolist())[1:-1]
        qry = (f"SELECT fund_id, nav, MAX(nav_date) AS max_nav_date"
                f" FROM navs"
                f" WHERE nav_date <= '{nav_date}' AND nav != 0"
                f" AND fund_id IN ({fund_ids})"
                f" GROUP BY fund_id")
        df = self.pd_read_sql_cached(qry)

        return df

        
    def get_performance(self, op_date, end_date):
        df_funds = self.get_fundlist(with_cats=True)

        df = self.compute_return(df_funds, op_date, end_date)

        if df.empty:
            return df

        df = pd.merge(df_funds, df, on="fund_id", how="left")
        
        delta_days = (end_date - op_date).days
        df = self.annualize_retun(df, "return", delta_days)
        df["return"] = round(df["return"] * 100, 2)

        return df

    def get_performance_multi(self, dates_list):
        df_funds = self.get_fundlist(with_cats=True)

        for title, op_date, end_date in dates_list:
            df = self.compute_return(df_funds, op_date, end_date)
            df = df.rename(columns={'return': title})
            df_funds = df_funds.merge(df[["fund_id", title]], on="fund_id", how="left")
            delta_days = (end_date - op_date).days
            df_funds = self.annualize_retun(df_funds, title, delta_days)
            df_funds[title] = round(df_funds[title] * 100, 2)

        return df_funds


if __name__ == "__main__":

    with MFDatabase() as db:
        # db_create()

        # db_update_fundlist(conn)
        # db_update_fundlist(conn, True)
        #db.db_make_attached_db()
        cutoff_date = datetime(2023, 7, 22)
        db.update_attached(cutoff_date)
        #db.update_attached(cutoff_date)
        # db.merge_attached(cutoff_date)
        # db.path_db_attach.unlink()
        #db.db_daily_update(datetime(2023, 7, 15))

        # end_date = datetime(2023,7,27)
        # start_date = datetime(2023,7,28)
        # df = db_get_performance_multi(conn, dates_list)
        # df = db_get_performance(conn, start_date, end_date)
        # df.to_excel('df.xlsx')

    
