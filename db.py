from datetime import datetime, timedelta
import sqlite3
import time
import pathlib

import pandas as pd

# from win32com import client as win32
import openpyxl as xl
from openpyxl.utils.dataframe import dataframe_to_rows
from bs4 import BeautifulSoup

from mufap import *

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
}
db_cache = {}

def pd_read_sql_cached(qry, conn, parse_dates=None):
    if qry in db_cache:
        return db_cache[qry].copy()
    else:
        df = pd.read_sql(qry, conn, parse_dates=parse_dates)
        db_cache[qry] = df.copy()
        return df
    
def db_connect():
    conn = sqlite3.connect(pathlib.Path(__file__).parent.resolve() / "data.db")
    return conn


def db_close(conn):
    conn.commit()
    conn.close()


def db_get_fund_id(conn, fund_name):
    cur = conn.cursor()
    res = cur.execute(f"SELECT fund_id FROM funds WHERE fund_name='{fund_name}'")
    return res.fetchone()[0]


def db_get_fund_backward(conn, fund_id):
    cur = conn.cursor()
    res = cur.execute(f"SELECT backward FROM funds WHERE fund_id='{fund_id}'")
    return res.fetchone()[0]


def db_create():
    conn = sqlite3.connect("data.db")
    cursor = conn.cursor()
    with open("schema.sql", "r") as file:
        schema = file.read()
    cursor.executescript(schema)
    conn.commit()
    conn.close()


def db_get_fundlist(conn):
    df_funds = pd_read_sql_cached("SELECT * from funds", conn, parse_dates=["inception"])
    return df_funds


def db_get_cat_list(conn, amc_id=""):
    qry = "SELECT DISTINCT categories.* FROM categories INNER JOIN funds ON categories.cat_id = funds.cat_id"

    if amc_id != "" and amc_id != "0":
        qry = qry + f" WHERE funds.amc_id='{amc_id}'"

    df = pd_read_sql_cached(qry, conn)
    return df


def db_get_amc_list(conn, all=False):
    qry = "SELECT DISTINCT amcs.* FROM amcs INNER JOIN funds ON amcs.amc_id = funds.amc_id"

    if all:
        qry = "SELECT * from amcs"

    df = pd_read_sql_cached(qry, conn)
    return df


def db_update_fundlist(conn, rebuild=False):
    fund_list = mufap_funds_list()
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
        cur = conn.cursor()
        cur.execute(f"drop table if exists funds")
        cur.execute(f"drop table if exists amcs")
        cur.execute(f"drop table if exists categories")

        fund_list.to_sql("funds", conn, if_exists="append", index=False)

        amc_list = mufap_amc_list()
        amc_list.to_sql("amcs", conn, if_exists="append", index=False)

        cat_list = mufap_category_list()
        cat_list.to_sql("categories", conn, if_exists="append", index=False)
    else:
        df_funds = db_get_fundlist(conn)
        fund_list["exists"] = fund_list["fund_id"].isin(df_funds["fund_id"])
        df_new = fund_list[fund_list["exists"] == False].drop(columns="exists")
        if not df_new.empty:
            df_new.to_sql("funds", conn, if_exists="append", index=False)


def db_daily_update(conn, start_date):
    df_funds = db_get_fundlist(conn)

    # l = ['36','28','35','29','32','31','30','34','33']
    # df_funds = df_funds[df_funds['mufap_tab'] == 'vps']

    cur = conn.cursor()

    n = df_funds.shape[0]
    i = 1

    cur.execute(f"DELETE FROM navs WHERE nav_date>='{start_date}'")
    print("Getting NAVs...")
    for idx, row in df_funds.iterrows():
        print(f"Updating {i} / {n}", end="\r")
        i = i + 1
        db_fill_navs(conn, start_date, row["fund_id"], row["mufap_tab"])
        # time.sleep(6)

    i = 1
    cur.execute(f"DELETE FROM payouts WHERE payout_date>='{start_date}'")
    print("\nGetting Payouts...")
    for idx, row in df_funds.iterrows():
        print(f"Updating {i} / {n}", end="\r")
        i = i + 1
        if row["mufap_tab"] != "vps":
            db_fill_payouts(conn, start_date, row["fund_id"], row["cat_id"])

    print("\nDone.")


def db_fill_navs(conn, start_date, fund_id, mufap_tab):
    df = mufap_fund_navs(start_date, fund_id, mufap_tab=mufap_tab)

    if df.shape[0] > 0:
        df.columns = ["fund_id", "nav", "nav_date"]
        df["fund_id"] = fund_id

        df["backward"] = df["fund_id"].apply(lambda x: db_get_fund_backward(conn, x))

        df["nav_date"] = df.apply(
            lambda x: x["nav_date"] - timedelta(days=1)
            if x["backward"]
            else x["nav_date"],
            axis=1,
        )
        del df["backward"]
        df = df[df["nav_date"] >= start_date]

        df.to_sql("navs", conn, if_exists="append", index=False)


def db_fill_payouts(conn, start_date, fund_id, fund_cat):
    df = mufap_fund_payouts(start_date, fund_id, fund_cat)
    if df.shape[0] > 0:
        df.columns = ["fund_id", "payout", "exnav", "payout_date"]
        backward = db_get_fund_backward(conn, fund_id)
        if backward == 1:
            df["payout_date"] = df["payout_date"] - timedelta(days=1)
            df = df[df["payout_date"] >= start_date]

        df["fund_id"] = fund_id
        df.to_sql("payouts", conn, if_exists="append", index=False)



def db_compute_return(conn, op_date, end_date):
    df_navs_op = pd_read_sql_cached(f"SELECT * from navs WHERE nav_date='{op_date}'", conn)
    del df_navs_op["nav_date"]
    df_navs_cl = pd_read_sql_cached(f"SELECT * from navs WHERE nav_date='{end_date}'", conn)
    del df_navs_cl["nav_date"]

    if not df_navs_cl.empty:
        df = df_navs_op.merge(df_navs_cl, on="fund_id", how="inner")
        df = df[(df["nav_x"] != 0) & (df["nav_y"] != 0)]

        df_payouts = pd.read_sql(
            f"SELECT * FROM payouts WHERE payout_date>'{op_date}' AND payout_date<='{end_date}'",
            conn,
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



def __annualize_return(ret, delta_days):
    r = 0
    if delta_days > 365:
        r = (ret + 1) ** (365 / delta_days) - 1
    else:
        r = ret * 365 / delta_days
    return r

def db_annualize_retun(df, ret_col, delta_days):
    df["annualize"] = df["category"].apply(mufap_cat_annualize)

    df[ret_col] = df.apply(
        lambda x: __annualize_return(x[ret_col], delta_days)
        if x["annualize"]
        else x[ret_col],
        axis=1,
    )
    return df
    
def db_get_performance(conn, op_date, end_date):
    df_funds = db_get_fundlist(conn)
    del df_funds["inception"]

    df = db_compute_return(conn, op_date, end_date)

    if df.empty:
        return df

    df_cats = db_get_cat_list(conn)
    df = pd.merge(df_funds, df, on="fund_id", how="left")
    df = pd.merge(df, df_cats, on="cat_id", how="left")
    
    delta_days = (end_date - op_date).days
    df = db_annualize_retun(df, "return", delta_days)
    df["return"] = round(df["return"] * 100, 2)

    return df

def db_get_performance1(conn, op_date, end_date):
    df_funds = db_get_fundlist(conn)
    del df_funds["inception"]

    df_navs_op = pd.read_sql(f"SELECT * from navs WHERE nav_date='{op_date}'", conn)
    del df_navs_op["nav_date"]
    df_navs_cl = pd.read_sql(f"SELECT * from navs WHERE nav_date='{end_date}'", conn)
    del df_navs_cl["nav_date"]

    df_navs = df_navs_op.merge(df_navs_cl, on="fund_id", how="inner")
    df_navs = df_navs[(df_navs["nav_x"] != 0) & (df_navs["nav_y"] != 0)]

    df_payouts = pd.read_sql(
        f"SELECT * FROM payouts WHERE payout_date>'{op_date}' AND payout_date<='{end_date}'",
        conn,
    )
    df_payouts["div_factor"] = (
        df_payouts["exnav"] + df_payouts["payout"]
    ) / df_payouts["exnav"]
    df_payouts = df_payouts[["fund_id", "div_factor"]]
    df_payouts = df_payouts.groupby("fund_id").prod()

    df = pd.merge(df_funds, df_navs, on="fund_id")
    df = pd.merge(df, df_payouts, on="fund_id", how="left")
    df["div_factor"] = df["div_factor"].fillna(1)
    df["return"] = df["nav_y"] / df["nav_x"] * df["div_factor"] - 1

    df_cats = db_get_cat_list(conn)
    df = pd.merge(df, df_cats, on="cat_id", how="left")

    df["annualize"] = df["category"].apply(mufap_cat_annualize)

    delta_days = (end_date - op_date).days
    df["return"] = df.apply(
        lambda x: __annualize_return(x["return"], delta_days)
        if x["annualize"]
        else x["return"],
        axis=1,
    )
    df["return"] = round(df["return"] * 100, 2)

    return df

def db_get_performance_multi(conn, dates_list):
    df_funds = db_get_fundlist(conn)
    del df_funds["inception"]

    df_cats = db_get_cat_list(conn)
    df_funds = df_funds.merge(df_cats, on="cat_id", how="left")

    for title, op_date, end_date in dates_list:
        df = db_compute_return(conn, op_date, end_date)
        df = df.rename(columns={'return': title})
        df_funds = df_funds.merge(df[["fund_id", title]], on="fund_id", how="left")
        delta_days = (end_date - op_date).days
        df_funds = db_annualize_retun(df_funds, title, delta_days)
        df_funds[title] = round(df_funds[title] * 100, 2)

    return df_funds


if __name__ == "__main__":
    # db_create()
    conn = db_connect()

    # db_update_fundlist(conn)
    # db_update_fundlist(conn, True)
    db_daily_update(conn, datetime(2023, 7, 15))

    end_date = datetime(2023,7,27)
    start_date = datetime(2023,7,28)
    dates_list = [
        ("MTD", datetime(end_date.year, end_date.month, 1) - timedelta(1), end_date),
        ("YTD", datetime(end_date.year - (1 if end_date.month <= 6 else 0), 6, 30), end_date),
        ("1 Day", end_date - timedelta(1), end_date),
        ("15 Days", end_date - timedelta(15), end_date),
        ("30 Days", end_date - timedelta(30), end_date),
        ("90 Days", end_date - timedelta(90), end_date),
        # ("160 Days", end_date - timedelta(160), end_date),
        # ("270 Days", end_date - timedelta(270), end_date),
        ("365 Days", end_date - timedelta(365), end_date),
    ]
    # df = db_get_performance_multi(conn, dates_list)
    # df = db_get_performance(conn, start_date, end_date)
    # df.to_excel('df.xlsx')

    db_close(conn)

    
