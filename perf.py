from datetime import datetime, timedelta

from db import MFDatabase

def get_performance_custom(start_date, end_date, cat_ids=[], amc_id=""):
    
    with MFDatabase() as db:
        df = db.get_performance(start_date, end_date)

    if df.empty:
        return df

    if cat_ids != []:
        if not "0" in cat_ids:
            df = df[df["cat_id"].isin(cat_ids)]

    if amc_id != "" and amc_id != "0":
        df = df[df["amc_id"] == amc_id]

    col_list = ["fund_name", "category", "return"]
    col_names = ["Fund", "Category", "Return"]
    df = df[col_list]
    df.columns = col_names
    df = df.dropna()

    return df


def get_performance_mufap(end_date, cat_ids=[], amc_id=""):
    dates_list = [
        (
            "YTD",
            datetime(end_date.year - (1 if end_date.month <= 6 else 0), 6, 30),
            end_date,
        ),
        ("MTD", datetime(end_date.year, end_date.month, 1) - timedelta(1), end_date),
        ("1 Day", end_date - timedelta(1), end_date),
        ("15 Days", end_date - timedelta(15), end_date),
        ("30 Days", end_date - timedelta(30), end_date),
        ("90 Days", end_date - timedelta(90), end_date),
        # ("160 Days", end_date - timedelta(160), end_date),
        # ("270 Days", end_date - timedelta(270), end_date),
        ("365 Days", end_date - timedelta(365), end_date),
    ]

    with MFDatabase() as db:
        df = db.get_performance_multi(dates_list)

    if cat_ids != []:
        if not "0" in cat_ids:
            df = df[df["cat_id"].isin(cat_ids)]

    if amc_id != "" and amc_id != "0":
        df = df[df["amc_id"] == amc_id]

    # df = df.dropna(subset=['return'])
    ret_cols = [x[0] for x in dates_list]
    col_list = ["fund_name", "category"]
    col_list.extend(ret_cols)
    # col_names = ["Fund", "Category", "Return"]
    df = df[col_list]
    df = df.rename(columns={"fund_name": "Fund", "category": "Category"})
    df = df.dropna(subset=ret_cols, how='all')
    df = df.fillna('n/a')

    return df