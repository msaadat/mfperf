from flask import Flask, render_template, request, json
from db import *
from time import sleep
from datetime import datetime, timedelta


class CustomFlask(Flask):
    jinja_options = Flask.jinja_options.copy()
    jinja_options.update(
        dict(
            variable_start_string="%%",  # Default is '{{', I'm changing this because Vue.js uses '{{' / '}}'
            variable_end_string="%%",
        )
    )


app = CustomFlask(__name__, static_folder="static")
config = {"DEBUG": True}
app.config.from_mapping(config)


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/performance", methods=["POST"])
def performance():
    data = request.get_json()
    end_date = datetime.fromisoformat(data["end_date"])

    if data["period_type"] == "mufap":
        df = get_performance_mufap(end_date, data["cat_ids"], data["amc_id"])
    else:
        start_date = datetime.fromisoformat(data["start_date"])
        df = get_performance_custom(start_date, end_date, data["cat_ids"], data["amc_id"])

    if not df.empty:
        html = df.to_html(
            index=False, table_id="performance", classes="table table-stripped"
        )
    else:
        html = "No data"

    return html

@app.route("/update", methods=["GET"])
def update():
    conn = db_connect()
    # db_update_fundlist(conn)
    # db_daily_update(conn, datetime(2023, 7, 15))
    db_close(conn)

@app.route("/get_data", methods=["POST"])
def get_data():
    conn = db_connect()
    data = ""

    requested = request.get_json()
    if requested["data"] == "categories":
        amc_id = requested["amc_id"]
        df = db_get_cat_list(conn, amc_id)
        data = df.to_json(orient="records")
    if requested["data"] == "amcs":
        df = db_get_amc_list(conn)
        data = df.to_json(orient="records")

    db_close(conn)
    return data


def get_performance_custom(start_date, end_date, cat_ids=[], amc_id=""):
    conn = db_connect()
    df = db_get_performance(conn, start_date, end_date)
    db_close(conn)

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

    conn = db_connect()
    df = db_get_performance_multi(conn, dates_list)
    db_close(conn)

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

    

    return df


if __name__ == "__main__":
    app.run("localhost", 8000)
