from flask import Flask, render_template, request, json
from time import sleep
from datetime import datetime

from db_mufap import MFDatabase
import perf


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
        df = perf.get_performance_mufap(end_date, data["cat_ids"], data["amc_id"])
    else:
        start_date = datetime.fromisoformat(data["start_date"])
        df = perf.get_performance_custom(start_date, end_date, data["cat_ids"], data["amc_id"])

    if not df.empty:
        html = df.to_html(
            index=False, table_id="performance", classes="table table-stripped"
        )
    else:
        html = "No data"

    return html

@app.route("/get_data", methods=["POST"])
def get_data():
    data = ""
    requested = request.get_json()

    with MFDatabase() as db:
        if requested["data"] == "categories":
            amc_id = requested["amc_id"]
            df = db.get_cat_list(amc_id)
            data = df.to_json(orient="records")
        if requested["data"] == "amcs":
            df = db.get_amc_list()
            data = df.to_json(orient="records")
        if requested["data"] == "latest_date":
            dt = db.get_latest_nav_date()
            data = f'{{"latest_date": "{dt}"}}'

    return data

@app.route("/merge", methods=["POST"])
def merge():
    db = MFDatabase()
    data = request.get_json()

    if db.path_db_attach.exists():
        db.merge_attached(datetime.fromisoformat(data["cutoff_date"]))
        db.path_db_attach.unlink()

        return "Success"
    else:
        return "Nothing to do"

if __name__ == "__main__":
    app.run("localhost", 8000)
