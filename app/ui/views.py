import json
import os
from pathlib import Path
from datetime import datetime

import ui.instr.data_formatter as dt_form
from flask import Blueprint, request, render_template
from ui.instr.storage import WeatherStorage


pages = Blueprint("pages", __name__, template_folder="templates")


DB_CONFIG = Path("./configs/db_config.ini")
DOWNLOAD_CONFIG = Path("./configs/download.ini")
BASE_DATE = datetime(2019, 1, 1)


@pages.route("/", methods=["POST", "GET"])
@pages.route("/home", methods=["POST", "GET"])
def get_weather_page():
    weather_storage = WeatherStorage(DB_CONFIG)

    if request.method == "POST":
        query_data = dt_form.form_query_data(request.form)
    elif request.method == "GET":
        query_data = dt_form.form_query_data()

    out_statistic = weather_storage.get_weather_statistic(query_data)
    post_var = dt_form.form_post_var(query_data)
    return render_template(
        "index.html", title="Home", result=out_statistic, post_var=post_var
    )
