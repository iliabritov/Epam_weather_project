import json
import os
from datetime import datetime
from flask import Blueprint, request, render_template


pages = Blueprint("pages", __name__, template_folder="templates")


"""
@app.before_first_request
def loading_data():
    with open('db_config.json', 'r') as info:
        dt_info = json.load(info)
    print('Server downloading data to database...')
    json_path = os.path.join('..', 'data', 'json')
    db_weather = storage.Storage(dt_info)
    db_weather.upload_json_data_to_database(json_path)
    print('Downloading complite!')
"""


@pages.route("/")
@pages.route("/home")
@pages.route("/index", methods=["POST", "GET"])
def get_weather_page():
    post_var = {
        "city_name": "Saint-Pet.",
        "start_data": "10.10.2015",
        "end_data": "10.10.2018",
    }
    test_result = {
        "min_temp": "",
        "average_temp": "",
        "max_temp": "",
        "no_prec": "",
        "prec": "",
        "frec_type_prec": "",
        "wind_speed": "",
        "wind_direction": "",
        "temp_by_years": None,
    }
    return render_template(
        "index.html", title="Home", result=test_result, post_var=post_var
    )
