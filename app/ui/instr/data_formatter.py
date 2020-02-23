import csv
import json
from datetime import datetime, timedelta


class CsvReformator:
    def __init__(self, path_to_csv_file, city):
        self.path = str(path_to_csv_file)
        self.city = city

    def reformat_data(self, raw_line):
        data = (
            "rp5",
            self.city,
            datetime.strptime(raw_line[0].split(" ")[0], "%d.%m.%Y").date(),
            float(raw_line[1]) if len(raw_line[1]) >= 3 else 0,
            raw_line[11] if raw_line[11] else "No data",
            float(raw_line[23]) if 7 > len(raw_line[23]) >= 3 else 0,
            raw_line[7] if 5 > len(raw_line[7]) >= 3 else 0,
            raw_line[6],
        )
        return data

    def clean_lines(self):
        with open(self.path, "r", encoding="utf-8") as dataset:
            reader = csv.reader(dataset, delimiter=";")
            n = 1
            for row in reader:
                if (n < 8):
                    n += 1
                    continue
                yield self.reformat_data(row)


def str_to_datetime(input_date):
    return datetime.strptime(input_date, "%d.%m.%Y")


def datetime_to_str(input_date):
    return input_date.strftime("%d.%m.%Y")


def form_post_var(input_data):
    post_var = {
        "city_name": input_data[0],
        "start_date": datetime_to_str(input_data[1]),
        "end_date": datetime_to_str(input_data[2]),
    }
    return post_var


def form_query_data(input_data=None):
    if input_data:
        query_data = [
            input_data["city_name"],
            str_to_datetime(input_data["start_date"]),
            str_to_datetime(input_data["end_date"]),
        ]
    else:
        query_data = ["Irkutsk", datetime.now().date() - timedelta(days=7), datetime.now().date()]
    return query_data
