import gzip
import json
import requests
from pathlib import Path
from configparser import ConfigParser
from io import BytesIO


class LoaderRp5:
    def __init__(self, config_file, start_date, end_date):
        self.config_path = Path(config_file)
        self.start_date = start_date
        self.end_date = end_date
        self.config = self.read_config()
        self.cities = json.loads(self.config["cities"])
        self.cookie_url = self.config["cookie_url"]
        self.source_url = self.config["source_url"]
        self.out_folder = Path(self.config["out_folder"])
        self.data_folder = Path(self.config["data_folder"])
        self.headers = json.loads(self.config["headers"])
        self._session = self.make_session()

    def read_config(self):
        with self.config_path.open() as config_file:
            parser = ConfigParser()
            parser.read_file(config_file)
            return dict(parser["download"])

    def make_session(self):
        with requests.session() as session:
            # get cookies for session
            session.get(self.cookie_url)
            return session

    def get_form_data(self, city_name, start_date, end_date):
        form_data = {
            "wmo_id": self.cities[city_name],
            "a_date1": start_date.strftime("%d.%m.%Y"),
            "a_date2": end_date.strftime("%d.%m.%Y"),
            "f_ed3": 2,
            "f_ed4": 2,
            "f_ed5": 17,
            "f_pe": 1,
            "f_pe1": 2,
            "lng_id": 1,
        }
        return form_data

    def download_dataset(self, form_data, file_path):
        r = self._session.post(
            self.source_url, data=form_data, headers=self.headers
        ).text
        link = r[r.find("href=") + 5 : r.find(">Download")].replace("../", "")
        dataset = gzip.GzipFile(fileobj=BytesIO(requests.get(link).content))
        with open(file_path, "wb") as csv_f:
            csv_f.write(dataset.read())

    def load_weather_datasets(self, start_date=None, end_date=None):
        if not self.out_folder.exists():
            self.out_folder.mkdir()

        datasets_folder = self.out_folder / self.data_folder
        if not datasets_folder.exists():
            datasets_folder.mkdir()

        start_date = start_date or self.start_date
        end_date = end_date or self.end_date

        for city_name in self.cities:
            out_city_folder = datasets_folder / Path(city_name)

            if not out_city_folder.exists():
                out_city_folder.mkdir()

            for year in range(start_date.year, end_date.year + 1):
                dataset_out_path = out_city_folder / Path(f"{year}.csv")
                if dataset_out_path.exists():
                    continue
                form_data = self.get_form_data(city_name, start_date, end_date)
                self.download_dataset(form_data, dataset_out_path)
