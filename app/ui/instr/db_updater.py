import shutil
from datetime import datetime, timedelta
from ui.instr.scraper import LoaderRp5
from ui.instr.db_driver import WeatherDate
from ui.instr.weather_storage import WeatherUploader


class DataUpdater:
    def __init__(self, base_date, download_cfg, db_cfg):
        self.base_date = base_date  # min date for db
        self.download_cfg = download_cfg
        self.uploader = WeatherUploader(db_cfg, "weather_db")
        self.curr_date = datetime.now().date()

    def get_max_db_date(self):
        db_date = self.uploader.db_driver.query.order_by(
            WeatherDate.date_day.desc()
        ).first()
        if db_date:
            return db_date.date_day

    def update_db(self):
        self.max_db_date = self.get_max_db_date()
        if self.max_db_date != self.curr_date:
            self.fetch_db_dates()
            # download
            loader = LoaderRp5(self.download_cfg, self.start_date, self.curr_date)
            loader.load_weather_datasets()

            weather_data_folder = loader.out_folder / loader.data_folder
            self.uploader.upload_csv_data(weather_data_folder)

            shutil.rmtree(str(loader.out_folder))

    def fetch_db_dates(self):
        if self.max_db_date:
            if self.max_db_date < self.curr_date:
                self.start_date = self.max_db_date + timedelta(days=1)
        else:
            self.start_date = self.base_date
