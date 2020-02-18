import shutil
from pathlib import Path
from datetime import datetime, timedelta
from ui.instr.scraper import LoaderRp5
from ui.instr.storage import WeatherStorage, StorageDriver


class DataUpdater:

	max_date = """SELECT MAX(date_day) FROM weather"""
	check_empty = """SELECT COUNT(*) FROM weather"""

	def __init__(self, base_date, download_cfg, db_cfg):
		self.base_date = base_date # min date for db 
		self.download_cfg = download_cfg
		self.db_cfg = db_cfg
		self.curr_date = curr_date = datetime.now().date()
		self.max_db_date = self.get_max_db_date(self.db_cfg)

	def get_max_db_date(self, db_cfg):
		storage = StorageDriver(db_cfg, "weather_db")
		db_date = storage.request(self.max_date)[0][0]
		return db_date

	def update_db(self):
		loader = LoaderRp5(self.download_cfg, self.start_date, self.curr_date)
		loader.load_weather_datasets()

		weather_storage = WeatherStorage(self.db_cfg)
		weather_data_source = loader.out_folder / loader.data_folder
		weather_storage.upload_csv_data_to_database(weather_data_source)

		shutil.rmtree(str(loader.out_folder))

	def fetch_db_dates(self):
		if self.max_db_date:
			if self.max_db_date < self.curr_date:
				self.start_date = self.max_db_date + timedelta(days=1)
		else:
			self.start_date = self.base_date


if __name__ == "__main__":
	start_date = datetime(2019, 10, 13).date()
	download_config = Path("../configs/download.ini")
	db_config = Path("../configs/db_config.ini")

	prep = DataPreparer(start_date, download_config, db_config)

	prep.fetch_db_dates()
	prep.update_db()