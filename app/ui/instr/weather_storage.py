from statistics import mean
from pathlib import Path
from datetime import datetime
from functools import reduce
from collections import Counter
from sqlalchemy import func
from ui.instr.db_driver import DbDriver, WeatherDate
from ui.instr.data_formatter import CsvReformator



"""
def time_stat(func):
	def wrapper(*args, **kwargs):
		start = datetime.now()
		result = func(*args, **kwargs)
		end = datetime.now()
		print(f"TOTAL TIME - {end - start}")
		return result
	return wrapper
"""


class WeatherStorage:
	def __init__(self, cfg_file, cfg_name):
		self.db_driver = DbDriver(cfg_file, cfg_name)


class WeatherUploader(WeatherStorage):
	def __init__(self, cfg_file, cfg_name):
		super().__init__(cfg_file, cfg_name)
		self.db_driver.make_query(WeatherDate)

	def make_weatherdate_obj(self, csv_data):
		return WeatherDate(*csv_data)

	def upload_csv_data(self, data_folder):
		for city_folder in data_folder.iterdir():
			for raw_csv_file in city_folder.iterdir():
				print(raw_csv_file)
				reformator = CsvReformator(raw_csv_file, city_folder.name)
				for clean_line in reformator.clean_lines():
					self.db_driver.add(self.make_weatherdate_obj(clean_line))


class WeatherStatistics(WeatherStorage):
	def __init__(self, cfg_file, cfg_name):
		super().__init__(cfg_file, cfg_name)

	def _get_raw_data(self, city, start_date, end_date):
		self.db_driver.make_query(WeatherDate)
		self.basic_conditions = [
			WeatherDate.city == city,
			WeatherDate.date_day >= start_date,
			WeatherDate.date_day <= end_date
		]
		raw_lines = [set(self.db_driver.query.filter(condition).all())
					for condition in self.basic_conditions]
		self.raw_data = reduce(lambda acc, x: acc & x, raw_lines)

	def temp_statistic(self):
		temps_values = [elem.temp for elem in self.raw_data]
		temp_stat = {
			"max_temp": str(max(temps_values)),
			"min_temp": str(min(temps_values)),
			"avg_temp": str(round(mean(temps_values), 2))
		}
		return temp_stat

	def prec_statistic(self):
		prec_values = [elem.prec_mm for elem in self.raw_data]
		prec_stat = {
			"prec": len([val for val in prec_values if val > 0]),
			"no_prec": len([val for val in prec_values if val == 0])
		}
		for stat in prec_stat:
			prec_stat[stat] = str(prec_stat[stat] / len(prec_values) * 100)

		prec_descr_count = Counter([elem.prec_desc 
			for elem in self.raw_data if elem.prec_desc != " "])
		prec_stat["frec_type_prec"] = " AND ".join(list(prec_descr_count.keys())[:2])
		return prec_stat

	def wind_statistic(self):
		wind_values = [elem.wind_speed for elem in self.raw_data]
		wind_directs = Counter([elem.wind_direc 
			for elem in self.raw_data if elem.prec_desc != " "])
		wind_stat = {
			"wind_speed": str(round(mean(wind_values), 2)),
			"wind_direction": " AND ".join(list(wind_directs.keys())[:2])
		}
		return wind_stat

	def get_weather_statistic(self, city, start_date, end_date):
		self._get_raw_data(city, start_date, end_date)
		full_statistic = {
			**self.temp_statistic(),
			**self.prec_statistic(),
			**self.wind_statistic()
		}
		return full_statistic