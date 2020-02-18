import csv
import json
import psycopg2
import os
from configparser import ConfigParser
from pathlib import Path
from datetime import datetime
from ui.instr.data_formatter import CsvReformator


class StorageDriver:
    def __init__(self, config_file, cfg_name):
        self.config_name = cfg_name
        self.config = self.read_config(config_file)
        self._connect = psycopg2.connect(
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
        )
        self._cursor = self._connect.cursor()

    def __del__(self):
        self._connect.close()

    def read_config(self, path_to_file):
        with path_to_file.open() as config_file:
            parser = ConfigParser()
            parser.read_file(config_file)
            return dict(parser[self.config_name])

    def add_data(self, request_command, data):
        self._cursor.execute(request_command, data)
        self._connect.commit()

    def request(self, request_command, data=None):
        if data:
            self._cursor.execute(request_command, data)
        else:
            self._cursor.execute(request_command)
        return self._cursor.fetchall()


class WeatherStorage:

    add_line = """INSERT INTO weather 
                            (data_base, city,
                            date_day, temp,
                            prec_desc, prec_mm, 
                            wind_speed, wind_direc)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"""
    basic_statistic_com = {
        "max_temp": """SELECT MAX(temp) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
        "average_temp": """SELECT ROUND(AVG(temp)::numeric,2) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
        "min_temp": """SELECT MIN(temp) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
        "prec": """SELECT COUNT(*)
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s 
                                AND city=%s AND prec_mm>0
                        """,
        "no_prec": """SELECT COUNT(*)
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s 
                                AND city=%s AND prec_mm=0""",
        "frec_type_prec": """SELECT prec_desc 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s
                                AND prec_desc!=' '
                        GROUP BY prec_desc 
                        ORDER BY COUNT(*) DESC LIMIT 2""",
        "wind_speed": """SELECT ROUND(AVG(wind_speed)::numeric,2)
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
        "wind_direction": """SELECT wind_direc 
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s AND city=%s
                                AND wind_direc!=''
                        GROUP BY wind_direc 
                        ORDER BY COUNT(*) DESC LIMIT 2""",
    }
    more_2_year_statistic_com = {
        "temp_by_years": """
                                SELECT extract(year FROM date_day), 
                                        MAX(temp), 
                                        MIN(temp)
                                FROM weather 
                                WHERE date_day>=%s AND date_day<=%s AND city=%s
                                GROUP BY extract(year FROM date_day)
                                ORDER BY extract(year FROM date_day)"""
    }

    def __init__(self, config_file):
        self.db_name = "weather_db"
        self.db_driver = StorageDriver(config_file, self.db_name)

    def upload_csv_data_to_database(self, data_folder):
        for city_folder in data_folder.iterdir():
            for raw_csv_file in city_folder.iterdir():
                print(raw_csv_file)
                reformator = CsvReformator(raw_csv_file, city_folder.name)
                for clean_line in reformator.clean_lines():
                    self.db_driver.add_data(self.add_line, clean_line)

    def get_weather_statistic(self, param):
        weather_statistic = {}
        commands = self.basic_statistic_com

        if param[1].year - param[0].year > 2:
            commands.update(self.more_2_year_statistic_com)

        for stat_name in commands:
            weather_statistic[stat_name] = self.db_driver.request(
                commands[stat_name], param
            )
        return self.get_clean_data(weather_statistic)
        # return weather_statistic

    @staticmethod
    def get_clean_data(requests_out):
        result = {}
        for stat in requests_out:
            if stat == "frec_type_prec":
                frec_types = requests_out["frec_type_prec"]
                if len(frec_types):
                    result[stat] = f"{frec_types[0][0]} and {frec_types[1][0]}"
                else:
                    result[stat] = f"Data not found"

            elif stat == "prec" or stat == "no_prec":
                prec = requests_out["prec"][0][0]
                no_prec = requests_out["no_prec"][0][0]
                try:
                    result["prec"] = str(round((prec / (prec + no_prec)) * 100))
                    result["no_prec"] = str(round((no_prec / (prec + no_prec)) * 100))
                except ZeroDivisionError:
                    result["prec"] = str(0)
                    result["no_prec"] = str(0)

            else:
                result[stat] = str(requests_out[stat][0][0])
        return result
