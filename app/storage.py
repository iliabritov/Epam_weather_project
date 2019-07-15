import csv
import json
import psycopg2
import os
from datetime import datetime
import download


class Storage():

    def __init__(self, database_data):
        self._connect = psycopg2.connect(database=database_data['database'],
                                         user=database_data['user'],
                                         password=database_data['password'],
                                         host=database_data['host'])
        self._cursor = self._connect.cursor()

    def __del__(self):
        self._connect.close()

    def upload_json_data_to_database(self, path_to_data_folder):

        for city in os.listdir(path_to_data_folder):
            file_path = os.path.join(path_to_data_folder, city, 'data.json')
            with open(file_path, 'r') as j_file:
                j_data = json.load(j_file)
                for mouth in j_data:
                    for day in j_data[mouth]:
                        sql_data = ('world online weather',
                            city,
                            datetime.strptime(day['date'], '%Y-%m-%d').date(),
                            day['avgtempC'],
                            day['hourly'][0]['weatherDesc'][0]['value'],
                            day['hourly'][0]['precipMM'],
                            day['hourly'][0]['windspeedKmph'],
                            day['hourly'][0]['winddir16Point'])
                        self.upload_data(sql_data)

    def upload_csv_data_to_database(self, path_to_data_folder):

        for city in os.listdir(path_to_data_folder):
            folder = os.path.join(path_to_data_folder, city)
            for year in os.listdir(folder):
                with open(os.path.join(folder, year), 'r') as csv_file:
                    reader = csv.reader(csv_file, delimiter=';')
                    n = 0
                    for row in reader:
                        if n < 9 or n % 8 != 0:
                            n += 1
                            continue
                        n += 1
                        sql_data = ('rp5',
                                    city,
                                    datetime.strptime(
                                    row[0].split(' ')[0], '%d.%m.%Y').date(),
                                    float(row[1]) if len(row[1]) >= 3 else 0,
                                    row[11] if row[11] else 'Нет данных',
                                    float(row[23]) if 7 > len(row[23]) >= 3 else 0,
                                    row[7] if 5 > len(row[7]) >= 3 else 0,
                                    row[6]
                                    )
                        self.upload_data(sql_data)

    def upload_data(self, sql_data):
        self._cursor.execute("""INSERT INTO weather 
                            (data_base, city,
                            date_day, temp,
                            prec_desc, prec_mm, 
                            wind_speed, wind_direc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s);""", sql_data)
        self._connect.commit()

    def update_data(self):
        file_path = os.path.join('..', 'data', 'latest_date.txt')
        with open(file_path, 'r') as date_file:
            latest_date = date_file.read()
        if latest_date == str(datetime.now().date()):
            return None
        print('here')
        save_path = os.path.join('..', 'data', 'update')
        for folder in os.listdir(save_path):
            os.remove(os.path.join(save_path, folder))
        download.get_rp5_weather_datasets(save_path, latest_date,
                                          datetime.now().date())
        download.get_wwo_weather_datasets(save_path, latest_date,
                                          datetime.now().date())
        self.upload_csv_data_to_database(save_path)
        self.upload_json_data_to_database(save_path)

        with open(file_path, 'w') as date_file:
            date_file.write(str(datetime.now().date()))

    def get_client_request(self, client_data):
        client_info = {}
        requests = {'max_temp': """SELECT MAX(temp) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
                    'average_temp': """SELECT ROUND(AVG(temp)::numeric,2) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
                    'min_temp': """SELECT MIN(temp) 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
                    'prec': """SELECT COUNT(*)
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s 
                                AND city=%s AND prec_mm>0
                        """,
                    'no_prec': """SELECT COUNT(*)
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s 
                                AND city=%s AND prec_mm=0""",
                    'frec_type_prec': """SELECT prec_desc 
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s
                                AND prec_desc!=' '
                        GROUP BY prec_desc 
                        ORDER BY COUNT(*) DESC LIMIT 2""",
                    'wind_speed': """SELECT ROUND(AVG(wind_speed)::numeric,2)
                        FROM weather 
                        WHERE date_day>=%s AND date_day<=%s AND city=%s""",
                    'wind_direction': """SELECT wind_direc 
                        FROM weather
                        WHERE date_day>=%s AND date_day<=%s AND city=%s
                                AND wind_direc!=''
                        GROUP BY wind_direc 
                        ORDER BY COUNT(*) DESC LIMIT 2""",
                    }

        if client_data[1].year - client_data[0].year > 2:
            requests.update({'temp_by_years': """
                                SELECT extract(year FROM date_day), 
                                        MAX(temp), 
                                        MIN(temp)
                                FROM weather 
                                WHERE date_day>=%s AND date_day<=%s AND city=%s
                                GROUP BY extract(year FROM date_day)
                                ORDER BY extract(year FROM date_day)"""
                             })

        for param in requests:
            self._cursor.execute(requests[param], client_data)
            client_info[param] = self._cursor.fetchall()

        client_info['max_temp'] = client_info['max_temp'][0][0]
        client_info['average_temp'] = str(client_info['average_temp'][0][0])
        client_info['min_temp'] = client_info['min_temp'][0][0]

        prec = client_info['prec'][0][0]
        no_prec = client_info['no_prec'][0][0]
        client_info['prec'] = round((prec / (prec + no_prec)) * 100)
        client_info['no_prec'] = round((no_prec / (prec + no_prec)) * 100)

        client_info['frec_type_prec'] = (client_info['frec_type_prec'][0][0] +
                                         ' and ' +
                                         client_info['frec_type_prec'][1][0])
        client_info['wind_speed'] = str(client_info['wind_speed'][0][0])
        client_info['wind_direction'] = client_info['wind_direction'][0][0]

        return client_info



if __name__ == '__main__':
    dt_info = {'database': 'dbweather',
               'user': 'postgres',
               'password': '123qwe',
               'host': 'localhost',
               'path': 'data'}
    json_path = os.path.join('..', 'data', 'json')
    csv_path = os.path.join('..', 'data', 'csv')

    db_weather = Storage(dt_info)
    #db_weather.upload_json_data_to_database(json_path)
    #db_weather.upload_csv_data_to_database(csv_path)
    db_weather.update_data()

    test_start_date = datetime(2010, 1, 1).date()
    test_end_date = datetime(2014, 12, 31).date()
    city = 'London'
    data = test_start_date, test_end_date, city
    info = db_weather.get_client_request(data)
    for elem in info:
        print(elem, '=', info[elem])

