import calendar
import gzip
import os
import json
import requests

from io import BytesIO
from datetime import datetime, timedelta


def load_input_data(path, cities_path, file_ext, url_path=None,
                        headers_path=None, form_data_path=None):
    """This function prepare collect data for requests"""

    loading_data = {}
    loading_data['data_path'] = os.path.join(path, file_ext)

    with open(os.path.join('..', 'data', cities_path), 'r') as data:
        loading_data['cities'] = json.load(data)

    if url_path:
        with open(os.path.join('..', 'data', url_path), 'r') as data:
            urls = json.load(data)
            for url in urls:
                loading_data[url] = urls[url]

    if headers_path:
        with open(os.path.join('..', 'data', headers_path), 'r') as data:
            loading_data['headers'] = json.load(data)

    if form_data_path:
        with open(os.path.join('..', 'data', form_data_path), 'r') as data:
            loading_data['form_data'] = json.load(data)

    return loading_data


def load_rp5_weather_datasets(input_data, start_date, end_date):
    with requests.session() as s:
        # get our cookies
        g = s.get(input_data['enter_url'])

        # check csv directory
        if not os.path.exists(input_data['data_path']):
            os.mkdir(input_data['data_path'])

        # download data from rp5.ru
        for city in input_data['cities']:
            input_data['form_data']['wmo_id'] = input_data['cities'][city]
            save_path = os.path.join(input_data['data_path'], city)

            if not os.path.exists(save_path):
                os.mkdir(save_path)

            for year in range(start_date.year, end_date.year + 1):
                a_date1 = str(start_date).split('-')
                a_date2 = str(end_date).split('-')
                input_data['form_data']['a_date1'] = f'{a_date1[2]}.{a_date1[1]}.{str(year)}'
                input_data['form_data']['a_date2'] = f'{a_date2[2]}.{a_date2[1]}.{str(year)}'
                gz_dataset = load_rp5_dataset(s,
                                        input_data['request_url'],
                                        input_data['form_data'],
                                        input_data['headers'])

                file_path = os.path.join(save_path, f'{str(year)}.csv')
                save_rp5_dataset(gz_dataset, file_path)


def load_rp5_dataset(session, url, form_data, headers):
    """ Function allow download and save rp5 dataset by dates """

    r = session.post(url, data=form_data, headers=headers).text
    link = r[r.find('href=') + 5:r.find('>Скачать')].replace('../', '')

    gz_file = gzip.GzipFile(fileobj=BytesIO(requests.get(link).content))

    return gz_file


def save_rp5_dataset(dataset, save_path):
    """ Function read .gz file and save .csv file"""

    with open(save_path, 'wb') as csv_f:
        csv_f.write(dataset.read())


def get_rp5_weather_datasets(data_folder_name, start_date, end_date):
    form_data_file = 'form_data_rp5.json'
    headers_file = 'headers_rp5.json'
    urls_file = 'urls_rp5.json'
    cities_file = 'cities.json'
    input_data = load_input_data(data_folder_name, cities_file,
                                 'csv', form_data_path=form_data_file,
                                 headers_path=headers_file,
                                 url_path=urls_file)
    load_rp5_weather_datasets(input_data, start_date, end_date)


def load_wwo_weather_datasets(input_data, start_date, end_date):

    if not os.path.exists(input_data['data_path']):
        os.mkdir(input_data['data_path'])

    for city in input_data['cities']:
        input_data['form_data']['q'] = city
        local_start_date = start_date
        date_diff = calendar.monthrange(start_date.year, start_date.month)[1]
        local_end_date = start_date + timedelta(days=(date_diff - 1))
        dataset = []
        save_path = os.path.join(input_data['data_path'], city)

        if not os.path.exists(save_path):
            os.mkdir(save_path)

        while True:
            if local_start_date > end_date:
                break
            if local_end_date > end_date:
                local_end_date = end_date

            input_data['form_data']['date'] = str(local_start_date)
            input_data['form_data']['enddate'] = str(local_end_date)

            url = input_data['request_url'] + '&'.join([key + '=' +
            input_data['form_data'][key] for key in input_data['form_data']])

            dataset.append(requests.get(url).text)

            local_start_date += timedelta(days=date_diff)
            date_diff = calendar.monthrange(local_start_date.year,
                                            local_start_date.month)[1]
            local_end_date += timedelta(days=date_diff)

        save_wwo_dataset(dataset, save_path)


def save_wwo_dataset(dataset, save_path):
    """saving dataset to json file"""
    json_data = {}
    with open(os.path.join(save_path, f'data.json'), 'w') as json_file:
        for set in dataset:
            r = json.loads(set)
            json_data[r['data']['weather'][0]['date']] = r['data']['weather']
        json_file.write(json.dumps(json_data))


def get_wwo_weather_datasets(data_folder_name, start_date, end_date):
    form_data_file = 'form_data_wwo.json'
    urls_file = 'urls_wwo.json'
    cities_file = 'cities.json'
    input_data = load_input_data(data_folder_name, cities_file,
                                 'json', form_data_path=form_data_file,
                                 url_path=urls_file)
    load_wwo_weather_datasets(input_data, start_date, end_date)


if __name__ == '__main__':
    start_dt = datetime(2010, 1, 1).date()
    end_dt = datetime.now().date()
    #get_wwo_weather_datasets('data', start_dt, end_dt)
    #get_rp5_weather_datasets('data', start_dt, end_dt)
    print('Done!')