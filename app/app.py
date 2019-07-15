import json
from datetime import datetime
from flask import Flask, request, render_template
import storage


app = Flask(__name__)
app.config.from_object('config')


@app.route('/')
@app.route('/index', methods=['POST', 'GET'])
def main():
    with open('db_config.json', 'r') as config:
        db_param = json.load(config)
    db_weather = storage.Storage(db_param)

    if request.method == 'POST':
        post_var = request.form

        start_data = post_var['start_data'].split('.')
        start_data = datetime(int(start_data[2]), int(start_data[1]),
                              int(start_data[0])).date()

        end_data = post_var['end_data'].split('.')
        end_data = datetime(int(end_data[2]), int(end_data[1]),
                            int(end_data[0])).date()

        client_param = start_data, end_data, post_var['city_name']
        respond = db_weather.get_client_request(client_param)

        return render_template('index.html', title='Home',
                               result=respond, post_var=post_var)
    elif request.method == 'GET':
        post_var = [('city_name', 'Saint-Pet.'),
                    ('start_data', 'today1'),
                    ('end_data', 'today2')]
        test_result = {
            'min_temp': '',
            'average_temp': '',
            'max_temp': '',
            'no_prec': '',
            'prec': '',
            'frec_type_prec': '',
            'wind_speed': '',
            'wind_direction': '',
            'temp_by_years': None
        }
        db_weather.update_data()
        return render_template('index.html', title='Home',
                               result=test_result, post_var=post_var)


if __name__ == '__main__':
    app.run()
