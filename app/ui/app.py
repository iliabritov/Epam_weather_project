from flask import Flask
from ui.views import pages, BASE_DATE, DOWNLOAD_CONFIG, DB_CONFIG
from ui.instr.db_updater import DataUpdater


app = Flask(__name__)
app.register_blueprint(pages)


@app.before_first_request
def update_data_for_service():
    updater = DataUpdater(BASE_DATE, DOWNLOAD_CONFIG, DB_CONFIG)
    updater.update_db()


if __name__ == "__main__":
    app.run(debug=True)
