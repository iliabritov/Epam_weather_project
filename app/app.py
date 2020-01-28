import json
import os

from datetime import datetime
from flask import Flask, request, render_template

from app.views import pages


app = Flask(__name__)
app.register_blueprint(pages)


@app.route("/")
@app.route("/index")
@app.route("/home")
def get_show_page():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
