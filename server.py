from flask import Flask, jsonify, render_template, request, redirect
from consumer import DataReader
import os


app = Flask(__name__, static_folder = os.getcwd())
print(os.getcwd())

dr = DataReader()
# print("here")
# @app.route('/data_get')
def get_data():
    data = dr.read_data()
    return jsonify(data).json

@app.route('/', methods = ['POST', 'GET'])
def index():
    json_data=get_data()
    return render_template('index.html', data=json_data)

@app.route('/info_1', methods = ['POST', 'GET'])
def second():
    return render_template('info_1.html', data=get_data())


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000)
