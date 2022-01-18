
import os
import json
import glob
from flask import Flask
from flask import jsonify
from flask import request
from flask import make_response
from datetime import datetime, timedelta
from dateutil.parser import parse
import numpy as np
import pandas as pd

########## configuration options ##########
# these are set in environment variables
# so you can update the server code without 
# having to re-set config options
HAPI_HOME = os.environ.get('HAPI_HOME')
HOST_NAME = os.environ.get('HAPI_HOST_NAME')
PORT_NUMBER = os.environ.get('HAPI_PORT_NUMBER')
SERVER_VERSION = os.environ.get('HAPI_SERVER_VERSION')
SERVER_ID = os.environ.get('HAPI_SERVER_ID')
SERVER_TITLE = os.environ.get('HAPI_SERVER_TITLE')
SERVER_CONTACT = os.environ.get('HAPI_SERVER_CONTACT')
SERVER_DESC = os.environ.get('HAPI_SERVER_DESC')
SERVER_CONTACTID = os.environ.get('HAPI_SERVER_CONTACTID')
SERVER_CITATION = os.environ.get('HAPI_SERVER_CITATION')
SERVER_DEBUG_MODE = os.environ.get('HAPI_SERVER_DEBUG')
###########################################

app = Flask(__name__)

# Cross-Origin Resource Sharing
@app.after_request
def after_request(response):
    header = response.headers
    header['Access-Control-Allow-Origin'] = '*'
    header['Access-Control-Allow-Methods'] = 'GET'
    header['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

def load_catalog():
    # load the catalog data
    json_file = open(os.path.join(HAPI_HOME, 'catalog.json'), 'r')
    items = json.load(json_file)
    json_file.close()
    return items['catalog']

def check_dataset(dataset):
    valid = True
    output = {"HAPI": SERVER_VERSION,
              "status": {"code": 1200, "message": "OK"}}

    if dataset is None:
        output = {"HAPI": SERVER_VERSION,
                  "status": {"code": 1400, "message": "Bad request - missing required parameter id"}}
        valid = False

    catalog = load_catalog()
    catalog_ids = [item['id'] for item in catalog]

    if dataset not in catalog_ids:
        output = {"HAPI": SERVER_VERSION,
                  "status": {"code": 1406, "message": "Bad request - unknown dataset id"}}
        valid = False

    return (valid, output)

########## /about ##########
@app.route('/hapi/about', methods=['GET'])
def about():
    output = {
              "HAPI": SERVER_VERSION,
              "status": {"code": 1200, "message": "OK"},
              "id": SERVER_ID,
              "title": SERVER_TITLE,
              "contact": SERVER_CONTACT
             }
    
    if SERVER_DESC is not None:
        output['description'] = SERVER_DESC

    if SERVER_CONTACTID is not None:
        output['contactID'] = SERVER_CONTACTID

    if SERVER_CITATION is not None:
        output['citation'] = SERVER_CITATION

    return jsonify(output)

########## /capabilities ##########
@app.route('/hapi/capabilities', methods=['GET'])
def capabilities():
    output = {"HAPI": SERVER_VERSION,
              "status": {"code": 1200, "message": "OK"},
              "outputFormats": ["csv"]}
    return jsonify(output)

########## /catalog ##########
@app.route('/hapi/catalog', methods=['GET'])
def catalog():
    items = load_catalog()
    output = {"HAPI": SERVER_VERSION,
              "status": {"code": 1200, "message": "OK"},
              "catalog": items}
    return jsonify(output)

########## /info ##########
@app.route('/hapi/info', methods=['GET'])
def info():
    dataset = request.args.get('dataset')
    parameters = request.args.get('parameters')
 
    if dataset is None:
        dataset = request.args.get('id')

    valid, output = check_dataset(dataset)

    if not valid:
        return jsonify(output)

    try:
        info_file = open(os.path.join(HAPI_HOME, 'info', dataset + '.json'), 'r')
        info_data = json.load(info_file)
        info_file.close()
    except:
        output = {"HAPI": SERVER_VERSION,
                  "status": {"code": 1500, "message": "Internal server error"}}
        return jsonify(output)

    return jsonify(dict(info_data, **output))

########## /data ##########
@app.route('/hapi/data', methods=['GET'])
def data():
    dataset = request.args.get('dataset')
    parameters = request.args.get('parameters')
    start = request.args.get('start')
    stop = request.args.get('stop')
    include = request.args.get('include')
 
    if dataset is None:
        dataset = request.args.get('id')
        
    valid, output = check_dataset(dataset)

    if not valid:
        return jsonify(output)

    if include is not None:
        if include == 'header':
            info_file = open(os.path.join(HAPI_HOME, 'info', dataset + '.json'), 'r')
            info_data = json.load(info_file)
            info_file.close()

    if start is None:
        start = request.args.get('time.min')

    if stop is None:
        stop = request.args.get('time.max')

    start_time = parse(start, fuzzy=True)
    stop_time = parse(stop, fuzzy=True)

    delta = stop_time-start_time
    data_out = []

    data_files = glob.glob(os.path.join(HAPI_HOME, 'data', dataset, '*', dataset + '.*.csv'))
    dateparse = lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')

    for day in range(0, delta.days+1):
        current_day = start_time + timedelta(days=day)
        data_file = os.path.join(HAPI_HOME, 'data', dataset, current_day.strftime('%Y'), dataset + '.' + current_day.strftime('%Y%m%d') + '.csv')

        if data_file not in data_files:
            continue

        try:
            full_file = pd.read_csv(data_file, index_col=None, header=0, parse_dates=[0], date_parser=dateparse, delimiter=',')
        except ValueError:
            continue

        data_out.append(full_file.to_numpy())

    if len(data_out) == 0:
        output = {"HAPI": SERVER_VERSION,
                  "status": {"code": 1405, "message": "Bad request - time outside valid range"}}
        return jsonify(output)

    data_out = np.concatenate(data_out)

    times = data_out[:, 0]

    idxs_in_range = np.argwhere((times >= np.datetime64(start_time)) & (times < np.datetime64(stop_time))).squeeze()
    data_in_trange = data_out[idxs_in_range]

    df = pd.DataFrame(data_in_trange)
    output = df.to_csv(date_format='%Y-%m-%dT%H:%M:%SZ', index=False, header=False)

    out_header = ''
    if include is not None:
        if include == 'header':
            out_header = '#' + json.dumps(info_data, indent=4).replace('\n', '\n#') + '\n'

    out = make_response(out_header + output)
    out.headers["Content-type"] = "text/csv"

    return out

if __name__ == '__main__':
    if SERVER_DEBUG_MODE is not None:
        app.run(host=HOST_NAME, port=PORT_NUMBER, debug=True)
    else:
        app.run(host=HOST_NAME, port=PORT_NUMBER)