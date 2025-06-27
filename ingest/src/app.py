from flask import Flask, request, abort
from concurrent.futures import ThreadPoolExecutor
import logging
import sys
import cygnss_ingest
import smap_ingest
import os

app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,  # or DEBUG
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,   # Important! Cloud Run reads from stdout
)

executor = ThreadPoolExecutor()

@app.route('/ingest')
def ingest():  # put application's code here
    data_type = request.args.get('datatype').upper()
    datestr = request.args.get('date')
    if data_type == 'SMAP':
        executor.submit(smap_ingest.ingest, datestr)
        return 'Start to ingest SMAP data', 202
    elif data_type == 'CYGNSS':
        executor.submit(cygnss_ingest.ingest, datestr)
        return 'Start to ingest CYGNSS data', 202
    else:
        abort(400, f'Invalid datatype: {data_type}')

@app.route('/')
def health():
    return 'OK', 200


@app.route('/env')
def showenv():
    str = f"EARTHDATA_USERNAME: {os.environ['EARTHDATA_USERNAME']}"
    return str, 200



if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
