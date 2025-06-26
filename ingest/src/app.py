from flask import Flask, request, abort
import cygnss_ingest
import smap_ingest
import os

app = Flask(__name__)

@app.route('/ingest')
def ingest():  # put application's code here
    data_type = request.args.get('datatype').upper()
    datestr = request.args.get('date')
    if data_type == 'SMAP':
        smap_ingest.ingest(datestr)
        return 'Start to ingest SMAP data', 200
    elif data_type == 'CYGNSS':
        cygnss_ingest.ingest(datestr)
        return 'Start to ingest CYGNSS data', 200
    else:
        abort(400, f'Invalid datatype: {data_type}')

if __name__ == '__main__':
    # testme.test()
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
