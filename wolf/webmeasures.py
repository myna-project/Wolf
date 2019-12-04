#!/usr/bin/env python3
from bottle import request, response, route, hook, static_file, run
from datetime import datetime
from dateutil import tz, parser
from influxdb import InfluxDBClient, resultset
from influxdb.exceptions import InfluxDBClientError
import json
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout
import wolf

groupTypes = {'AVG': 'MEAN', 'MIN': 'MIN', 'MAX': 'MAX', 'STDDEV': 'STDDEV', 'SUM': 'SUM'}
groupTimes = {'MINUTE': '1m', 'QHOUR': '15m', 'HOUR': '1h', 'DAY': '1d', 'WEEK': '1w', 'MONTH': '1m', 'YEAR': '1y'}

class WWebMeasures():

    def __init__(self):

        if 'influxdb' not in wolf.config.sections():
            return
        self.host = wolf.config.get('influxdb', 'host', fallback = 'localhost')
        self.port = wolf.config.getint('influxdb', 'port', fallback = '8086')
        self.db = wolf.config.get('influxdb', 'db', fallback = 'wolf')
        self.username = wolf.config.get('influxdb', 'username', fallback = 'root')
        self.password = wolf.config.get('influxdb', 'password', fallback = 'root')
        self.ssl = wolf.config.getboolean('influxdb', 'ssl', fallback = False)

        self.client = InfluxDBClient(host=self.host, port=self.port, username=self.username, password=self.password, ssl=self.ssl)
        wolf.logger.info('Influxdb connection: %s:%d db "%s"' % (self.host, self.port, self.db))
        self.client.switch_database(self.db)

    def query(self, query):
        try:
            results = self.client.query(query)
        except (ConnectionError, InfluxDBClientError) as e:
            wolf.logger.error('Influxdb host %s:%d cannot read: %s' % (self.host, self.port, str(e)))
            return None
        return results

def __json(results):
    data = {}
    if results and isinstance(results, resultset.ResultSet):
        measures = []
        datapoints = list(results.get_points())
        for point in datapoints:
            measures.append({'time': point['time'], 'value': point['value']})
        data['measures'] = measures
    return json.dumps(data)

def __rfc3339_check(text):
    try:
        dt = parser.parse(text)
        return (dt.isoformat())
    except ValueError:
        return ''

@route('/measures/drains', method='GET')
def measures_drains():
    data = cache.load_meta()
    return json.dumps(data)

@route('/measures/csv', method='GET')
def measures_csv():
    return "{}"

@route('/measures', method='GET')
def measures():
    client_id = request.query.client_id
    device_id = request.query.device_id
    measure_id = request.query.measure_id
    start = request.query.start
    end = request.query.end
    query = "SELECT value FROM measures WHERE "
    if client_id:
        query += "client_id='%s' AND " % client_id
    if device_id:
        query += "device_id='%s' AND " % device_id
    if measure_id:
        query += "measure_id='%s' AND " % measure_id
    if start:
        query += "time => '%s' AND " % start
    if end:
        query += "time <= '%s' AND " % end
    if end:
        query += "time <= '%s' AND " % end
    query += "TRUE "
    results = webm.query(query)
    return __json(results)

@route('/measures/group', method='GET')
def measures_group():
    client_id = request.query.client_id
    device_id = request.query.device_id
    measure_id = request.query.measure_id
    aggr_time = request.query.timeaggregation
    aggr_type = request.query.measureaggregation
    start = request.query.start
    end = request.query.end
    try:
        aggr_type = groupTypes[aggr_type]
    except KeyError:
        response.status = 403
        return
    try:
        aggr_time = groupTimes[aggr_time]
    except KeyError:
        response.status = 403
        return
    query = "SELECT %s(value) AS value FROM measures WHERE " % aggr_type
    if client_id:
        query += "client_id='%s' AND " % client_id
    if device_id:
        query += "device_id='%s' AND " % device_id
    if measure_id:
        query += "measure_id='%s' AND " % measure_id
    if start:
        query += "time >= '%s' AND " % __rfc3339_check(start)
    if end:
        query += "time <= '%s' AND " % __rfc3339_check(end)
    query += "TRUE "
    query += "GROUP BY time(%s) fill(linear)" % aggr_time
    results = webm.query(query)
    return __json(results)

@route('/group/types', method='GET')
def group_types():
    return json.dumps(list(groupTypes.keys()))

@route('/group/times', method='GET')
def group_times():
    return json.dumps(list(groupTimes.keys()))

