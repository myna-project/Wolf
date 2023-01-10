#!/usr/bin/env python3
from bottle import auth_basic, request, response, route, hook, static_file, run
from datetime import datetime
from dateutil import tz, parser
import json
import wolf

groupTypes = {'AVG': 'MEAN', 'MIN': 'MIN', 'MAX': 'MAX', 'STDDEV': 'STDDEV', 'SUM': 'SUM'}
groupTimes = {'MINUTE': '1m', 'QHOUR': '15m', 'HOUR': '1h', 'DAY': '1d', 'WEEK': '1w', 'MONTH': '1m', 'YEAR': '1y'}
prefix = 'wolf'

def __rfc3339_check(text):
    try:
        dt = parser.parse(text)
        return (dt.isoformat())
    except ValueError:
        return ''

def __std_measures(data):
    measures = data.get('measures')
    data['measures'] = [{'measure_id': x, 'value': measures[x]} for x in measures]

@route('/%s/status' % prefix, method='GET')
@wolf.webconfig.content_json
def wolf_status():
    return json.dumps({'status': 'OK'})

@route('/%s/clients' % prefix, method='GET')
@route('/%s/clients/<client_id>' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def wolf_clients(client_id = None):
    clients = wolf.cache.clients(client_id = client_id)
    return json.dumps(clients)

@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/measures' % prefix, method='PUT')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def wolf_write(client_id, device_id, measure_id):
    if not client_id or not device_id or not measure_id:
        response.status = 400
        return '{}'
    try:
        data = request.json
    except:
        response.status = 400
        return
    if data is None or not 'value' in data:
        response.status = 404
        return
    data['client_id'] = client_id
    data['device_id'] = device_id
    data['measure_id'] = measure_id
    wolf.app.queue.put(data)
    return data

@route('/%s/clients/<client_id>/devices/<device_id>/drains/last' % prefix, method='GET')
@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/last' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def wolf_read(client_id, device_id, measure_id = None):
    if not client_id or not device_id:
        response.status = 400
        return '{}'
#    data = {'client_id': client_id, 'device_id': device_id, 'measure_id': measure_id}
#    wolf.app.queue.put(data)
    data = wolf.cache.load_last(client_id, device_id)
    if not data:
        return '{}'
    if measure_id:
        value = data['measures'].get(measure_id)
        data['measures'] = {}
        data['measures'][measure_id] = value
    __std_measures(data)
    return json.dumps(data)

@route('/%s/clients/<client_id>/devices/<device_id>/drains' % prefix, method='GET')
@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def wolf_drains(client_id, device_id, measure_id = None):
    if not client_id or not device_id:
        response.status = 400
        return '{}'
    data = wolf.cache.load_meta(client_id = client_id, device_id = device_id, measure_id = measure_id)
    if not data:
        response.status = 404
        return '{}'
    if measure_id:
        data = data[0]
    return json.dumps(data)

@route('/%s/clients/<client_id>/devices' % prefix, method='GET')
@route('/%s/clients/<client_id>/devices/<device_id>' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def wolf_devices(client_id, device_id = None):
    data = wolf.cache.load_meta(client_id = client_id, device_id = device_id)
    devices = {(item.get('client_id'), item.get('device_id')): {'client_id': item.get('client_id'), 'device_id': item.get('device_id'), 'device_descr': item.get('device_descr')} for item in data}
    values = list(devices.values())
    if device_id:
        values = values[0]
    return json.dumps(values)

@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/measures/group' % prefix, method='GET')
@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/measures/group/<csv>' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def measures_group(client_id, device_id, measure_id, csv=None):
    aggr_time = request.query.timeaggregation
    aggr_type = request.query.measureaggregation
    start = request.query.start
    end = request.query.end
    fill = request.query.fill.lower()
    if not client_id or not device_id or not measure_id:
        response.status = 400
        return '{}'
    if fill not in ['linear', 'none', 'null', 'previous']:
        response.status = 400
        return '{}'
    try:
        aggr_type = groupTypes[aggr_type.upper()]
    except KeyError:
        response.status = 400
        return '{}'
    try:
        aggr_time = groupTimes[aggr_time.upper()]
    except KeyError:
        response.status = 400
        return '{}'
#    subquery = "SELECT client_id, device_id, measure_id, %s(value) AS value FROM measures WHERE " % aggr_type
    subquery = "SELECT %s(value) AS value FROM measures WHERE " % aggr_type
    if client_id:
        subquery += "client_id='%s' AND " % client_id
    if device_id:
        subquery += "device_id='%s' AND " % device_id
    if measure_id:
        subquery += "measure_id='%s' AND " % measure_id
    if start:
        subquery += "time >= '%s' AND " % __rfc3339_check(start)
    if end:
        subquery += "time < '%s' AND " % __rfc3339_check(end)
    subquery += "TRUE "
    subquery += "GROUP BY time(%s) fill(%s)" % (aggr_time, fill)
    query = "SELECT COUNT(*) FROM (%s)" % subquery
    wolf.logger.debug(query)
    results = wolf.influx.query(query)
    if not len(results):
        return '{}'
    count = int(list(results.get_points(measurement='measures'))[0]['count_value'])
    if (count > 10000):
        response.status = 403
        return '{"error_code": -1, "error_description": "Too many data requested!"}'
    query = subquery
    wolf.logger.debug(query)
    results = wolf.influx.query(query)
    if csv:
        columns = results.raw['series'][0]['columns']
        values = results.raw['series'][0]['values']
        out = ''
        out += '"{0}"'.format('", "'.join(columns))
        out += '\n'
        for value in values:
            out += ','.join(str(v) for v in value)
            out += '\n'
    else:
        out = json.dumps(results.raw['series'][0])
    return out

@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/measures' % prefix, method='GET')
@route('/%s/clients/<client_id>/devices/<device_id>/drains/<measure_id>/measures/<csv>' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def measures(client_id, device_id, measure_id, csv=None):
    start = request.query.start
    end = request.query.end
    if not client_id or not device_id or not measure_id:
        response.status = 400
        return '{}'
#    subquery = "SELECT client_id, device_id, measure_id, value FROM measures WHERE "
    subquery = "SELECT value FROM measures WHERE "
    if client_id:
        subquery += "client_id='%s' AND " % client_id
    if device_id:
        subquery += "device_id='%s' AND " % device_id
    if measure_id:
        subquery += "measure_id='%s' AND " % measure_id
    if start:
        subquery += "time >= '%s' AND " % __rfc3339_check(start)
    if end:
        subquery += "time < '%s' AND " % __rfc3339_check(end)
    subquery += "TRUE "
    query = "SELECT COUNT(*) FROM (%s)" % subquery
    wolf.logger.debug(query)
    results = wolf.influx.query(query)
    if not len(results):
        return '{}'
    count = int(list(results.get_points(measurement='measures'))[0]['count_value'])
    if (count > 10000):
        response.status = 403
        return '{"error_code": -1, "error_description": "Too many data requested!"}'
    query = subquery
    wolf.logger.debug(query)
    results = wolf.influx.query(query)
    if csv:
        columns = results.raw['series'][0]['columns']
        values = results.raw['series'][0]['values']
        out = ''
        out += '"{0}"'.format('", "'.join(columns))
        out += '\n'
        for value in values:
            out += ','.join(str(v) for v in value)
            out += '\n'
    else:
        out = json.dumps(results.raw['series'][0])
    return out

@route('/%s/group/types' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def group_types():
    return json.dumps(list(groupTypes.keys()))

@route('/%s/group/times' % prefix, method='GET')
@auth_basic(wolf.webconfig.auth)
@wolf.webconfig.content_json
def group_times():
    return json.dumps(list(groupTimes.keys()))
