from datetime import datetime
from dateutil import tz
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout

class influxdb():

    def __init__(self, name):
        self.name = name

    def __point(self, ts, client_id, device_id, measure_id, val):
        #<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
        point = 'measures,client_id=%s,device_id=%s,measure_id=%s value=%f %d' % (client_id, device_id, measure_id, val, ts)
        return point

    def post(self, data):
        client_id = data.get('client_id')
        device_id = data.get('device_id')
        points = []
        for measure_id in data.get('measures'):
            try:
                meta = cache.load_meta(client_id = client_id, device_id = device_id, measure_id = measure_id)
                measure_type = meta[0].get('measure_type')
                if not measure_type in ('b', 'B', 'h', 'H', 'i', 'I', 'q', 'Q', 'f', 'd', 'c'):
                    continue
                point = self.__point(data.get('ts'), client_id, device_id, measure_id, float(data.get('measures').get(measure_id)))
                points.append(point)
            except IndexError:
                pass
        return influx.write_points(points, protocol='line', time_precision='s')
