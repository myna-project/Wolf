from datetime import datetime
from dateutil import tz
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout
from builtins import ConnectionRefusedError

class influxdb():

    params = [{'name': 'host', 'type': 'string', 'default': 'localhost', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 8086, 'required': True},
            {'name': 'db', 'type': 'string', 'default': None, 'required': True},
            {'name': 'username', 'type': 'string', 'default': None, 'required': True},
            {'name': 'password', 'type': 'string', 'default': None, 'required': True},
            {'name': 'ssl', 'type': 'boolean', 'default': False},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)

    def __point(self, ts, client_id, device_id, measure_id, val):
        #<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]
        point = 'measures,client_id=%s,device_id=%s,measure_id=%s value=%f %d' % (client_id, device_id, measure_id, val, ts)
        return point

    def post(self, data):
        client_id = data.get('client_id')
        device_id = data.get('device_id')
        points = []
        for measure_id in data.get('measures', []):
            try:
                meta = cache.load_meta(client_id = client_id, device_id = device_id, measure_id = measure_id)
                measure_type = meta[0].get('measure_type')
                if not measure_type in ('b', 'B', 'h', 'H', 'i', 'I', 'q', 'Q', 'f', 'd', 'c'):
                    continue
                point = self.__point(data.get('ts'), client_id, device_id, measure_id, float(data.get('measures').get(measure_id)))
                points.append(point)
            except (IndexError, TypeError):
                pass
        try:
            return influx.write_points(points, protocol='line', time_precision='s')
        except (ConnectionError, ConnectionRefusedError) as e:
            logger.error(str(e))
