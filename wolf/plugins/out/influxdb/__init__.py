from datetime import datetime
from dateutil import tz
from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout

class influxdb():

    def __init__(self, name):
        self.name = name
        self.host = config.get(self.name, 'host', fallback = 'localhost')
        self.port = config.getint(self.name, 'port', fallback = '8086')
        self.db = config.get(self.name, 'db', fallback = 'wolf')
        self.username = config.get(self.name, 'username', fallback = 'root')
        self.password = config.get(self.name, 'password', fallback = 'root')
        self.ssl = config.getboolean(self.name, 'ssl', fallback = False)

        self.client = InfluxDBClient(host=self.host, port=self.port, username=self.username, password=self.password, ssl=self.ssl)
        logger.info('Influxdb connection: %s:%d db "%s"' % (self.host, self.port, self.db))
        self.client.switch_database(self.db)

    class __point:
        def __init__(self, ts, client_id, device_id, measure_id, val):
            dt = datetime.fromtimestamp(ts)
            dd = dt.replace(tzinfo=tz.gettz())
            ts = dd.strftime('%Y-%m-%dT%H:%M:%S%z')
            self.point = {
                "measurement": "measures",
                "time": ts,
                "fields": {
                    "value": float(val)
                },
                "tags": {
                    "client_id": client_id,
                    "device_id": device_id,
                    "measure_id": measure_id
                },
            }

    def post(self, data):
        points = []
        for measure in data['measures']:
            point = self.__point(data['ts'], data['client_id'], data['device_id'], measure, data['measures'][measure])
            points.append(point.__dict__['point'])
        try:
            self.client.write_points(points)
        except (ConnectionError, InfluxDBClientError) as e:
            logger.error('Influxdb host %s:%d cannot write: %s' % (self.host, self.port, str(e)))
            return False
        return True
