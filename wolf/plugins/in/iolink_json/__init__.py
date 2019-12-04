from bitstring import BitArray
from dateutil import tz, parser
from wolf.mapconfig import WCSVMap, WCSVType
import math
import time
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout, ReadTimeout

class iolink_json():

    def __init__(self):
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = None)
        self.url = config.get(self.name, 'url')
        self.timeout = config.getint(self.name, 'timeout', fallback = 3)
        self.cid = config.getint(self.name, 'cid', fallback = -1)
        self.xport = config.getint(self.name, 'xport')
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)
        self.__client = requests.session()

    def poll(self):
        post = {}
        post['cid'] = self.cid
        post['code'] = 10
        post['adr'] = '/iolinkmaster/port[%d]/iolinkdevice/pdin/getdata' % self.xport
        try:
            resp = self.__client.post(self.url, timeout=self.timeout, json=post)
            logger.debug('POST %s %s %d %s' % (self.url, post, resp.status_code, resp.json()))
        except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
            logger.error (str(e))
            return False
        except TypeError as e:
            # workaround for urllib3 Retry() bug
            logger.error (str(e.__context__))
            return False
        if resp.status_code // 100 != 2:
            logger.error("Error reading iolink device %s iolink port %d status code %d" % (self.url, self.xport, response.status_code))
            return False
        json = resp.json()
        barr = BitArray(hex=json['data']['value'])
        ut = time.time()
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, bits) = row
            scale = float(scale)
            offset = float(offset)
            bits = int(bits)
            # int8, int16, int32
            if datatype in ['b', 'h', 'i']:
                decoded = barr[-bits:].int
            # uint8, uint16, uint32
            if datatype in ['B', 'H', 'I']:
                decoded = barr[-bits:].uint
            # float (ieee754)
            elif datatype == 'f':
                decoded = barr[-bits:].float
            # void (reserved bits)
            elif datatype == 'v':
                del barr[-bits:]
                continue
            del barr[-bits:]
            if math.isnan(decoded):
                logger.error("Error reading iolink device %s iolink port %d %s" % (self.url, self.xport, name))
                return None
            value = decoded * scale + offset
            measures[name] = value
            logger.debug('IoLink device %s iolink port %d %s %s %s' % (self.url, self.xport, name, value, unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
