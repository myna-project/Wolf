import bitstring
from dateutil import tz, parser
from wolf.mapconfig import WCSVMap, WCSVType
import math
import time
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout, ReadTimeout

class iolink_json():

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
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
            logger.error("Error reading IO-Link device %s port %d status code %d" % (self.url, self.xport, response.status_code))
            return False
        json = resp.json()
        barr = bitstring.BitArray(hex=json['data']['value'])
        ut = time.time()
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, bitoffset, bitlenght) = row
            scale = float(scale)
            offset = float(offset)
            bitoffset = int(bitoffset)
            bitlenght = int(bitlenght)
            try:
                # bool
                if datatype == 'c':
                    value = barr[bitoffset]
                # int8, int16, int32
                if datatype in ['b', 'h', 'i']:
                    value = barr[bitoffset:bitoffset+bitlenght].int
                # uint8, uint16, uint32
                if datatype in ['B', 'H', 'I']:
                    value = barr[bitoffset:bitoffset+bitlenght].uint
                # float (ieee754)
                elif datatype == 'f':
                    value = barr[bitoffset:bitoffset+bitlenght].float
            except bitstring.InterpretError:
                logger.error("Error reading IO-Link device %s port %d %s" % (self.url, self.xport, name))
                return None
            if math.isnan(value):
                logger.error("Error reading IO-Link device %s port %d %s" % (self.url, self.xport, name))
                return None
            if datatype == 'c':
                measures[name] = value
            else:
                measures[name] = round(value * scale, 8) + offset
            logger.debug('IO-Link device %s port %d %s %s %s' % (self.url, self.xport, measures[name], unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
