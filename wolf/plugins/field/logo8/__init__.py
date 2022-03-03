from dateutil import tz, parser
import snap7 as s7
from snap7.util import *
import time
import threading
from wolf.mapconfig import WCSVMap, WCSVType

locks = {}

class logo8():

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.host = config.get(self.name, 'host')
        self.port = config.getint(self.name, 'port', fallback = 102)
        self.ltsap = self.__str2tsap(config.get(self.name, 'ltsap'))
        self.ctsap = self.__str2tsap(config.get(self.name, 'ctsap'))
        self.version = config.getenum(self.name, 'version', enum={'logo7': 'logo7', 'logo8': 'logo8'}, fallback='logo8')
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Raw)

        self.lam = [('I', 1024, 8), ('AI', 1032, 32), ('Q', 1064, 8), ('AQ', 1072, 32),
        ('M',  1104, 14), ('AM', 1118, 128), ('NI', 1246, 16), ('NAI', 1262, 128),
        ('NQ', 1390, 16), ('NAQ', 1406, 64), ('VD', 0, 1466), ('VW', 0, 1468), ('V', 0, 1469)]
        if self.version == 'logo7':
            self.lam = [('I', 923, 3), ('AI', 926, 16), ('Q', 942, 2), ('AQ', 944, 4),
            ('M',  948, 4), ('AM', 952, 32), ('VD', 0, 980), ('VW', 0, 982), ('V', 0, 983)]

        self.__plc_validate()
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

        try:
            self.plc = s7.logo.Logo()
        except s7.snap7exceptions.Snap7Exception as e:
            logger.error('Snap7 error: %s' % str(e))
            return None

    def __lacquire(self):
        global locks
        if self.host in locks:
            self.lock = locks[self.host]
            logger.debug("Plugin %s resource %s already in use" % (self.name, self.host))
        locks[self.host] = self.lock
        self.lock.acquire()
        logger.debug("Plugin %s lock on PLC %s acquired" % (self.name, self.host))

    def __lrelease(self):
        self.lock.release()
        logger.debug("Plugin %s lock on PLC %s released" % (self.name, self.host))

    def __str2tsap(self, str):
        return int(str.replace('.', ''), 16)

    def __plc_validate(self):
        for m in self.mapping.copy():
            res = list(filter(lambda item: m[0].startswith(item[0]), self.lam))
            if not res:
                logger.warning('Plugin %s device %s invalid' % (self.name, m[0]))
                self.mapping.remove(m)
                continue
            dev = res[0][0]
            num = int(m[0].split('.')[0].replace(dev, '')) - 1
            if dev in ('I', 'Q', 'M', 'NI', 'NQ'):
                if (num // 8) >= res[0][2]:
                    logger.warning('Plugin %s device %s out of range' % (self.name, m[0]))
                    self.mapping.remove(m)
            if dev in ('AI', 'AQ', 'AM', 'NAI', 'NAQ'):
                if (num << 1) >= res[0][2]:
                    logger.warning('Plugin %s device %s out of range' % (self.name, m[0]))
                    self.mapping.remove(m)
            if dev in ('V', 'VW', 'VD'):
                if (num) >= res[0][2]:
                    logger.warning('Plugin %s device %s out of range' % (self.name, m[0]))
                    self.mapping.remove(m)

    def __plc_read(self, req):
        res = list(filter(lambda item: req.startswith(item[0]), self.lam))
        try:
            dev = res[0][0]
            num = int(req.split('.')[0].replace(dev, '')) - 1
            if dev in ('I', 'Q', 'M', 'NI', 'NQ'):
                return self.plc.read('V%d.%d' % (res[0][1] + num // 8, num % 8))
            if dev in ('AI', 'AQ', 'AM', 'NAI', 'NAQ'):
                return self.plc.read('VW%d' % (res[0][1] + (num << 1)))
            if dev in ('V', 'VW', 'VD'):
                return self.plc.read(req)
        except Exception as e:
            logger.error('Cannot read %s from PLC %s local TSAP 0x%04x' % (req, self.host, self.ltsap))
            return None

    def __plc_write(self, req, value):
        res = list(filter(lambda item: req.startswith(item[0]), self.lam))
        try:
            dev = res[0][0]
            num = int(req.split('.')[0].replace(dev, '')) - 1
            if dev in ('I', 'Q', 'M', 'NI', 'NQ'):
                return not self.plc.write('V%d.%d' % (res[0][1] + num // 8, num % 8), value)
            if dev in ('AI', 'AQ', 'AM', 'NAI', 'NAQ'):
                return not self.plc.write('VW%d' % (res[0][1] + (num << 1)), value)
            if dev in ('V', 'VW', 'VD'):
                return self.plc.write(req, value)
        except Exception as e:
            logger.error('Cannot write %s to PLC %s local TSAP 0x%04x' % (req, self.host, self.ltsap))
            return False

    def poll(self):
        self.__lacquire()
        try:
            self.plc.connect(self.host, self.ctsap, self.ltsap, self.port)
        except s7.snap7exceptions.Snap7Exception as e:
            logger.error('Cannot connect PLC %s local TSAP 0x%04x %s' % (self.host, self.ltsap, str(e)))
            self.__lrelease()
            return None
        measures = {}
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset) = row
            scale = float(scale)
            offset = float(offset)
            value = self.__plc_read(name)
            if value is None:
                continue
            if datatype in ['b', 'B', 'h', 'H', 'i', 'I', 'q', 'Q', 'f', 'd']:
                measures[name] = round(value * scale, 8) + offset
            else:
                measures[name] = value
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        self.plc.disconnect()
        self.__lrelease()
        return data

    def write(self, name, value):
        self.__lacquire()
        try:
            self.plc.connect(self.host, self.ctsap, self.ltsap, self.port)
        except s7.snap7exceptions.Snap7Exception as e:
            logger.error('Cannot connect PLC %s local TSAP 0x%04x %s' % (self.host, self.ltsap, str(e)))
            self.__lrelease()
            return False
        row = list(filter(lambda r: r[0] == name, self.mapping))
        if len(row):
            (name, descr, unit, datatype, rw, scale, offset) = row[0]
            if not rw:
                logger.error('Error writing to PLC %s local TSAP 0x%04x %s: read only' % (self.host, self.ltsap, device))
                self.plc.disconnect()
                self.__lrelease()
                return False
            value = self.__plc_write(name, value)
        self.plc.disconnect()
        self.__lrelease()
        return True
