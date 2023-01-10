import datetime
from dateutil import tz, parser
import snap7 as s7
from snap7.util import *
from snap7.snap7types import *
import time
import threading
from wolf.mapconfig import WCSVMap, WCSVType

locks = {}

class snap7():

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'host', 'type': 'string', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 102, 'required': True},
            {'name': 'slot', 'type': 'int', 'required': True},
            {'name': 'rack', 'type': 'int', 'required': True},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
        self.mapping = WCSVMap().load(self.csvmap, WCSVType.Raw)

        self.lam = [('%IB', 'PE', 1), ('%IW', 'PE', 2), ('%ID', 'PE', 4), ('%I', 'PE', 1),
        ('%QB', 'PA', 1), ('%QW', 'PA', 2), ('%QD', 'PA', 4), ('%Q', 'PA', 1), 
        ('%MB', 'MK', 1), ('%MW', 'MK', 2), ('%MD', 'MK', 4), ('%M', 'MK', 1), 
        ('%DB', 'DB', 0),
        ('DBB', 'DB', 1), ('DBW', 'DB', 2), ('DBD', 'DB', 4), ('DBX', 'DB', 1)] 

        self.__plc_validate()
        cache.store_meta(self.deviceid, self.name, self.description, self.mapping)

        try:
            self.plc = s7.client.Client()
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

    def __plc_validate(self):
        for row in self.mapping.copy():
            (name, descr, unit, datatype, rw, scale, offset, s7type) = row
            if not (s7type in ('BOOL', 'INT', 'REAL', 'DWORD') or s7type.startswith('STRING')):
                logger.warning('Plugin %s invalid data type %s for %s' % (self.name, s7type, name))
                self.mapping.remove(row)
            req = name
            try:
                (dev, area, count, start) = self.__get_params(req)
            except ValueError:
                pass
            if start and name.startswith('%DB'):
                req = name.replace('%%DB%d.' % start, '')
            if not self.__get_params(req):
                logger.warning('Plugin %s invalid format or address %s' % (self.name, name))
                self.mapping.remove(row)

    def __get_params(self, req):
        try:
            res = list(filter(lambda item: req.startswith(item[0]), self.lam))
            dev = res[0][0]
            start = int(req.split('.')[0].replace(dev, ''))
            return (dev, res[0][1], res[0][2], start)
        except (IndexError, ValueError):
            return ()

    def __plc_read(self, req, s7type):
        try:
            (dev, area, count, start) = self.__get_params(req)
            db = 0
            if dev.startswith('%DB'):
                db = start
                (dev, area, count, start) = self.__get_params(req.replace('%%DB%d.' % start, ''))
            data = self.plc.read_area(areas[area], db, start, count)
            if s7type == 'BOOL':
                bit = int(req.split('.')[-1])
                value = get_bool(data, 0, bit)
                logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
                return value 
            if s7type == 'INT':
                value =  get_int(data, 0)
                logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
                return value 
            if s7type == 'REAL':
                value =  get_real(data, 0)
                logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
                return value 
            if s7type == 'DWORD':
                value = get_dword(data, 0)
                logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
                return value 
            if s7type.startswith('STRING'):
                size = re.search('\d+', s7type).group(0)
                size = int(size)
                value = get_string(data, 0, size)
                logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
                return value 
        except Exception as e:
            logger.error('Cannot read %s from PLC %s rack %d slot %d %s' % (req, self.host, self.rack, self.slot, str(e)))
            return None

    def __plc_write(self, req, s7type, value):
        try:
            (dev, area, count, start) = self.__get_params(req)
            db = 0
            if dev.startswith('%DB'):
                db = start
                (dev, area, count, start) = self.__get_params(req.replace('%%DB%d.' % start, ''))
            data = self.plc.read_area(areas[area], db, start, count)
            if s7type == 'BOOL':
                bit = int(req.split('.')[-1])
                set_bool(data, 0, bit, value)
            if s7type == 'INT':
                set_int(data, 0, value)
            if s7type == 'REAL':
                set_real(data, 0, value)
            if s7type == 'DWORD':
                set_dword(data, 0, value)
            if s7type.startswith('STRING'):
                size = re.search('\d+', s7type).group(0)
                size = int(size)
                set_string(data, 0, value, size)
            self.plc.write_area(areas[area], db, start, data)
            logger.debug('PLC %s rack %d slot %d %s: %s' % (self.host, self.rack, self.slot, req, value))
        except Exception as e:
            logger.error('Cannot write %s to PLC %s rack %d slot %d %s' % (req, self.host, self.rack, self.slot, str(e)))
            return False

    def poll(self):
        self.__lacquire()
        try:
            self.plc.connect(self.host, self.rack, self.slot, self.port)
        except s7.snap7exceptions.Snap7Exception as e:
            logger.error('Cannot connect PLC %s rack %d slot %d %s' % (self.host, self.rack, self.slot, str(e)))
            self.__lrelease()
            return None

        if str(self.plc.get_cpu_state()) != "S7CpuStatusRun":
            logger.error('Cannot poll PLC %s rack %d slot %d: not running!' % (self.host, self.rack, self.slot))
            self.__lrelease()
            return None

        measures = {}
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, s7type) = row
            scale = float(scale)
            offset = float(offset)
            value = self.__plc_read(name, s7type)
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
            self.plc.connect(self.host, self.rack, self.slot, self.port)
        except s7.snap7exceptions.Snap7Exception as e:
            logger.error('Cannot connect PLC %s rack %d slot %d %s' % (self.host, self.rack, self.slot, str(e)))
            self.__lrelease()
            return False
        row = list(filter(lambda r: r[0] == name, self.mapping))
        if len(row):
            (name, descr, unit, datatype, rw, scale, offset, s7type) = row[0]
            if not rw:
                logger.error('Error writing to PLC %s rack %d slot %d %s: read only' % (self.host, self.rack, self.slot, name))
                self.plc.disconnect()
                self.__lrelease()
                return False
            value = self.__plc_write(name, s7type, value)
        self.plc.disconnect()
        self.__lrelease()
        return True
