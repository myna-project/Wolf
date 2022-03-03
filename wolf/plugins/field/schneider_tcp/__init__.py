from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder, BinaryPayloadBuilder
from pymodbus.pdu import ExceptionResponse
from pymodbus.transaction import ModbusSocketFramer
import time
import struct
import threading

locks = {}

class schneider_tcp():

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.host = config.get(self.name, 'host')
        self.port = config.getint(self.name, 'port', fallback = 502)
        self.slaveid = config.getint(self.name, 'slaveid', fallback = 0)
        self.timeout = config.getfloat(self.name, 'timeout', fallback = 3)
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Modbus)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def __lacquire(self):
        global locks
        if self.host in locks:
            self.lock = locks[self.host]
            logger.debug("Plugin %s resource %s already in use" % (self.name, self.host))
        locks['%s' % (self.host)] = self.lock
        self.lock.acquire()
        logger.debug("Plugin %s lock on %s acquired" % (self.name, self.host))

    def __lrelease(self):
        self.lock.release()
        logger.debug("Plugin %s lock on %s released" % (self.name, self.host))

    def poll(self):
        self.__lacquire()
        client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            self.__lrelease()
            return None
        hr = {}
        r_off = 7
        r_min = int(round(min(self.mapping, key=lambda x:x[r_off])[r_off], -2))
        r_max = int(round(max(self.mapping, key=lambda x:x[r_off])[r_off], -2))
        ut = time.time()
        for r in range(r_min, r_max + 100, 100):
            if list(filter(lambda x:((x[r_off] // 100) * 100) == r, self.mapping)):
                try:
                    result = client.read_holding_registers(r - 1, 100, unit=self.slaveid)
                except ConnectionException as e:
                    logger.error("Error reading bridge %s slave %d registers %d-%d: %s" % (self.host, self.slaveid, r, r + 100, str(e)))
                    client.close()
                    self.__lrelease()
                    return None
                if type(result) == ExceptionResponse:
                    logger.error("Error reading bridge %s slave %d registers %d-%d: %s" % (self.host, self.slaveid, r, r + 100, result))
                    client.close()
                    self.__lrelease()
                    return None
                if result.isError():
                    logger.error("Error reading bridge %s slave %d registers %d-%d" % (self.host, self.slaveid, r, r + 100))
                    client.close()
                    self.__lrelease()
                    return None
                hr.update(dict(enumerate(result.registers, r)))
        client.close()
        self.__lrelease()
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 'c': 1}
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, register) = row
            register = int(register)
            scale = float(scale)
            offset = float(offset)
            length = types.get(datatype)
            endian = Endian.Little
            if length >= 2:
                endian = Endian.Big
            if datatype in ['h', 'H', 'i', 'I'] and hr[register] == 0x8000:
                continue
            try:
                registers = list(dict(filter(lambda x:x[0] in range(register, register + types[datatype]), hr.items())).values())
                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=Endian.Big, wordorder=endian)
                if datatype == 'b':
                    value = decoder.decode_8bit_int()
                if datatype == 'B':
                    value = decoder.decode_8bit_uint()
                if datatype == 'h':
                    value = decoder.decode_16bit_int()
                if datatype == 'H':
                    value = decoder.decode_16bit_uint()
                if datatype == 'i':
                    value = decoder.decode_32bit_int()
                if datatype == 'I':
                    value = decoder.decode_32bit_uint()
                if datatype == 'q':
                    value = decoder.decode_64bit_int()
                if datatype == 'Q':
                    value = decoder.decode_64bit_uint()
                if datatype == 'f':
                    value = decoder.decode_32bit_float()
                if datatype == 'd':
                    value = decoder.decode_64bit_float()
            except (AttributeError, ValueError, struct.error):
                logger.error("Error reading bridge %s slave %d register %d" % (self.host, self.slaveid, register))
                return None
            if math.isnan(value):
                continue
            measures[name] = round(value * scale, 8) + offset
            logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s %s' % (self.host, self.slaveid, register, name, measures[name], unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
