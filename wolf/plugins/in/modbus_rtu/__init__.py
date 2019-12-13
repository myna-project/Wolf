from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
from pymodbus.constants import Endian, Defaults
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu import ExceptionResponse
import time
import re
import struct
import threading

ModbusProtocols = {'ascii': 'ascii', 'rtu': 'rtu'}

ModbusEndianity = {'little': Endian.Little, 'big': Endian.Big}

locks = {}

class modbus_rtu():

    def __init__(self):
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = None)
        self.port = config.get(self.name, 'port')
        self.slaveid = config.getint(self.name, 'slaveid', fallback = 0)
        self.endian = config.getenum(self.name, 'endianity', enum=ModbusEndianity, fallback = 'little')
        self.stopbits = config.getfloat(self.name, 'stopbits', fallback = Defaults.Stopbits)
        self.bytesize = config.getint(self.name, 'bytesize', fallback = Defaults.Bytesize)
        self.parity = config.get(self.name, 'parity', fallback = Defaults.Parity)
        self.baudrate = config.getint(self.name, 'baudrate', fallback = Defaults.Baudrate)
        self.timeout = config.getint(self.name, 'timeout', fallback = Defaults.Timeout)
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Modbus)
        protocol = config.getenum(self.name, 'protocol', enum=ModbusProtocols, fallback = 'rtu')
        self.method = protocol.name
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def __lacquire(self):
        global locks
        if self.port in locks:
            self.lock = locks[self.port]
            logger.debug("Plugin %s resource %s already in use" % (self.name, self.port))
        locks['%s' % (self.port)] = self.lock
        self.lock.acquire()
        logger.debug("Plugin %s lock on %s acquired" % (self.name, self.port))

    def __lrelease(self):
        self.lock.release()
        logger.debug("Plugin %s lock on %s released" % (self.name, self.port))

    def poll(self):
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 's': 0}
        measures = {}
        self.__lacquire()
        client = ModbusClient(method=self.method, port=self.port, stopbits=self.stopbits, bytesize=self.bytesize, partity=self.parity, baudrate=self.baudrate, timeout=self.timeout, retry_on_empty=True)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.port))
            self.__lrelease()
            return None
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, register) = row
            register = int(register)
            scale = float(scale)
            offset = float(offset)
            try:
                if register > 40000:
                    addr = register - 40001
                    result = client.read_holding_registers(addr, types[datatype], unit=self.slaveid)
                elif register > 30000:
                    addr = register - 30001
                    result = client.read_input_registers(addr, types[datatype], unit=self.slaveid)
                elif register > 10000:
                    addr = register - 10001
                    result = client.read_discrete_inputs(addr, types[datatype], unit=self.slaveid)
                else:
                    addr = register - 1
                    result = client.read_coils(addr, types[datatype], unit=self.slaveid)
            except ConnectionException:
                logger.error("Error reading modbus device %s slave %d address %d: %s" % (self.port, self.slaveid, addr, result))
                client.close()
                self.__lrelease()
                return None
            if type(result) == ExceptionResponse:
                logger.error("Error reading modbus device %s slave %d address %d: %s" % (self.port, self.slaveid, addr, result))
                client.close()
                self.__lrelease()
                return None
            if result.isError():
                logger.error("Unrecoverable error reading modbus device %s slave %d modbus address %d" % (self.port, self.slaveid, addr))
                client.close()
                self.__lrelease()
                return None
            if hasattr(result, 'bits'):
                measures[name] = result.bits[0]
                logger.debug('Modbus device: %s slave: %s register: %s (%s) value: %s' % (self.port, self.slaveid, register, name, result.bits[0]))
                continue
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=self.endian)
                string = re.match( r'^s(\d*)$', datatype)
                if string:
                    decoded = decoder.decode_string(string.group(1))
                    measures[name] = decoded
                    logger.debug('Modbus device: %s slave: %s register: %s (%s) value: %s' % (self.port, self.slaveid, register, name, decoded))
                    continue
                if datatype == 'b':
                    decoded = decoder.decode_8bit_int()
                if datatype == 'B':
                    decoded = decoder.decode_8bit_uint()
                if datatype == 'h':
                    decoded = decoder.decode_16bit_int()
                if datatype == 'H':
                    decoded = decoder.decode_16bit_uint()
                if datatype == 'i':
                    decoded = decoder.decode_32bit_int()
                if datatype == 'I':
                    decoded = decoder.decode_32bit_uint()
                if datatype == 'q':
                    decoded = decoder.decode_64bit_int()
                if datatype == 'Q':
                    decoded = decoder.decode_64bit_uint()
                if datatype == 'f':
                    decoded = decoder.decode_32bit_float()
                if datatype == 'd':
                    decoded = decoder.decode_64bit_float()
            except (AttributeError, ValueError, struct.error):
                logger.error("Unrecoverable error reading modbus device %s slave %d modbus address %d" % (self.port, self.slaveid, addr))
                client.close()
                self.__lrelease()
                return None
            if math.isnan(decoded) or type(decoded) == bool:
                logger.error("Unrecoverable error reading modbus device %s slave %d modbus address %d" % (self.port, self.slaveid, addr))
                client.close()
                self.__lrelease()
                return None
            value = decoded * scale + offset
            measures[name] = value
            logger.debug('Modbus device: %s slave: %s register: %s (%s) value: %s %s' % (self.port, self.slaveid, register, name, value, unit))
        client.close()
        self.__lrelease()
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
