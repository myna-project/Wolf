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

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'host', 'type': 'string', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 502, 'required': True},
            {'name': 'slaveid', 'type': 'int', 'default': 0, 'required': True},
            {'name': 'timeout', 'type': 'float', 'default': 3, 'required': True},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
        self.mapping = WCSVMap().load(self.csvmap, WCSVType.Modbus)
        cache.store_meta(self.deviceid, self.name, self.description, self.mapping)
        self.excodes = {0x00: "Success", 0x01: "Illegal function code", 0x02: "Illegal data address", 0x03: "Illegal data value", 
                        0x04: "Server device failure", 0x05: "Acknowledge", 0x06: "Server device busy", 0x07: "Negative acknowledge",
                        0x08: "Memory parity error", 0x0A: "Gateway path unavailable", 0x0B: "Gateway target not responding",
                        0xE0: "Timeout", 0xE1: "Invalid server", 0xE2: "CRC check error", 0xE3: "Function code mismatch", 
                        0xE4: "Server ID mismatch", 0xE5: "Packet length error", 0xE6: "Wrong # of parameters", 0xE7: "Parameter out of bounds",
                        0xE8: "Request queue full", 0xE9: "Illegal IP or port", 0xEA: "IP connection failed", 0xEB: "TCP header mismatch",
                        0xEC: "Incomplete request", 0xED: "Invalid ASCII frame", 0xEE: "Invalid ASCII CRC", 0xEF: "Invalid ASCII character"}

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

    def __parse_row(self, row):
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 's': 0, 'c': 1}
        (name, descr, unit, datatype, rw, scale, offset, *address) = row
        bit = 0
        if len(address) > 1:
            bit = int(address[1])
        address = int(address[0])
        rw = bool(rw)
        scale = float(scale)
        offset = float(offset)
        length = types.get(datatype)
        return (name, descr, unit, datatype, rw, scale, offset, address, bit, length)

    def poll(self):
        self.__lacquire()
        client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            self.__lrelease()
            return None
        hr = {}
        r_off = 7
        r_num = 100
        r_min = int(round(min(self.mapping, key=lambda x:x[r_off])[r_off], -2))
        r_max = int(round(max(self.mapping, key=lambda x:x[r_off])[r_off], -2))
        ut = time.time()
        for r in range(r_min, r_max + r_num, r_num):
            if list(filter(lambda x:((x[r_off] // r_num) * r_num) == r, self.mapping)):
                try:
                    result = client.read_holding_registers(r - 1, r_num, unit=self.slaveid)
                except ConnectionException as e:
                    logger.error("Error reading bridge %s slave %d registers %d-%d: %s" % (self.host, self.slaveid, r, r + r_num, str(e)))
                    client.close()
                    self.__lrelease()
                    return None
                if isinstance(result, ExceptionResponse):
                    logger.error("Error reading bridge %s slave %d registers %d-%d: %s" % (self.host, self.slaveid, r, r + r_num, self.excodes[result.exception_code]))
                    client.close()
                    self.__lrelease()
                    return None
                if result.isError():
                    logger.error("Error reading bridge %s slave %d registers %d-%d" % (self.host, self.slaveid, r, r + r_num))
                    client.close()
                    self.__lrelease()
                    return None
                hr.update(dict(enumerate(result.registers, r)))
        client.close()
        self.__lrelease()
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 'c': 1}
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, register, bit, length) = self.__parse_row(row)
            endianity = Endian.Little
            if length >= 2:
                endianity = Endian.Big
            if datatype in ['h', 'H', 'i', 'I'] and hr[register] == 0x8000:
                continue
            try:
                registers = list(dict(filter(lambda x:x[0] in range(register, register + types[datatype]), hr.items())).values())
                decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=Endian.Big, wordorder=endianity)
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
