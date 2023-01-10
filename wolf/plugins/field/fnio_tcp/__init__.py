from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder, BinaryPayloadBuilder
from pymodbus.pdu import ExceptionResponse
import time
import struct

ModbusEndianity = {'little': Endian.Little, 'big': Endian.Big}

class fnio_tcp():

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'host', 'type': 'string', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 502, 'required': True},
            {'name': 'slaveid', 'type': 'int', 'default': 0, 'required': True},
            {'name': 'endianity', 'type': 'enum', 'default': 'little', 'enum': ModbusEndianity, 'required': True},
            {'name': 'timeout', 'type': 'float', 'default': 3, 'required': True},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
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

    def __parse_row(self, row):
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 'c': 1}
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
        measures = {}
        client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return None
        ut = time.time()
        last = None
        for row in sorted(filter(lambda x:x[3] == 'c', self.mapping), key=lambda x:x[7]):
            (name, descr, unit, datatype, rw, scale, offset, address, bit, length) = self.__parse_row(row)
            if address != last:
                try:
                    result = client.read_holding_registers(address, 1, unit=self.slaveid)
                except ConnectionException as e:
                    logger.error("Error reading bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                    client.close()
                    return None
                if isinstance(result, ExceptionResponse):
                    logger.error("Error reading bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, self.excodes[result.exception_code]))
                    client.close()
                    return None
                if result.isError():
                    logger.error("Error reading bridge %s slave %d address %d" % (self.host, self.slaveid, address))
                    client.close()
                    return None
            value = bool(result.registers[0] & (1 << bit))
            measures[name] = value
            logger.debug('Modbus bridge: %s slave: %s address: %d.%d (%s) value: %s' % (self.host, self.slaveid, address, bit, name, value))
            last = address
        for row in filter(lambda x:x[3] != 'c', self.mapping):
            (name, descr, unit, datatype, rw, scale, offset, address, bit, length) = self.__parse_row(row)
            try:
                result = client.read_holding_registers(address, length, unit=self.slaveid)
            except ConnectionException as e:
                logger.error("Error reading bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                client.close()
                return None
            if isinstance(result, ExceptionResponse):
                logger.error("Error reading bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, self.excodes[result.exception_code]))
                client.close()
                return None
            if result.isError():
                logger.error("Error reading bridge %s slave %d address %d" % (self.host, self.slaveid, address))
                client.close()
                return None
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=self.endianity)
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
                logger.error("Error reading bridge %s slave %d address %d" % (self.host, self.slaveid, address))
                client.close()
                return None
            if math.isnan(value) or type(value) == bool:
                logger.error("Error reading bridge %s slave %d address %d" % (self.host, self.slaveid, address))
                client.close()
                return None
            measures[name] = round(value * scale, 14) + offset
            logger.debug('Modbus bridge: %s slave: %s address: %s (%s) value: %s %s' % (self.host, self.slaveid, address, name, measures[name], unit))
        client.close()
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data

    def write(self, name, value):
        client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return False
        row = list(filter(lambda r: r[0] == name, self.mapping))
        if len(row):
            (name, descr, unit, datatype, rw, scale, offset, address, bit) = self.__parse_row(row)
            if not rw:
                logger.error("Error writing to bridge %s slave %d address %d: read only" % (self.host, self.slaveid, address))
                client.close()
                return False
            if datatype == 'c':
                try:
                    result = client.read_holding_registers(address, 1, unit=self.slaveid)
                    r = result.registers[0]
                except ConnectionException as e:
                    logger.error("Error reading bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                    client.close()
                    return None
                r &= ~(1 << bit)
                if value:
                    r |= (1 << bit)
                try:
                    result = client.write_register(address, r, unit=self.slaveid)
                except ConnectionException as e:
                    logger.error("Error writing bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                    client.close()
                    return None
                logger.debug('Modbus bridge: %s slave: %s address: %d.%d (%s) value: %s' % (self.host, self.slaveid, address, bit, name, value))
                client.close()
                return True
            try:
                builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=self.endianity)
                if datatype == 'b':
                    builder.add_8bit_int(value)
                if datatype == 'B':
                    builder.add_8bit_uint(value)
                if datatype == 'h':
                    builder.add_16bit_int(value)
                if datatype == 'H':
                    builder.add_16bit_uint(value)
                if datatype == 'i':
                    builder.add_32bit_int(value)
                if datatype == 'I':
                    builder.add_32bit_uint(value)
                if datatype == 'q':
                    builder.add_64bit_int(value)
                if datatype == 'Q':
                    builder.add_64bit_uint(value)
                if datatype == 'f':
                    builder.add_32bit_float(value)
                if datatype == 'd':
                    builder.add_64bit_float(value)
                registers = builder.to_registers()
            except (AttributeError, ValueError, struct.error) as e:
                logger.error("Error writing to bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                client.close()
                return False
            try:
                if len(registers) > 1:
                    result = client.write_registers(address, registers, unit=self.slaveid)
                else:
                    result = client.write_register(address, value, unit=self.slaveid)
            except ConnectionException as e:
                logger.error("Error writing to bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, str(e)))
                client.close()
                return False
            if isinstance(result, ExceptionResponse):
                logger.error("Error writing to bridge %s slave %d address %d: %s" % (self.host, self.slaveid, address, self.excodes[result.exception_code]))
                client.close()
                return False
            if result.isError():
                logger.error("Error writing to bridge %s slave %d address %d" % (self.host, self.slaveid, address))
                client.close()
                return False
            logger.debug('Modbus bridge: %s slave: %s address: %s (%s) value: %s' % (self.host, self.slaveid, address, name, value))
        client.close()
        return True
