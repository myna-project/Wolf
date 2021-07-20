from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder, BinaryPayloadBuilder
from pymodbus.pdu import ExceptionResponse
from pymodbus.transaction import ModbusRtuFramer, ModbusAsciiFramer, ModbusBinaryFramer, ModbusSocketFramer
import time
import re
import struct

ModbusProtocols = {'tcp': {'client': ModbusTcpClient, 'framer': ModbusSocketFramer},
                   'udp': {'client': ModbusUdpClient, 'framer': ModbusSocketFramer},
                   'rtutcp': {'client': ModbusTcpClient, 'framer': ModbusRtuFramer}}

ModbusEndianity = {'little': Endian.Little, 'big': Endian.Big}

class modbus_tcp():

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.host = config.get(self.name, 'host')
        self.port = config.getint(self.name, 'port', fallback = 502)
        self.slaveid = config.getint(self.name, 'slaveid', fallback = 0)
        self.endian = config.getenum(self.name, 'endianity', enum=ModbusEndianity, fallback = 'little')
        self.retries = config.getint(self.name, 'retries', fallback = 3)
        self.backoff = config.getfloat(self.name, 'backoff', fallback = 0.3)
        self.timeout = config.getfloat(self.name, 'timeout', fallback = 3)
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Modbus)
        protocol = config.getenum(self.name, 'protocol', enum=ModbusProtocols, fallback = 'tcp')
        self.mbclient = protocol['client']
        self.mbframer = protocol['framer']
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def poll(self):
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 's': 0, 'c': 1}
        measures = {}
        client = self.mbclient(host=self.host, port=self.port, retries=self.retries, backoff=self.backoff, timeout=self.timeout, framer=self.mbframer, retry_on_empty=True, retry_on_invalid=True)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return None
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, register) = row
            register = int(register)
            scale = float(scale)
            offset = float(offset)
            length = types.get(datatype)
            string = re.match(r'^s(\d*)$', datatype)
            if string:
                length = int(string.group(1)) >> 1
            try:
                if register > 40000:
                    addr = register - 40001
                    result = client.read_holding_registers(addr, length, unit=self.slaveid)
                elif register > 30000:
                    addr = register - 30001
                    result = client.read_input_registers(addr, length, unit=self.slaveid)
                elif register > 10000:
                    addr = register - 10001
                    result = client.read_discrete_inputs(addr, length, unit=self.slaveid)
                else:
                    addr = register - 1
                    result = client.read_coils(addr, length, unit=self.slaveid)
            except ConnectionException as e:
                logger.error("Error reading bridge %s slave %d register %d: %s" % (self.host, self.slaveid, register, str(e)))
                client.close()
                return None
            if type(result) == ExceptionResponse:
                logger.error("Error reading bridge %s slave %d register %d: %s" % (self.host, self.slaveid, register, result))
                client.close()
                return None
            if result.isError():
                logger.error("Error reading bridge %s slave %d register %d" % (self.host, self.slaveid, register))
                client.close()
                return None
            if hasattr(result, 'bits'):
                measures[name] = result.bits[0]
                logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s' % (self.host, self.slaveid, register, name, result.bits[0]))
                continue
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=self.endian)
                if string:
                    value = decoder.decode_string(255).decode()
                    measures[name] = value
                    logger.debug('Modbus bridge: %s slave %s register %s (%s) value: %s' % (self.host, self.slaveid, register, name, value))
                    continue
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
                client.close()
                return None
            if math.isnan(value) or type(value) == bool:
                logger.error("Error reading bridge %s slave %d register %d" % (self.host, self.slaveid, register))
                client.close()
                return None
            measures[name] = round(value * scale, 14) + offset
            logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s %s' % (self.host, self.slaveid, register, name, value, unit))
        client.close()
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        print (data)
        return data

    def write(self, name, value):
        client = self.mbclient(host=self.host, port=self.port, retries=self.retries, backoff=self.backoff, timeout=self.timeout, framer=self.mbframer, retry_on_empty=True, retry_on_invalid=True)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return False
        row = list(filter(lambda r: r[0] == name, self.mapping))
        if len(row):
            (name, descr, unit, datatype, rw, scale, offset, register) = row[0]
            if not rw:
                logger.error("Error writing to bridge %s slave %d register %d: read only" % (self.host, self.slaveid, register))
                client.close()
                return False
            register = int(register)
            try:
                builder = BinaryPayloadBuilder(byteorder=Endian.Big, wordorder=self.endian)
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
                if re.match( r'^s(\d*)$', datatype):
                    builder.add_string(value)
                registers = builder.to_registers()
            except (AttributeError, ValueError, struct.error) as e:
                logger.error("Error writing to bridge %s slave %d register %d: %s" % (self.host, self.slaveid, register, str(e)))
                client.close()
                return False
            try:
                if register > 40000:
                    addr = register - 40001
                    if len(registers) > 1:
                        result = client.write_registers(addr, registers, unit=self.slaveid)
                    else:
                        result = client.write_register(addr, value, unit=self.slaveid)
                else:
                    addr = register - 1
                    result = client.write_coil(addr, bool(value), unit=self.slaveid)
            except ConnectionException as e:
                logger.error("Error writing to bridge %s slave %d register %d: %s" % (self.host, self.slaveid, register, str(e)))
                client.close()
                return False
            if type(result) == ExceptionResponse:
                logger.error("Error writing to bridge %s slave %d register %d: %s" % (self.host, self.slaveid, register, result))
                client.close()
                return False
            if result.isError():
                logger.error("Error writing to bridge %s slave %d register %d" % (self.host, self.slaveid, register))
                client.close()
                return False
            logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s' % (self.host, self.slaveid, register, name, value))
        client.close()
        return True
