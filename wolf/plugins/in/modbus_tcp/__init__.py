from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient, ModbusUdpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ConnectionException
from pymodbus.payload import BinaryPayloadDecoder
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

    def __init__(self):
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = None)
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
        types = {'b': 1, 'B': 1, 'h': 1, 'H': 1, 'i': 2, 'I': 2, 'q': 4, 'Q': 4, 'f': 2, 'd': 4, 's': 0}
        measures = {}
        client = self.mbclient(host=self.host, port=self.port, retries=self.retries, backoff=self.backoff, timeout=self.timeout, framer=self.mbframer, retry_on_empty=True)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
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
                logger.error("Error reading bridge %s slave %d modbus address %d: %s" % (self.host, self.slaveid, addr, result))
                client.close()
                self.__lrelease()
                return None
            if type(result) == ExceptionResponse:
                logger.error("Error reading bridge %s slave %d modbus address %d: %s" % (self.host, self.slaveid, addr, result))
                client.close()
                return None
            if result.isError():
                logger.error("Unrecoverable error reading bridge %s slave %d modbus address %d" % (self.host, self.slaveid, addr))
                client.close()
                return None
            if hasattr(result, 'bits'):
                measures[name] = result.bits[0]
                logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s' % (self.host, self.slaveid, register, name, result.bits[0]))
                continue
            try:
                decoder = BinaryPayloadDecoder.fromRegisters(result.registers, byteorder=Endian.Big, wordorder=self.endian)
                string = re.match( r'^s(\d*)$', datatype)
                if string:
                    decoded = decoder.decode_string(string.group(1))
                    measures[name] = decoded
                    logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s' % (self.host, self.slaveid, register, name, decoded))
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
                logger.error("Unrecoverable error reading bridge %s slave %d modbus address %d" % (self.host, self.slaveid, addr))
                client.close()
                return None
            if math.isnan(decoded):
                logger.error("Unrecoverable error reading modbus device %s slave %d modbus address %d" % (self.port, self.slaveid, addr))
                client.close()
                return None
            value = decoded * scale + offset
            measures[name] = value
            logger.debug('Modbus bridge: %s slave: %s register: %s (%s) value: %s %s' % (self.host, self.slaveid, register, name, value, unit))
        client.close()
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
