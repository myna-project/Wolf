import bitstring
from dateutil import tz, parser
from functools import reduce
from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.pdu import ExceptionResponse
import time
import struct

class iolink_modbus():

    def __init__(self):
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = None)
        self.host = config.get(self.name, 'host')
        self.port = config.getint(self.name, 'port', fallback = 502)
        self.timeout = config.getfloat(self.name, 'timeout', fallback = 3)
        self.xport = config.getint(self.name, 'xport')
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def __mbread(self, addr, length):
        try:
            result = self.client.read_holding_registers(addr, length)
        except ConnectionException:
            logger.error("Error reading Modbus IO-Link device %s address %d: %s" % (self.host, addr, result))
            self.client.close()
            return False
        if type(result) == ExceptionResponse:
            logger.error("Error reading Modbus IO-Link device %s address %d: %s" % (self.host, addr, result))
            self.client.close()
            return False
        if result.isError():
            logger.error("Unrecoverable error reading Modbus IO-Link device %s address %d" % (self.host, addr))
            self.client.close()
            return False
        return result

    def poll(self):
        self.client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout, retry_on_empty=True, retry_on_invalid=True)
        if not self.client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return False
        if self.xport < 4:
            result = self.__mbread(199, 1)
        else:
            result = self.__mbread(299, 1)
        nc = result.registers[0] & 0xff
        invalid = result.registers[0] >> 8
        if self.xport & nc:
            logger.error("Modbus IO-Link device %s port %d: device not connected" % (self.host, self.xport))
            return None
        if self.xport & invalid:
            logger.error("Modbus IO-Link device %s port %d: data invalid" % (self.host, self.xport))
            return None
        result = self.__mbread(8998, 1)
        length = (result.registers[0] & 0xff)
        result = self.__mbread(8999, 1)
        swap = result.registers[0]
        result = self.__mbread(self.xport * 1000 + 1, length + 1)
        self.client.close()
        registers = result.registers[-length:]
        registers.reverse()
        bsum = reduce(lambda x, y: (0, x[1] + (y[1] << 16 * y[0])), enumerate(registers))
        barr = bitstring.BitArray(hex=hex(bsum[1]))
        ut = time.time()
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, bits) = row
            scale = float(scale)
            offset = float(offset)
            bits = int(bits)
            try:
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
            except bitstring.InterpretError:
                logger.error("Error reading Modbus IO-Link device %s port %d %s" % (self.host, self.xport, name))
                return None
            del barr[-bits:]
            if math.isnan(decoded):
                logger.error("Error reading Modbus IO-Link device %s port %d %s" % (self.host, self.xport, name))
                return None
            value = decoded * scale + offset
            measures[name] = value
            logger.debug('Modbus IO-Link device %s port %d %s %s %s' % (self.host, self.xport, name, value, unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data

