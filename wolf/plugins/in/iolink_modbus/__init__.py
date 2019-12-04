from bitstring import BitArray
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

    def __mbread(addr, length):
        result = client.read_holding_registers(addr, length)
        if type(result) == ExceptionResponse:
            logger.error("Error reading bridge %s modbus address %d: %s" % (self.host, addr, result))
            client.close()
            return False
        if result.isError():
            logger.error("Unrecoverable error reading bridge %s modbus address %d" % (self.host, addr))
            client.close()
            return False
        return result

    def poll(self):
        client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout, retry_on_empty=True)
        if not client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return False
        result = client.read_holding_registers(8998, 1)
        length = (result.registers[0] & 0xff)
        result = client.read_holding_registers(8999, 1)
        swap = result.registers[0]
        result = client.read_holding_registers(self.xport * 1000 + 1, length + 1)
        registers = result.registers[-length:]
        registers.reverse()
        bsum = reduce(lambda x, y: (0, x[1] + (y[1] << 16 * y[0])), enumerate(registers))
        barr = BitArray(hex=hex(bsum[1]))
        ut = time.time()
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, bits) = row
            scale = float(scale)
            offset = float(offset)
            bits = int(bits)
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
            del barr[-bits:]
            if math.isnan(decoded):
                logger.error("Error reading iolink device %s iolink port %d %s" % (self.host, self.xport, name))
                return None
            value = decoded * scale + offset
            measures[name] = value
            logger.debug('IoLink device %s iolink port %d %s %s %s' % (self.host, self.xport, name, value, unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data

