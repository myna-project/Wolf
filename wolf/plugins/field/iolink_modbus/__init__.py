import bitstring
from dateutil import tz, parser
from functools import reduce
from wolf.mapconfig import WCSVMap, WCSVType
import math
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.pdu import ExceptionResponse
from pymodbus.exceptions import ConnectionException
import time
import struct

class iolink_modbus():

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'host', 'type': 'string', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 502, 'required': True},
            {'name': 'xport', 'type': 'int', 'required': True},
            {'name': 'timeout', 'type': 'float', 'default': 3, 'required': True},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
        self.mapping = WCSVMap().load(self.csvmap, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.description, self.mapping)
        self.excodes = {0x00: "Success", 0x01: "Illegal function code", 0x02: "Illegal data address", 0x03: "Illegal data value", 
                        0x04: "Server device failure", 0x05: "Acknowledge", 0x06: "Server device busy", 0x07: "Negative acknowledge",
                        0x08: "Memory parity error", 0x0A: "Gateway path unavailable", 0x0B: "Gateway target not responding",
                        0xE0: "Timeout", 0xE1: "Invalid server", 0xE2: "CRC check error", 0xE3: "Function code mismatch", 
                        0xE4: "Server ID mismatch", 0xE5: "Packet length error", 0xE6: "Wrong # of parameters", 0xE7: "Parameter out of bounds",
                        0xE8: "Request queue full", 0xE9: "Illegal IP or port", 0xEA: "IP connection failed", 0xEB: "TCP header mismatch",
                        0xEC: "Incomplete request", 0xED: "Invalid ASCII frame", 0xEE: "Invalid ASCII CRC", 0xEF: "Invalid ASCII character"}

    def __mbread(self, addr, length):
        try:
            result = self.client.read_holding_registers(addr, length)
        except ConnectionException:
            logger.error("Error reading Modbus IO-Link device %s address %d" % (self.host, addr))
            self.client.close()
            return False
        if isinstance(result, ExceptionResponse):
            logger.error("Error reading Modbus IO-Link device %s address %d: %s" % (self.host, addr, self.excodes[result.exception_code]))
            self.client.close()
            return False
        if result.isError():
            logger.error("Unrecoverable error reading Modbus IO-Link device %s address %d" % (self.host, addr))
            self.client.close()
            return False
        return result

    def poll(self):
        self.client = ModbusTcpClient(host=self.host, port=self.port, timeout=self.timeout)
        if not self.client.connect():
            logger.error("Cannot connect to bridge %s" % (self.host))
            return False
        result = self.__mbread(self.xport * 1000 + 1, 1)
        pqi = result.registers[0] & 0xff
        if (pqi & 2):
            logger.error("Modbus IO-Link device %s port %d: device not connected" % (self.host, self.xport))
            return None
        if (pqi & 4):
            logger.error("Modbus IO-Link device %s port %d: data invalid" % (self.host, self.xport))
            return None
        result = self.__mbread(8998, 1)
        length = (1 << (result.registers[0] & 0xff) + 1)
        result = self.__mbread(8999, 1)
        swap = result.registers[0] & 0xff
        # Some version of firmware requires to begin reading from 1001, not 1002
        result = self.__mbread(self.xport * 1000 + 1, length + 1)
        if not hasattr(result, 'registers'):
            return None
        registers = result.registers[-length:]
        self.client.close()
        barr = bitstring.BitArray(struct.pack('>%dH' % length, *registers))
        ut = time.time()
        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, bitoffset, bitlenght) = row
            scale = float(scale)
            offset = float(offset)
            bitoffset = int(bitoffset)
            bitlenght = int(bitlenght)
            try:
                # bool
                if datatype == 'c':
                    value = barr[bitoffset]
                # int8, int16, int32
                if datatype in ['b', 'h', 'i']:
                    value = barr[bitoffset:bitoffset+bitlenght].int
                # uint8, uint16, uint32
                if datatype in ['B', 'H', 'I']:
                    value = barr[bitoffset:bitoffset+bitlenght].uint
                # float (ieee754)
                elif datatype == 'f':
                    value = barr[bitoffset:bitoffset+bitlenght].float
            except bitstring.InterpretError:
                logger.error("Error reading Modbus IO-Link device %s port %d %s" % (self.host, self.xport, name))
                return None
            if math.isnan(value):
                logger.error("Error reading Modbus IO-Link device %s port %d %s" % (self.host, self.xport, name))
                return None
            if datatype == 'c':
                measures[name] = value
            else:
                measures[name] = round(value * scale, 8) + offset
            logger.debug('Modbus IO-Link device %s port %d %s %s' % (self.host, self.xport, measures[name], unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
