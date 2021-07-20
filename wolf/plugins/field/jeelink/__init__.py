import crcmod
import serial
from wolf.mapconfig import WCSVMap, WCSVType
import time
import os
import threading

comms = {}
waits = {}
queues = {}

try:
    import queue
except ImportError:
    import Queue as queue

class jeelink():

    def __init__(self, name):
        self.name = name
        self.wait = threading.Event()
        self.comm = None
        self.queue = queue.Queue()
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.port = config.get(self.name, 'port', fallback = None)
        self.init = config.get(self.name, 'init', fallback = None)

        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def decode(self, payload):
        ut = time.time()
        measures = {}
        decoded = {}
        postamble = b'\x00'*8

        # 30 7B A1 CE 44 78 D6
        # FF II UT TT HH CC BB

        # Fine Offset WH31 (Froggit DP50)
        if payload[0] == 0x30  and payload.find(postamble) == 0x07:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:5]) != payload[5]:
                return
            if ((sum(payload[0:6]) % 0x100) != payload[6]):
                return
            decoded['LOWBAT'] = bool((payload[2] & 0x08) >> 3)
            decoded['CH'] =  int((payload[2] & 0x70) >> 4) + 1
            decoded['HUM'] =  int(payload[4])
            decoded['TMP'] = (((payload[2] & 0x07) << 8) + payload[3]) * 0.1 - 40
            logger.debug("Detected Fine Offset WH31 CH:%d TMP: %f°C HUM: %d%% LOWBAT: %s" % (decoded['CH'], decoded['TMP'], decoded['HUM'], decoded['LOWBAT']))

        #    e5 02 72 28 27 21 c9 bb aa
        #    ?I IT TT HH PP PP CC BB

        # Fine Offset WH25
        if (payload[0] & 0xf0) == 0xe0 and payload.find(postamble) == 0x09:
            invalid = (payload[1] & 0x04) >> 2
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:6]) != payload[6]:
                return
            if ((sum(payload[0:7]) % 0x100) != payload[7]):
                return
            if invalid:
                return
            decoded['LOWBAT'] = (payload[1] & 0x08) >> 3
            temp = (payload[1] & 0x03) << 8 | payload[2]
            decoded['TMP'] = (temp - 400) * 0.1
            decoded['HUM'] = payload[3]
            decoded['PRES'] = (payload[4] << 8 | payload[5]) * 0.1
            logger.debug("Detected Fine Offset WH25 TMP: %f°C HUM: %d%% PRES: %fhPa LOWBAT: %s" % (decoded['TMP'], decoded['HUM'], decoded['PRES'], decoded['LOWBAT']))

        #    e3 92 5f 21 27 6c 88
        #    ?I IT TT HH PP PP CC

        # Fine Offset WH32B
        if (payload[0] & 0xf0) == 0xe0 and payload.find(postamble) == 0x07:
            invalid = (payload[1] & 0x04) >> 2
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:6]) != payload[6]:
                return
            if invalid:
                return
            decoded['LOWBAT'] = (payload[1] & 0x08) >> 3
            temp = (payload[1] & 0x03) << 8 | payload[2]
            decoded['TMP'] = (temp - 400) * 0.1
            decoded['HUM'] = payload[3]
            decoded['PRES'] = (payload[4] << 8 | payload[5]) * 0.1
            logger.debug("Detected Fine Offset WH32B TMP: %f°C HUM: %d%% PRES: %fhPa LOWBAT: %s" % (decoded['TMP'], decoded['HUM'], decoded['PRES'], decoded['LOWBAT']))

        #   51 00 6b 58 6e 7f 24 f8 d2 ff ff ff 3c 28 08
        #   FF II II II TB YY MM ZA AA XX XX XX CC SS

        # Fine Offset WH51
        if payload[0] == 0x51 and payload.find(postamble) == 0x0f:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:12]) != payload[12]:
                return
            if ((sum(payload[0:13]) % 0x100) != payload[13]):
                return
            bat_mv = (payload[4] & 0x1f) * 100
            decoded['BAT'] = (bat_mv - 700) / 900.0
            decoded['MOIST'] = payload[6]
            logger.debug("Detected Fine Offset WH51 MOISTURE: %f%% BAT: %d%%" % (decoded['MOIST'], decoded['BAT']))

        #   42 cc 41 9a 41 ae c1 99 09
        #   FF DD ?P PP ?A AA CC BB

        # Fine Offset WH0290
        if payload[0] == 0x42 and payload.find(postamble) == 0x09:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:6]) != payload[6]:
                return
            if ((sum(payload[0:7]) % 0x100) != payload[7]):
                return
            decoded['PM25'] = (payload[2] & 0x3f) << 8 | payload[3]
            decoded['PM100'] = (payload[4] & 0x3f) << 8 | payload[5]
            logger.debug("Detected Fine Offset WH0290 PM2.5: %fug/m3 PM10: %fug/m3" % (decoded['PM25'], decoded['PM100']))

        # WH24  24 bf 0a e2 06 4e 08 02 00 4a 00 01 00 00 00 8f 07 20
        #       FF II DD VT TT HH WW GG RR RR UU UU LL LL LL CC BB

        # Fine Offset WH24
        if payload[0] == 0x24 and payload.find(postamble) == 0x12:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:15]) != payload[15]:
                return
            if ((sum(payload[0:16]) % 0x100) != payload[16]):
                return
            decoded['WNDDIR'] = payload[2] | (payload[3] & 0x80) << 1
            decoded['LOWBAT'] = (payload[3] & 0x08) >> 3
            temp = (payload[3] & 0x07) << 8 | payload[4]
            decoded['TMP'] = (temp - 400) * 0.1
            decoded['HUM'] = payload[5]
            wind_avg = payload[6] | (payload[3] & 0x10) << 4
            wind_max = payload[7]
            decoded['WNDAVG'] = wind_avg * 0.125 * 1.12 # WH24
            decoded['WNDMAX'] = wind_max * 1.12 # WH24
            rain = payload[8] << 8 | payload[9]
            decoded['RAIN'] = rain * 0.3 # WH24
            decoded['UVR'] = payload[10] << 8 | payload[11]
            decoded['LUX'] = (payload[12] << 16 | payload[13] << 8 | payload[14]) * 0.1
            uvi_table = [432, 851, 1210, 1570, 2017, 2450, 2761, 3100, 3512, 3918, 4277, 4650, 5029]
            uvi = 0
            while (uvi < len(uvi_table) and uvi_table[uvi] < decoded['UVR']):
                uvi += 1
            decoded['UVI'] = uvi
            logger.debug("Detected Fine Offset WH24 WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° TEMP: %f°C HUM: %d%% LOWBAT: %s RAIN: %dmm LUX: %flux UVI: %d" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['TMP'], decoded['HUM'], decoded['LOWBAT'], decoded['RAIN'], decoded['LUX'], decoded['UVI']))

        # WH65B 24 50 67 e2 87 41 00 00 00 01 00 02 00 00 00 8c 14 20 10
        #       FF II DD VT TT HH WW GG RR RR UU UU LL LL LL CC BB

        # Fine Offset WH65B
        if payload[0] == 0x24:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:15]) != payload[15]:
                return
            if ((sum(payload[0:16]) % 0x100) != payload[16]):
                return
            decoded['WNDDIR'] = payload[2] | (payload[3] & 0x80) << 1
            decoded['LOWBAT'] = (payload[3] & 0x08) >> 3
            temp = (payload[3] & 0x07) << 8 | payload[4]
            decoded['TMP'] = (temp - 400) * 0.1
            decoded['HUM'] = payload[5]
            wind_avg = payload[6] | (payload[3] & 0x10) << 4
            wind_max = payload[7]
            decoded['WNDAVG'] = wind_avg * 0.125 * 1.12 # WH65B
            decoded['WNDMAX'] = wind_max * 1.12 # WH65B
            rain = payload[8] << 8 | payload[9]
            decoded['RAIN'] = rain * 0.254 # WH65B
            decoded['UVR'] = payload[10] << 8 | payload[11]
            decoded['LUX'] = (payload[12] << 16 | payload[13] << 8 | payload[14]) * 0.1
            uvi_table = [432, 851, 1210, 1570, 2017, 2450, 2761, 3100, 3512, 3918, 4277, 4650, 5029]
            uvi = 0
            while (uvi < len(uvi_table) and uvi_table[uvi] < decoded['UVR']):
                uvi += 1
            decoded['UVI'] = uvi
            logger.debug("Detected Fine Offset WH65B WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° TEMP: %f°C HUM: %d%% LOWBAT: %s RAIN: %dmm LUX: %flux UVI: %d" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['TMP'], decoded['HUM'], decoded['LOWBAT'], decoded['RAIN'], decoded['LUX'], decoded['UVI']))

        #       38 a2 8f 02 00 ff e7 51
        #       FI IT TT RR RR ?? CC AA

        # Fine Offset WH0530
        if (payload[0] & 0xf0) == 0x30 and payload.find(postamble) == 0x08:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:6]) != payload[6]:
                return
            if ((sum(payload[0:7]) % 0x100) != payload[7]):
                return
            decoded['LOWBAT'] = (payload[1] >> 3) & 0x1
            temp = (payload[1] & 0x7) << 8 | payload[2]
            decoded['TMP'] = (temp - 400) * 0.1
            decoded['RAIN'] = (payload[4] << 8 | payload[3]) * 0.3
            logger.debug("Detected Fine Offset WH0530 TMP: %f°C RAIN: %fmm LOWBAT: %s" % (decoded['TMP'], decoded['RAIN'], decoded['LOWBAT']))

        #       0c 2a 18 80 17 34 18 fe 79 fe 29 28 22 02 30 ff f0 fb 00 00
        #       CC CC FF -- -- -- -- GG GW WW DD D- TT T- HH -- -t -- -- --
        #       f7 43 18 80 17 34 18 ff ff ff 18 08 ff ee 69 ff 01 91 00 00
        #       CC CC FF -- -- -- -- GG GW WW DD D- -- RR RR -- -t -- -- --

        # Bresser 6-in-1
        if payload[2] == 0x18 and payload.find(postamble) == 0x12:
            crc16 = crcmod.predefined.Crc('xmodem')
            crc16.update(payload[2:17])
            if (crc16.crcValue != int.from_bytes(payload[0:2], byteorder='big')):
                return
            if (sum(payload[2:18]) % 0x100) != 0xff:
                return
            decoded['WNDMAX'] = (0xff - (payload[7])) * 10 + (0x0f - ((payload[8] & 0xf0) >> 4));
            decoded['WNDAVG'] = (0xff - (payload[9])) * 10 + 0x0f - (payload[8] & 0x0f)
            decoded['WNDDIR'] = ((payload[10] & 0xf0) >> 4) * 100 + (payload[10] & 0x0f) * 10 + ((payload[11] & 0xf0) >> 4)
            if (payload[16] & 0xf == 0):
                temp = ((payload[12] & 0xf0) >> 4) * 100 + (payload[12] & 0x0f) * 10 + ((payload[13] & 0xf0) >> 4)
                if (temp > 600):
                    temp -= 1000
                decoded['TMP'] = temp * 0.1
                decoded['HUM'] = (payload[14] & 0x0f) + ((payload[14] & 0xf0) >> 4) * 10
                logger.debug("Detected Bresser 6in1 WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° TEMP: %f°C HUM: %d%%" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['TMP'], decoded['HUM']))
            elif (payload[16] & 0xf == 1):
                rain = ((0xff - payload[13]) & 0x0f) * 100 + ((((0xff - payload[14]) & 0xf0) >> 4) * 10 + (0xff - payload[14]) & 0x0f)
                decoded['RAIN'] = rain * 0.1
                logger.debug("Detected Bresser 6in1 WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° RAIN: %fmm" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['RAIN']))

        #       ee 93 7f f7 bf fb ef 9e fe ae bf ff ff 11 6c 80 08 40 04 10 61 01 51 40 00 00
        #       CC CC CC CC CC CC CC CC CC CC CC CC CC uu II  G GG DW WW    TT  T HH RR  R  t

        # Bresser 5-in-1
        if [_a ^ _b for _a, _b in zip(payload[0:13], payload[13:26])] == [0xff]*13 and payload.find(postamble) == 0x18:
            temp = (payload[20] & 0x0f) + ((payload[20] & 0xf0) >> 4) * 10 + (payload[21] & 0x0f) * 100
            if payload[25] & 0x0f:
                temp = -temp
            decoded['TMP'] = temp * 0.1
            decoded['HUM'] = (payload[22] & 0x0f) + ((payload[22] & 0xf0) >> 4) * 10
            decoded['WNDDIR'] = ((payload[17] & 0xf0) >> 4) * 22.5
            decoded['WNDAVG'] = ((payload[18] & 0x0f) + ((payload[18] & 0xf0) >> 4) * 10 + (payload[17] & 0x0f) * 100) * 0.1
            decoded['WNDMAX'] = ((payload[16] & 0x0f) + ((payload[16] & 0xf0) >> 4) * 10 + (payload[15] & 0x0f) * 100) * 0.1
            decoded['RAIN'] = ((payload[23] & 0x0f) + ((payload[23] & 0xf0) >> 4) * 10 + (payload[24] & 0x0f) * 100) * 0.1
            logger.debug("Detected Bresser 5in1 WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° RAIN: %fmm" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['RAIN']))

        #       a1 31 d1 59 00 00 09 dd 02 68
        #       a1 82 0e 5d 02 04 00 4e 06 86
        #       FI IT TT HH SS GG ?R RR BD CC

        # Fine Offset WH1080/WH3080
        if (payload[0] & 0xf0) == 0xa0 and payload.find(postamble) == 0x0a:
            crc8 = crcmod.mkCrcFun(0x131, rev=False, initCrc=0x0000, xorOut=0x0000)
            if crc8(payload[0:9]) != payload[9]:
                return
            if ((payload[0] >> 4) == 0x0a):
                temp = ((payload[1] & 0x0f) << 8) | payload[2]
                decoded['HUM'] = payload[3]
                dir_table = [0, 23, 45, 68, 90, 113, 135, 158, 180, 203, 225, 248, 270, 293, 315, 338]
                decoded['WNDDIR'] = dir_table[payload[8] & 0x0f]
                decoded['WNDAVG'] = payload[4] * 0.34
                decoded['WNDMAX'] = payload[5] * 0.34
                decoded['RAIN'] = (((payload[6] & 0x0f) << 8) | payload[7]) * 0.3
                decoded['LOWBAT'] = (payload[8] >> 4)
                logger.debug("Detected Fine Offset WH1080/WH3080  WNDMAX: %d m/s WNDAVG: %d m/s WNDDIR: %d° RAIN: %fmm LOWBAT: %s" % (decoded['WNDMAX'], decoded['WNDAVG'], decoded['WNDDIR'], decoded['RAIN'], decoded['LOWBAT']))
            if ((payload[0] >> 4) == 0x07):
                decoded['LUX'] = ((payload[3] << 16) | (payload[4] << 8) | payload[5]) * 0.1
                decoded['UVI'] = -1
                if payload[2] == 85:
                    decoded['UVI'] = payload[1] & 0x0f
                logger.debug("Detected Fine Offset WH1080/WH3080 LUX: %flux UVI: %d" % (decoded['LUX'], decoded['UVI']))
 
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset) = row
            scale = float(scale)
            offset = float(offset)
            if name in decoded:
                if datatype in ('c', 's'):
                    measures[name] = value
                else:
                    measures[name] = round(float(decoded[name]) * scale, 14) + offset

        if measures:
            data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
            cache.store(data)

    def __wait(self):
        global waits
        if self.port in waits:
            logger.debug("Plugin %s waiting for Jeelink on port %s..." % (self.name, self.port))
            self.wait = waits[self.port]
            self.wait.wait()
        waits[self.port] = self.wait

    def __ready(self):
        if not self.wait.is_set():
            logger.debug("Plugin %s Jeelink on port %s ready" % (self.name, self.port))
            self.wait.set()

    def __get_comm(self):
        global comms
        if self.port in comms:
            self.comm = comms[self.port]

    def __set_comm(self):
        global comms
        comms[self.port] = self.comm

    def __enqueue(self):
        global queues
        while self.comm.isOpen():
            try:
                line = self.comm.readline()
            except serial.SerialException as e:
                logger.error(str(e))
                return
            try:
                data = line.decode().rstrip().split(' ')
                if not data:
                    continue
                value = ''.join([x.zfill(2) for x in data[:64]])
                payload = bytearray.fromhex(value)
            except ValueError:
                payload = None
            if payload and len(payload) == 64:
                if len(data) > 64:
                    logger.debug("Received radio payload %s RSSI: %s" % (payload.hex(), data[64]))
                else:
                    logger.debug("Received radio payload %s" % payload.hex())
                for queue in queues:
                    queues[queue].put(payload)

    def __set_queue(self):
        global queues
        queues[self.name] = self.queue

    def __listen(self):
        global queues
        queues[self.name] = self.queue

    def run(self):
        self.__wait()
        self.__get_comm()
        if self.comm is None:
            try:
                self.comm = serial.Serial(port=self.port, baudrate=57600)
            except (OSError, serial.SerialException) as e:
                logger.error(str(e))
                return
            if self.init:
                time.sleep(2)
                self.comm.write(self.init.encode())
            logger.info("Starting Jeelink listener on port %s (init string: %s)" % (self.port, self.init))
            listen = threading.Thread(target = self.__enqueue)
            listen.name = self.name
            listen.start()
            self.__set_comm()
        self.__ready()
        self.__set_queue()

        while True:
            if self.queue.qsize() > 0:
                payload = self.queue.get()
                self.decode(payload)
            time.sleep(1)

    def stop(self):
        if self.comm and self.comm.isOpen():
            self.comm.close()
            logger.info("Stopping Jeelink listener on port %s" % self.port)
