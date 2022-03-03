from enocean.communicators.serialcommunicator import SerialCommunicator
from enocean.protocol.packet import RadioPacket
from enocean.protocol.constants import PACKET, RORG
import enocean.utils as utils
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

class enocean():

    def __init__(self, name):
        self.name = name
        self.wait = threading.Event()
        self.comm = None
        self.queue = queue.Queue()
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.port = config.get(self.name, 'port', fallback = None)
        self.addr = config.get(self.name, 'addr')

        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = []
        self.mapping = csvmap.load(csvfile, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def decode(self, packet):
        ut = time.time()
        measures = {}
        if packet.packet_type == PACKET.RADIO_ERP1 and packet.rorg == RORG.VLD:
            packet.select_eep(0x05, 0x00)
            packet.parse_eep()
            for k in packet.parsed:
                self.__measure_map(k, packet, measures)
        if packet.packet_type == PACKET.RADIO_ERP1 and packet.rorg == RORG.BS4:
            for k in packet.parse_eep(0x02, 0x05):
                self.__measure_map(k, packet, measures)
        if packet.packet_type == PACKET.RADIO_ERP1 and packet.rorg == RORG.BS1:
            packet.select_eep(0x00, 0x01)
            packet.parse_eep()
            for k in packet.parsed:
                self.__measure_map(k, packet, measures)
        if packet.packet_type == PACKET.RADIO_ERP1 and packet.rorg == RORG.RPS:
            for k in packet.parse_eep(0x02, 0x02):
                self.__measure_map(k, packet, measures)
        if measures:
            data = {'ts': ut, 'client_id': self.clientid, 'device_id': packet.sender_hex, 'measures': measures}
            cache.store(data)

    def __measure_map(self, k, packet, measures):
        logger.debug('Decoded radio packet type %s: %s' % (k, packet.parsed[k]))
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset) = row
            scale = float(scale)
            offset = float(offset)
            if name == k:
                if datatype in ('c', 's'):
                    value = packet.parsed[k]['value']
                else:
                    value = round(float(packet.parsed[k]['value']) * scale, 8) + offset
                unit = packet.parsed[k]['unit']
                descr = packet.parsed[k]['description']
                measures[k] = value
                logger.info('EnOcean sensor: %s packet type: %s measure: %s (%s) %s %s' % (packet.sender_hex, packet.packet_type, name, descr, value, unit))

    def __wait(self):
        global waits
        if self.port in waits:
            logger.debug("Plugin %s waiting for SerialCommunicator on port %s..." % (self.name, self.port))
            self.wait = waits[self.port]
            self.wait.wait()
        waits[self.port] = self.wait

    def __ready(self):
        if not self.wait.is_set():
            logger.debug("Plugin %s SerialCommunicator on port %s ready" % (self.name, self.port))
            self.wait.set()

    def __get_comm(self):
        global comms
        if self.port in comms:
            self.comm = comms[self.port]

    def __set_comm(self):
        global comms
        comms[self.port] = self.comm

    def __enqueue(self, packet):
        global queues
        logger.debug("Received radio packet %s" % packet)
        for queue in queues:
            queues[queue].put(packet)

    def __set_queue(self):
        global queues
        queues[self.name] = self.queue

    def run(self):
        self.__wait()
        self.__get_comm()
        if self.comm is None:
            self.comm = SerialCommunicator(port=self.port, callback=self.__enqueue)
            logger.info("Starting EnOcean listener on port %s" % self.port)
            # Workaround for setting thread name coherent with plugin's thread name
            self.comm.name = self.name
            self.comm.start()
            if self.comm.base_id:
                logger.info("The Base ID of your module is %s" % utils.to_hex_string(self.comm.base_id))
            self.__set_comm()
        self.__ready()
        self.__set_queue()

        while self.comm.is_alive():
            if self.queue.qsize() > 0:
                packet = self.queue.get()
                if hasattr(packet, 'sender_hex'):
                    if packet.sender_hex == self.addr:
                        self.decode(packet)
            time.sleep(1)

    def stop(self):
        if self.comm and self.comm.is_alive:
            self.comm.stop()
            logger.info("Stopping EnOcean listener on port %s" % self.port)
