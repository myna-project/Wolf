from wolf.mapconfig import WCSVMap, WCSVType
import math
import os
import time
import threading

from gurux_serial import GXSerial
from gurux_net import GXNet
from gurux_net.enums import NetworkType
from gurux_dlms.enums import ObjectType
from gurux_dlms.objects.GXDLMSObjectCollection import GXDLMSObjectCollection
from gurux_dlms.enums.DataType import DataType
from gurux_dlms.GXDateTime import GXDateTime
from gurux_dlms.internal._GXCommon import _GXCommon
from gurux_dlms import GXDLMSException, GXDLMSExceptionResponse, GXDLMSConfirmedServiceError
from gurux_common.io import Parity, StopBits, BaudRate
from gurux_dlms import GXDLMSClient, GXReplyData, ValueEventArgs
from gurux_dlms.enums import Authentication, Security, InterfaceType, Standard, ObjectType
from gurux_dlms.GXByteBuffer import GXByteBuffer
from gurux_dlms.objects.GXDLMSData import GXDLMSData
from gurux_dlms.enums import InterfaceType, ObjectType, Authentication, Conformance, DataType,\
    Security, AssociationResult, SourceDiagnostic, AccessServiceCommandType
from gurux_common import ReceiveParameters, GXCommon, TimeoutException
from gurux_dlms.objects import GXDLMSObject, GXDLMSObjectCollection, GXDLMSData, GXDLMSRegister,\
    GXDLMSDemandRegister, GXDLMSProfileGeneric, GXDLMSExtendedRegister

SerialBaudRates = {'300': 300, '600': 600, '1800': 1800, '2400': 2400, '4800': 4800, '9600': 9600, '19200': 19200, '38400': 38400}
SerialParity = {'NONE': Parity.NONE, 'N': Parity.NONE, 'ODD': Parity.ODD, 'O': Parity.ODD, 'EVEN': Parity.EVEN, 'E': Parity.EVEN, 'MARK': Parity.MARK, 'M': Parity.MARK, 'SPACE': Parity.SPACE, 'S': Parity.SPACE}
SerialByteSize = {'7': 7, '8': 8}
SerialStopBits = {'1': StopBits.ONE, '2': StopBits.TWO}
LNSN = {'ln': True, 'LN': True, 'sn': False, 'SN': False}

locks = {}

class dlms_serial():

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'port', 'type': 'string', 'required': True},
            {'name': 'serialno', 'type': 'int', 'required': True},
            {'name': 'clientaddress', 'type': 'int', 'default': 0x10, 'required': True},
            {'name': 'cachedir', 'type': 'string', 'default': '/tmp'},
            {'name': 'stopbits', 'type': 'enum', 'default': '1', 'enum': SerialStopBits, 'required': True},
            {'name': 'bytesize', 'type': 'enum', 'default': '8', 'enum': SerialByteSize, 'required': True},
            {'name': 'parity', 'type': 'enum', 'default': 'N', 'enum': SerialParity, 'required': True},
            {'name': 'baudrate', 'type': 'enum', 'default': '9600', 'enum': SerialBaudRates, 'required': True},
            {'name': 'referencing', 'type': 'enum', 'default': 'ln', 'enum': LNSN, 'required': True},
            {'name': 'timeout', 'type': 'float', 'default': 1000, 'required': True},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.lock = threading.Lock()
        self.clientid = config.clientid
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
        self.mapping = WCSVMap().load(self.csvmap, WCSVType.Raw)
        cache.store_meta(self.deviceid, self.name, self.description, self.mapping)

        self.media = GXSerial(self.port)
        self.media.baudrate = self.baudrate
        self.media.bytesize = self.bytesize
        self.media.parity = self.parity
        self.media.stopbits = self.stopbits

        self.client = GXDLMSClient(True)
        self.client.clientAddress = self.clientaddress
        self.client.serverAddress = GXDLMSClient.getServerAddressFromSerialNumber(self.serialno)
        self.client.useLogicalNameReferencing = self.referencing

        self.cached = False

    def run(self):
        if not self.serialno:
            return
        # cache association view, scalers, units, profiles
        self.cached = False
        cachefile = os.path.join(self.cachedir, '%s.xml' % self.serialno)
        if cachefile and os.path.exists(cachefile):
            try:
                c = GXDLMSObjectCollection.load(cachefile)
                self.client.objects.extend(c)
                self.cached = True
            except Exception as e:
                pass

        if not self.cached:
            logger.warning('Missing cache for meter %d' % self.serialno)
            self.open()
            if self.media.isOpen():
                try:
                    self.getAssociationView()
                    self.readScalerUnitsProfiles()
                    self.client.objects.save(cachefile)
                    self.cached = True
                except Exception as e:
                    logger.error(str(e))
            self.close()

    def stop(self):
        self.close()

    def open(self):
        self.__lacquire()
        try:
            self.media.open()
        except Exception as e:
            logger.error('%s: %s' % (str(self.media), str(e)))
            self.close()
            return

        # SNMR and AARE handshake
        try:
            reply = GXReplyData()
            data = self.client.snrmRequest()
            if data:
                self.readDLMSPacket(data, reply)
                self.client.parseUAResponse(reply.data)
                size = self.client.limits.maxInfoTX + 40
                logger.debug('DLMS Max RX %d bytes TX %d bytes' % (self.client.limits.maxInfoRX, self.client.limits.maxInfoTX))
                self.replyBuff = bytearray(size)
            reply.clear()
            self.readDataBlock(self.client.aarqRequest(), reply)
            self.client.parseAareResponse(reply.data)
            reply.clear()
        except Exception as e:
            logger.error('cannot connect %s client address 0x%x server address 0x%x: %s' % (str(self.media), self.client.clientAddress, self.client.serverAddress, str(e)))
            self.close()

    def close(self):
        if self.media and self.media.isOpen():
            logger.debug("DisconnectRequest")
            try:
                reply = GXReplyData()
                self.readDLMSPacket(self.client.disconnectRequest(), reply)
                self.media.close()
            except Exception:
                pass
        self.__lrelease()

    def read(self, item, attributeIndex):
        data = self.client.read(item, attributeIndex)[0]
        reply = GXReplyData()
        self.readDataBlock(data, reply)
        #Update data type on read.
        if item.getDataType(attributeIndex) == DataType.NONE:
            item.setDataType(attributeIndex, reply.valueType)
        if reply.error == 0:
            return self.client.updateValue(item, attributeIndex, reply.value)
        else:
            return TranslatorSimpleTags.errorCodeToString(reply.error)

    def readList(self, list_):
        if not list_:
            return
        data = self.client.readList(list_)
        reply = GXReplyData()
        values = list()
        for it in data:
            self.readDataBlock(it, reply)
            if reply.value:
                values.extend(reply.value)
            reply.clear()
        if len(values) != len(list_):
            raise ValueError("Invalid reply. Read items count do not match.")
        self.client.updateValues(list_, values)

    def readDataBlock(self, data, reply):
        if data:
            if isinstance(data, (list)):
                for it in data:
                    reply.clear()
                    self.readDataBlock(it, reply)
            else:
                self.readDLMSPacket(data, reply)
                while reply.isMoreData():
                    if reply.isStreaming():
                        data = None
                    else:
                        data = self.client.receiverReady(reply)
                    self.readDLMSPacket(data, reply)

    def readDLMSPacket(self, data, reply=None):
        if not reply:
            reply = GXReplyData()
        if isinstance(data, bytearray):
            self.readDLMSPacket2(data, reply)
        elif data:
            for it in data:
                reply.clear()
                self.readDLMSPacket2(it, reply)

    def readDLMSPacket2(self, data, reply):
        if not data:
            return
        notify = GXReplyData()
        reply.error = 0
        eop = 0x7E
        #In network connection terminator is not used.
        if self.client.interfaceType == InterfaceType.WRAPPER and isinstance(self.media, GXNet):
            eop = None
        p = ReceiveParameters()
        p.eop = eop
        p.waitTime = self.timeout
        if eop is None:
            p.Count = 8
        else:
            p.Count = 5
        self.media.eop = eop
        rd = GXByteBuffer()
        with self.media.getSynchronous():
            if not reply.isStreaming():
                logger.debug("TX: \t" + GXByteBuffer.hex(data))
                self.media.send(data)
            pos = 0
            try:
                while not self.client.getData(rd, reply, notify):
                    if notify.data.size != 0:
                        if not notify.isMoreData():
                            notify.clear()
                        continue
                    if not p.eop:
                        p.count = self.client.getFrameSize(rd)
                    while not self.media.receive(p):
                        pos += 1
                        if pos == 3:
                            raise TimeoutException("Failed to receive reply from the device in given time.")
                        logger.error("Data send failed.  Try to resend " + str(pos) + "/3")
                        self.media.send(data, None)
                    rd.set(p.reply)
                    p.reply = None
            except Exception as e:
                logger.error("RX: \t" + str(rd))
                raise e
            logger.debug("RX: \t" + str(rd))

    def getAssociationView(self):
        reply = GXReplyData()
        self.readDataBlock(self.client.getObjectsRequest(), reply)
        self.client.parseObjects(reply.data, True, False)

    def readScalerUnitsProfiles(self):
        objs = self.client.objects.getObjects([ObjectType.REGISTER, ObjectType.EXTENDED_REGISTER, ObjectType.DEMAND_REGISTER, ObjectType.PROFILE_GENERIC])
        list_ = list()
        for it in objs:
            if isinstance(it, (GXDLMSRegister,GXDLMSExtendedRegister, GXDLMSProfileGeneric)):
                list_.append((it, 3))
            elif isinstance(it, (GXDLMSDemandRegister,)):
                list_.append((it, 4))
        self.readList(list_)

    def __lacquire(self):
        global locks
        if self.port in locks:
            self.lock = locks[self.port]
            logger.debug("Plugin %s resource %s already in use" % (self.name, self.port))
        locks['%s' % (self.port)] = self.lock
        self.lock.acquire()
        logger.debug("Plugin %s lock on %s acquired" % (self.name, self.port))

    def __lrelease(self):
        if self.lock.locked():
            self.lock.release()
            logger.debug("Plugin %s lock on %s released" % (self.name, self.port))

    def poll(self):
        if not self.cached:
            self.run()
        list_ = list()
        objs = self.client.objects.getObjects([ObjectType.DATA, ObjectType.CLOCK, ObjectType.REGISTER, ObjectType.EXTENDED_REGISTER, ObjectType.DEMAND_REGISTER])
        for obj in objs:
            list_.append((obj, 2))
        ut = time.time()
        self.open()
        if not self.media.isOpen():
            return []
        try:
            self.readList(list_)
        except Exception as e:
            logger.error(str(e))
            self.close()
            return []
        self.close()

        obj = objs.findByLN(False, _GXCommon.toLogicalName('1.0.0.0.0.255'))
        values = obj.getValues()
        serialno = values[1]
        obj = objs.findByLN(False, _GXCommon.toLogicalName('1.0.0.1.0.255'))
        values = obj.getValues()
        billno = values[1]
        obj = objs.findByLN(False, _GXCommon.toLogicalName('0.0.97.97.0.255'))
        values = obj.getValues()
        error = values[1]
        obj = objs.findByLN(False, _GXCommon.toLogicalName('0.0.1.0.0.255'))
        values = obj.getValues()
        date = values[1].value
        obj = objs.findByLN(False, _GXCommon.toLogicalName('1.0.0.1.2.255'))
        values = obj.getValues()
        last0 = GXDLMSClient.changeType(values[1], DataType.DATETIME).value
        logger.info ('DLMS meter %d F.F (%s) 0.0.0 (%s) 0.1.0 (%s) 0.9.1 (%s) 0.9.2 (%s) 0.9.6 (%s) 0.9.7 (%s)' % (self.serialno, GXByteBuffer(error), serialno, billno, date.strftime('%y-%m-%d'), date.strftime('%H:%M:%S'), last0.strftime('%y-%m-%d'), last0.strftime('%H:%M:%S')))

        measures = {}
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, obis) = row
            scale = float(scale)
            offset = float(offset)
            value = 0
            obj = objs.findByLN(False, _GXCommon.toLogicalName(obis))
            if not obj:
                continue
            values = obj.getValues()
            value = values[1]
            measures[name] = round(value * scale, 8) + offset
            logger.debug('DLMS meter %d OBIS %s (%s): %f %s' % (self.serialno, obis, obj.description, measures[name], unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data
