from dateutil import tz, parser
from lxml import etree
from wolf.mapconfig import WCSVMap, WCSVType
import time
import paho.mqtt.client as paho
import ssl
import json

MQProtocols = {'mqttv31' : paho.MQTTv31, 'mqttv311' : paho.MQTTv311}
TLSVersions = {'tlsv1.0': ssl.PROTOCOL_TLSv1, 'tlsv1.1': ssl.PROTOCOL_TLSv1_1, 'tlsv1.2': ssl.PROTOCOL_TLSv1_2}
TLSTransports = {'tcp': 'tcp', 'websockets': 'websockets'}

class mqtt_xml():

    params = [{'name': 'deviceid', 'type': 'string', 'required': True},
            {'name': 'host', 'type': 'string', 'default': '127.0.0.1', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 1883, 'required': True},
            {'name': 'username', 'type': 'string', 'default': None},
            {'name': 'password', 'type': 'string', 'default': None},
            {'name': 'transport', 'type': 'enum', 'default': 'tcp', 'enum': TLSTransports, 'required': True},
            {'name': 'protocol', 'type': 'enum', 'default': 'mqttv311', 'enum': MQProtocols, 'required': True},
            {'name': 'keepalive', 'type': 'int', 'default': 60, 'required': True},
            {'name': 'topic', 'type': 'string', 'required': True},
            {'name': 'qos', 'type': 'int', 'default': 0, 'required': True},
            {'name': 'command', 'type': 'string', 'default': None},
            {'name': 'replace', 'type': 'string', 'default': None},
            {'name': 'cacert', 'type': 'string', 'default': None},
            {'name': 'tlsenable', 'type': 'boolean', 'default': False},
            {'name': 'tlsverify', 'type': 'boolean', 'default': True},
            {'name': 'tlsversion', 'type': 'enum', 'default': 'tlsv1.0', 'enum': TLSVersions},
            {'name': 'csvmap', 'type': 'string', 'required': True},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
        self.mapping = WCSVMap().load(self.csvmap, WCSVType.XML)
        cache.store_meta(self.deviceid, self.name, self.description, self.mapping)

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.warn("Connected to MQTT broker %s result code %d" % (self.host, rc))
        else:
            logger.info("Connected to MQTT broker %s result code %d" % (self.host, rc))
            self.client.subscribe(self.topic, qos=self.qos)
            logger.debug('Subscribed to topic "%s" QoS %d' % (self.topic, self.qos))

    def on_disconnect(self, client, userdata, rc=0):
        if rc != 0:
            logger.warn("Disconnected from MQTT broker %s result code %d" % (self.host, rc))
        else:
            logger.debug("Disconnected from MQTT broker %s result code %d" % (self.host, rc))

    def on_message(self, client, userdata, message):
        logger.debug('Plugin %s broker %s topic "%s" QoS %s message "%s"' % (self.name, self.host, message.topic, message.qos, message.payload.decode("utf-8")))
        payload = message.payload.decode("utf-8")

        try:
            tree = etree.fromstring(payload)
        except etree.XMLSyntaxError as e:
            logger.warn('Plugin %s received invalid message on %s topic %s QoS %s: %s' % (self.name, self.host, message.topic, message.qos, str(e)))
            return

        measures = {}
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, xpath, *wrpayload) = row
            scale = float(scale)
            offset = float(offset)
            value = None
            try:
                value = tree.xpath(xpath)[0]
            except IndexError:
                value = None
                continue
            if datatype == 't':
                try:
                    dt = parser.parse(value)
                    ut = time.mktime(dt.timetuple())
                except ValueError:
                    logger.warn('Invalid timestamp %s, defaulting to current time' % value)
                continue
            if datatype in ('c'):
                try:
                    value = {'on': True, 'On': True, 'ON': True, '1': True, 1: True, 'off': False, 'Off': False, 'OFF': False, '0': False, 0: False}[payload]
                except KeyError:
                    value = False
            if datatype not in ('c', 's'):
                value = round(value * scale, 8) + offset
            measures[name] = value
            logger.debug('MQTT broker: %s topic: %s QoS: %s measure: %s value: %s %s' % (self.host, self.topic, self.qos, name, value, unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        cache.store(data)

    def on_publish(self, client, userdata, mid):
        logger.debug("Published message %d by MQTT broker %s" % (mid, self.host))

    def run(self):
        logger.info("MQTT broker %s:%d" % (self.host, self.port))

        self.client = paho.Client(transport=self.transport)
        self.client.on_message=self.on_message
        self.client.on_connect=self.on_connect
        self.client.on_disconnect=self.on_disconnect

        if self.tlsenable:
            logger.info("MQTT TLS version: %s verify: %s CA certificate: %s" % (self.tlsversion, self.tlsverify, self.cacert))
            try:
                self.client.tls_set(ca_certs=self.cacert, certfile=None, keyfile=None, cert_reqs=ssl.CERT_NONE, tls_version=self.tlsversion, ciphers=None)
            except FileNotFoundError as e:
                logger.error('TLS CA certificate not found: %s', str(e))
                return
            self.client.tls_insecure_set(not self.tlsverify)
            # Workaround for ssl.CertificateError: hostname don't match uncatchable exception
            ssl.match_hostname = lambda cert, hostname: True

        if self.username and self.password:
            self.client.username_pw_set(username=self.username, password=self.password)
        try:
            self.client.connect(self.host, port=self.port, keepalive=self.keepalive)
        except (IOError, OSError) as e:
            logger.warn('Cannot connect MQTT broker %s: %s' % (self.host, str(e)))
            return
        self.client.loop_start()
        # Workaround for setting thread name coherent with plugin's thread name
        self.client._thread.name = self.name

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Disconnected from MQTT broker %s:%d" % (self.host, self.port))

    def write(self, name, value):
        row = list(filter(lambda r: r[0] == name, self.mapping))
        if len(row):
            (name, descr, unit, datatype, rw, scale, offset, xpath, *wrpayload) = row[0]
            topic = self.topic
            if self.command:
                topic = self.command
            if not rw:
                logger.warn("Not published message on MQTT broker %s topic %s QoS %d: not writable" % (self.host, topic, self.qos))
                return False
            if self.replace:
                try:
                    replace = eval(self.replace)
                    value = replace[value]
                except (IndexError, SyntaxError):
                    pass
            if wrpayload:
                value = wrpayload[0].replace('?', json.dumps(value))
            (rc, mid) = self.client.publish(topic, payload=value, qos=self.qos)
            if rc:
                logger.warn("Not published message on MQTT broker %s topic %s QoS %d result code %d message %d: %s" % (self.host, topic, self.qos, rc, mid, value))
            else:
                logger.debug("Published message on MQTT broker %s topic %s QoS %d result code %d message %d: %s" % (self.host, topic, self.qos, rc, mid, value))
            return not rc
        return False
