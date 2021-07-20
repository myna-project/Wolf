from dateutil import tz, parser
from lxml import etree
from wolf.mapconfig import WCSVMap, WCSVType
import time
import paho.mqtt.client as paho
import ssl

MQProtocols = {'mqttv31' : paho.MQTTv31, 'mqttv311' : paho.MQTTv311}
TLSVersions = {'tlsv1.0': ssl.PROTOCOL_TLSv1, 'tlsv1.1': ssl.PROTOCOL_TLSv1_1, 'tlsv1.2': ssl.PROTOCOL_TLSv1_2}
TLSTransports = {'tcp': 'tcp', 'websockets': 'websockets'}

class mqtt_xml():

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.host = config.get(self.name, 'host', fallback = '127.0.0.1')
        self.port = config.getint(self.name, 'port', fallback = 1883)
        self.username = config.get(self.name, 'username', fallback = None)
        self.password = config.get(self.name, 'password', fallback = None)
        self.transport = config.getenum(self.name, 'transport', enum=TLSTransports, fallback = 'tcp')
        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.XML)
        self.protocol = config.getenum(self.name, 'protocol', enum=MQProtocols, fallback = 'mqttv311')
        self.keepalive = config.getint(self.name, 'keepalive', fallback = 60)
        self.topic = config.get(self.name, 'topic')
        self.qos = config.getint(self.name, 'qos', fallback = 0)
        self.cacert = config.get(self.name, 'cacert', fallback = None)
        self.tlsenable = config.getboolean(self.name, 'tlsenable', fallback = False)
        self.tlsversion = config.getenum(self.name, 'tlsversion', enum=TLSVersions, fallback = 'tlsv1.0')
        self.tlsverify = config.getboolean(self.name, 'tlsverify', fallback = True)
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

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
            (name, descr, unit, datatype, rw, scale, offset, xpath) = row
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
            if datatype not in ('c', 's'):
                value = round(value * scale, 14) + offset
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
            (name, descr, unit, datatype, rw, scale, offset, jsonpath) = row[0]
            (rc, mid) = self.client.publish(topic, payload=value, qos=self.qos)
            if rc:
                logger.warn("Not published message on MQTT broker %s topic %s QoS %d result code %d message %d: %s" % (self.host, topic, self.qos, rc, mid, value))
            else:
                logger.debug("Published message on MQTT broker %s topic %s QoS %d result code %d message %d: %s" % (self.host, topic, self.qos, rc, mid, value))
            return not rc
        return False
