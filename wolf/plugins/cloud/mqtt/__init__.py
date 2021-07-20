import datetime
from datetime import datetime
from dateutil import tz, parser
import json
import time
import os
import paho.mqtt.client as paho
import ssl

MQProtocols = {'mqttv31' : paho.MQTTv31, 'mqttv311' : paho.MQTTv311}
TLSVersions = {'tlsv1.0': ssl.PROTOCOL_TLSv1, 'tlsv1.1': ssl.PROTOCOL_TLSv1_1, 'tlsv1.2': ssl.PROTOCOL_TLSv1_2}
TLSTransports = {'tcp': 'tcp', 'websockets': 'websockets'}

class mqtt():

    def __init__(self, name):
        self.name = name
        self.host = config.get(self.name, 'host', fallback = '127.0.0.1')
        self.port = config.getint(self.name, 'port', fallback = 1883)
        self.username = config.get(self.name, 'username', fallback = None)
        self.password = config.get(self.name, 'password', fallback = None)
        self.transport = config.getenum(self.name, 'transport', enum=TLSTransports, fallback = 'tcp')
        self.protocol = config.getenum(self.name, 'protocol', enum=MQProtocols, fallback = 'mqttv311')
        self.keepalive = config.getint(self.name, 'keepalive', fallback = 60)
        self.topic = config.get(self.name, 'topic')
        self.qos = config.getint(self.name, 'qos', fallback = 0)
        self.cacert = config.get(self.name, 'cacert', fallback = None)
        self.tlsenable = config.getboolean(self.name, 'tlsenable', fallback = False)
        self.tlsversion = config.getenum(self.name, 'tlsversion', enum=TLSVersions, fallback = 'tlsv1.2')
        self.tlsverify = config.getboolean(self.name, 'tlsverify', fallback = True)
        self.retain = config.getboolean(self.name, 'retain', fallback = False)
        logger.info("MQTT broker %s:%d" % (self.host, self.port))

        self.client = paho.Client(transport=self.transport)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message=self.on_message

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

    def on_connect(self, client, userdata, flags, rc):
        if rc != 0:
            logger.warn("Connected to MQTT broker %s result code %d" % (self.host, rc))
        else:
            logger.info("Connected to MQTT broker %s result code %d" % (self.host, rc))
            topic = '%s/write' % self.topic
            self.client.subscribe(topic, qos=self.qos)
            logger.debug('Subscribed to topic "%s" QoS %d' % (topic, self.qos))

    def on_disconnect(self, client, userdata, rc=0):
        if rc != 0:
            logger.warn("Disconnected from MQTT broker %s result code %d" % (self.host, rc))
        else:
            logger.debug("Disconnected from MQTT broker %s result code %d" % (self.host, rc))

    def on_publish(self, client, userdata, mid):
        logger.debug("Published message %d by MQTT broker %s" % (mid, self.host))

    def post(self, rawdata):
        topic = '%s/measures' % self.topic
        data = rawdata.copy()
        dt = datetime.fromtimestamp(data['ts'])
        dd = dt.replace(tzinfo=tz.gettz())
        ts = dd.strftime('%Y-%m-%dT%H:%M:%S%z')
        data['at'] = ts
        del data['ts']
        measures = []
        for measure in data['measures']:
            measures.append({'measure_id': measure, 'value': data['measures'][measure]})
        data['measures'] = measures
        (rc, mid) = self.client.publish(topic, payload=json.dumps(data), qos=self.qos, retain=self.retain)
        if rc:
            logger.warn("Published measures on MQTT broker %s topic %s QoS %d result code %d message %d retain %r" % (self.host, topic, self.qos, rc, mid, self.retain))
        else:
            logger.debug("Published measures on MQTT broker %s topic %s QoS %d result code %d message %d retain %r" % (self.host, topic, self.qos, rc, mid, self.retain))
        return not rc

    def post_config(self):
        data = cache.load_meta()
        topic = '%s/config' % self.topic
        (rc, mid) = self.client.publish(topic, payload=json.dumps(data), qos=self.qos, retain=self.retain)
        if rc:
            logger.warn("Published configuration on MQTT broker %s topic %s QoS %d result code %d message %d retain %r" % (self.host, topic, self.qos, rc, mid, self.retain))
        else:
            logger.debug("Published configuration on MQTT broker %s topic %s QoS %d result code %d message %d retain %r" % (self.host, topic, self.qos, rc, mid, self.retain))
        return not rc

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Disconnected from MQTT broker %s:%d" % (self.host, self.port))

    def on_message(self, client, userdata, message):
        logger.debug('Plugin %s broker %s topic "%s" QoS %s message "%s"' % (self.name, self.host, message.topic, message.qos, message.payload.decode("utf-8")))
        payload = message.payload.decode("utf-8")
        try:
            data = json.loads(payload)
        except ValueError:
            logger.warn('Plugin %s received empty or invalid message on %s topic %s QoS %s' % (self.name, self.host, message.topic, message.qos))
            return
        data = json.loads(payload)
        queue.put(data)
