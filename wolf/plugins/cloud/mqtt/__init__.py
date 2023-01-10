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

    params = [{'name': 'host', 'type': 'string', 'default': '127.0.0.1', 'required': True},
            {'name': 'port', 'type': 'int', 'default': 1883, 'required': True},
            {'name': 'username', 'type': 'string', 'default': None},
            {'name': 'password', 'type': 'string', 'default': None},
            {'name': 'transport', 'type': 'enum', 'default': 'tcp', 'enum': TLSTransports, 'required': True},
            {'name': 'protocol', 'type': 'enum', 'default': 'mqttv311', 'enum': MQProtocols, 'required': True},
            {'name': 'keepalive', 'type': 'int', 'default': 60, 'required': True},
            {'name': 'topic', 'type': 'string', 'required': True},
            {'name': 'qos', 'type': 'int', 'default': 0, 'required': True},
            {'name': 'retain', 'type': 'boolean', 'default': False},
            {'name': 'cacert', 'type': 'string', 'default': None},
            {'name': 'tlsenable', 'type': 'boolean', 'default': False},
            {'name': 'tlsverify', 'type': 'boolean', 'default': True},
            {'name': 'tlsversion', 'type': 'enum', 'default': 'tlsv1.0', 'enum': TLSVersions},
            {'name': 'description', 'type': 'string', 'default': ''},
            {'name': 'disabled', 'type': 'boolean', 'default': False}]

    def __init__(self, name):
        self.name = name
        self.config = config.parse(self.name, self.params)
        self.__dict__.update(self.config)
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
            meta = cache.load_meta(client_id = data['client_id'], device_id = data['device_id'], measure_id = measure)[0]
            measures.append({'measure_id': measure, 'value': data['measures'][measure], 'measure_unit': meta['measure_unit'], 'measure_type': meta['measure_type']})
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
