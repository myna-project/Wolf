from dateutil import tz, parser
import functools
import json
from jsonpath_rw import jsonpath, parse
from wolf.mapconfig import WCSVMap, WCSVType
import time
import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker
import ssl
import threading
from urllib.parse import urlparse

class amqp_json():

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = None)
        self.host = config.get(self.name, 'host', fallback = '127.0.0.1')
        self.port = config.getint(self.name, 'port', fallback = 5672)
        self.retries = config.getint(self.name, 'retries', fallback = 3)
        self.timeout = config.getint(self.name, 'timeout', fallback = 60)
        self.username = config.get(self.name, 'username', fallback = None)
        self.password = config.get(self.name, 'password', fallback = None)
        self.vhost = config.get(self.name, 'vhost', fallback = '/')
        self.amqpurl = config.get(self.name, 'amqpurl', fallback = None)
        self.key = config.get(self.name, 'key')
        self.qos = config.getint(self.name, 'qos', fallback = 1)
        self.ssl = config.getboolean(self.name, 'ssl', fallback = False)
        self.channel = None

        csvfile = config.get(self.name, 'csvmap')
        csvmap = WCSVMap()
        self.mapping = csvmap.load(csvfile, WCSVType.JSON)

        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def on_message(self, channel, basic_deliver, properties, body):
        payload = body.decode("utf-8")
        logger.debug('Plugin %s broker %s routing key "%s" QoS %s message "%s"' % (self.name, self.host, self.key, self.qos, payload))
        channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)
        try:
            data = json.loads(payload)
        except ValueError:
            logger.warn('Plugin %s received empty or invalid message on %s routing key %s QoS %s' % (self.name, self.host, self.key, payload))
            return

        data = json.loads(payload)
        measures = {}
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, scale, offset, jsonpath) = row
            scale = float(scale)
            offset = float(offset)
            value = None
            path = parse(jsonpath)
            try:
                value = [match.value for match in path.find(data)][0]
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
            if datatype != 'b' and datatype != 's':
                value = value * scale + offset
            measures[name] = value
            logger.debug('AMQP broker: %s routing key: %s QoS: %s measure: %s value: %s %s' % (self.host, self.key, self.qos, name, value, unit))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        cache.store(data)

    def run(self):
        ssl_options = None

        if self.ssl:
            context = ssl.create_default_context()
            ssl_options = pika.SSLOptions(context, self.host)

        if self.amqpurl:
            parsed = urlparse(self.amqpurl)
            self.host = parsed.hostname
            replaced = parsed._replace(netloc="{}:{}@{}".format(parsed.username, "*" * 8, parsed.hostname))
            logger.info("AMQP URL %s" % replaced.geturl())
            parameters = pika.URLParameters(self.amqpurl)
        else:
            logger.info("AMQP broker %s virtual host %s" % (self.host, self.vhost))
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(host=self.host, port=self.port, connection_attempts=self.retries, socket_timeout=self.timeout, virtual_host=self.vhost, credentials=credentials, ssl_options=ssl_options)
        try:
            self.connection = pika.BlockingConnection(parameters)
        except (AMQPConnectionError, ConnectionResetError, ssl.SSLError) as e:
            logger.error("AMQP broker %s %s" % (self.host, e))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='queue')

        try:
            self.channel.exchange_declare(exchange=self.key, exchange_type='direct', passive=False, durable=True, auto_delete=False)
            self.channel.queue_declare(queue='standard', auto_delete=True)
            self.channel.queue_bind(queue='standard', exchange=self.key, routing_key=self.key)
            self.channel.basic_qos(prefetch_count=self.qos)
            self._consumer_tag = self.channel.basic_consume('standard', self.on_message)
        except (ChannelClosedByBroker, TypeError) as e:
            logger.error("AMQP broker %s %s" % (self.host, e))
            return

        logger.debug('Routing key "%s" QoS %d' % (self.key, self.qos))
        thread = threading.Thread(target=self.channel.start_consuming, name=self.name)
        thread.start()

    def stop(self):
        if self.channel:
            try:
                self.channel.stop_consuming()
                self.connection.close()
            # temporary workaround for pika (seem to be incompatible with threading)
            except:
                pass
            logger.info("Disconnected from AMQP broker %s:%d" % (self.host, self.port))
