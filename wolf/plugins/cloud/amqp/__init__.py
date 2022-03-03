import datetime
from dateutil import tz, parser
import json
import time
import os
import pika
import ssl
from urllib.parse import urlparse

class amqp():

    def __init__(self, name):
        self.name = name
        self.host = config.get(self.name, 'host', fallback = '127.0.0.1')
        self.port = config.getint(self.name, 'port', fallback = 5672)
        self.retries = config.getint(self.name, 'retries', fallback = 3)
        self.timeout = config.getint(self.name, 'timeout', fallback = 60)
        self.username = config.get(self.name, 'username', fallback = None)
        self.password = config.get(self.name, 'password', fallback = None)
        self.vhost = config.get(self.name, 'vhost', fallback = '/')
        self.amqpurl = config.get(self.name, 'amqpurl', fallback = None)
        self.key = config.get(self.name, 'key', fallback = None)
        self.ssl = config.getboolean(self.name, 'ssl', fallback = False)

        ssl_options = None

        if self.ssl:
            context = ssl.create_default_context()
            ssl_options = pika.SSLOptions(context, self.host)

        if self.amqpurl:
            parsed = urlparse(self.amqpurl)
            replaced = parsed._replace(netloc="{}:{}@{}".format(parsed.username, "********", parsed.hostname))
            logger.info("AMQP URL %s" % replaced.geturl())
            parameters = pika.URLParameters(self.amqpurl)
        else:
            logger.info("AMQP broker %s virtual host %s" % (self.host, self.vhost))
            credentials = pika.PlainCredentials(self.username, self.password)
            parameters = pika.ConnectionParameters(host=self.host, port=self.port, connection_attempts=self.retries, socket_timeout=self.timeout, virtual_host=self.vhost, credentials=credentials, ssl_options=ssl_options)

        try:
            self.connection = pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("AMQP broker %s %s" % (self.host, e))
        except ConnectionResetError as e:
            logger.error("AMQP broker %s %s" % (self.host, e))
        except ssl.SSLError as e:
            logger.error("AMQP broker %s %s" % (self.host, e))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.key)

    def post(self, rawdata):
        key = '%s/measures' % self.key
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
        self.channel.basic_publish(exchange='', routing_key=key, body=json.dumps(data))
        logger.debug("AMQP routing key: %s message: %s" % (key, json.dumps(data)))

    def post_config(self):
        key = '%s/config' % self.key
        data = cache.load_meta()
        self.channel.basic_publish(exchange='', routing_key=key, body=json.dumps(data))
        logger.debug("AMQP routing key: %s message: %s" % (key, json.dumps(data)))
