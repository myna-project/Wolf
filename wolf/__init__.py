#!/usr/bin/env python3
""" Wolf IoT gateway

Wolf is a lightweight and modular IoT gateway written in python. Wolf receives
or retrieves data, depending on the protocol used, from various industrial
electronic devices via its input plugins and then stores the data in Influxdb
DB and/or sends it to other Information Systems through the output plugins.
Wolf uses Redis to implement a persistent caching; it also exposes REST
services to update the configuration or retrieve the data stored in InfluxDB,
these REST services are used by WolfUI to provide the web based UI.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__      = 'Myna-Project.org Srl'
__contact__     = 'info@myna-project.org'
__copyright__   = 'Copyright 2020, Myna-Project.org Srl'
__credits__     = 'Myna-Project.org Srl'
__email__       = 'info@myna-project.org'
__license__     = 'GPLv3'
__status__      = 'Production'
__summary__     = 'Wolf is a lightweight and modular IoT gateway.'
__title__       = 'Wolf'
__uri__         = 'https://github.com/myna-project/Wolf'
__version__     = 'v1.6.1'

import configparser
from datetime import datetime
import json
import logging
import os
import msgpack
from pathlib import Path
import pkgutil
import redis
import schedule
import signal
import threading
import time
from wolf.webupdate import WWebUpdate

try:
    import queue
except ImportError:
    import Queue as queue

toupdate = False
try:
    from wolf.webconfig import WWebConfig
    import wolf.webmeasures
    from influxdb import InfluxDBClient, resultset
    from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError
except (ImportError, ModuleNotFoundError) as e:
    toupdate = True

class WLogger(logging.Logger):

    def __init__(self, name, level=logging.INFO):
        FORMAT = '%(asctime)-s %(name)s %(threadName)s %(levelname)s %(message)s'
        logging.basicConfig(format=FORMAT)
        super(WLogger, self).__init__(name, level)

    def setup(self):
        levels = {'CRITICAL': logging.CRITICAL, 'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        level = levels.setdefault(config.loglevel, self.level)
        loglevel = config.loglevel
        self.setLevel(level)
        logger.debug('%s logging level set to %s' % (__title__, config.loglevel))
        try:
            import http.client as http_client
        except ImportError:
            import httplib as http_client
        if level == logging.DEBUG:
            http_client.HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(level)
        requests_log.propagate = True
        requests_log = logging.getLogger("pymodbus.client.sync")
        requests_log.setLevel(levels[loglevel])
        requests_log.propagate = True
        requests_log = logging.getLogger("pymodbus.transaction")
        requests_log.setLevel(levels[loglevel])
        requests_log.propagate = True
        requests_log = logging.getLogger("pymodbus.framer.socket_framer")
        requests_log.setLevel(levels[loglevel])
        requests_log.propagate = True

class WCache():

    def __init__(self):
        self.defaults = {'host': 'localhost', 'port': 6379, 'db': 0, 'password': None, 'expire': 0}
        for k, v in self.defaults.items():
            setattr(self, k, v)
        self.setup()

    def setup(self):
        self.client_id = config.clientid
        self.client_descr = config.description
        section = 'redis'
        if section in config.sections():
            self.host = config.get(section, 'host', fallback = self.host)
            self.port = config.getint(section, 'port', fallback = self.port)
            self.db = config.getint(section, 'db', fallback = self.db)
            self.password = config.get(section, 'password', fallback = self.password)
            self.expire = config.getint(section, 'expire', fallback = self.expire)
        logger.debug('Redis connection pool: %s:%d db %d' % (self.host, self.port, self.db))
        if self.expire:
            logger.info('%s cache expire %s days' % (__title__, self.expire))
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db, password=self.password)
        self.meta = {}
        self.queues = []
        self.__store_client()

    def __store_client(self):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        client_id = self.client_id
        client_descr = self.client_descr
        key = 'clients:%s' % client_id
        data = msgpack.packb({'client_id': client_id, 'client_descr': client_descr})
        r.set(key, data)

    def clients(self, **kwargs):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        clients = []
        keys = r.keys('clients:*')
        for key in keys:
            clients.append(msgpack.unpackb(r.get(key)))
        client_id = kwargs.get('client_id')
        if client_id:
            clients = list(filter(lambda item: item.get('client_id') == client_id, clients))[0]
        return clients

    def __ping(self):
        r = redis.Redis(connection_pool=self.pool)
        try:
            r.ping()
        except redis.exceptions.ConnectionError:
            logger.critical('Redis server unreachable!')
            os._exit(2)
        except redis.exceptions.ResponseError:
            logger.critical('Redis server error, check redis configuration!')
            os._exit(3)

    def store(self, data):
        if not data:
            return
        self.__ping()
        client_id = self.client_id
        r = redis.Redis(connection_pool=self.pool)
        p = r.pipeline()
        p.multi()
        key = 'data:%s:%s:%s' % (data.get('client_id'), data.get('device_id'), int(data.get('ts')))
        value = r.get(key)
        if value:
            last = msgpack.unpackb(value)
            data['measures'] = {**data.get('measures'), **last.get('measures')}
        p.set(key, msgpack.packb(data))
        if self.expire:
            p.expire(key, self.expire * 86400)
        for queue in self.queues:
            p.sadd('queue:%s:%s' % (client_id, queue), key)
        key = 'data:%s:%s:last' % (data.get('client_id'), data.get('device_id'))
        p.set(key, msgpack.packb(data))
        p.execute()

    def load_last(self, client_id, device_id):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        key = 'data:%s:%s:last' % (client_id, device_id)
        value = r.get(key)
        if not value:
            return {}
        return msgpack.unpackb(value)

    def store_last(self, data):
        if not data:
            return
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        key = 'data:%s:%s:last' % (data.get('client_id'), data.get('device_id'))
        r.set(key, msgpack.packb(data))

    def load(self, queue, callback):
        self.__ping()
        client_id = self.client_id
        r = redis.Redis(connection_pool=self.pool)
        for key in r.smembers('queue:%s:%s' % (client_id, queue)):
            p = r.pipeline()
            p.watch('queue:%s:*' % client_id)
            p.multi()
            if not r.keys(key):
                logger.error('Expected cache key %s not found' % key)
                p.srem('queue:%s:%s' % (client_id, queue), key)
                p.execute()
                continue
            data = msgpack.unpackb(r.get(key))
            if callback(data):
                # pop from current queue
                p.srem('queue:%s:%s' % (client_id, queue), key)
                # orphaned keys check and delete in other queues
                ism = False
                for kueue in self.queues:
                    if kueue == queue:
                        continue
                    ism |= r.sismember('queue:%s:%s' % (client_id, kueue), key)
                if not ism:
                    p.delete(key)
                p.execute()
            else:
                p.reset()

    def save(self):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        r.bgsave()

    def load_meta(self, **kwargs):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool)
        keys = r.keys('meta:*')
        for key in keys:
            self.meta[tuple(key.decode().split(':')[1:])] = msgpack.unpackb(r.get(key))
        meta = list(self.meta.values())
        client_id = kwargs.get('client_id')
        device_id = kwargs.get('device_id')
        measure_id = kwargs.get('measure_id')
        read_write = kwargs.get('read_write')
        if client_id:
            meta = list(filter(lambda item: item.get('client_id') == client_id, meta))
        if device_id:
            meta = list(filter(lambda item: item.get('device_id') == device_id, meta))
        if measure_id:
            meta = list(filter(lambda item: item.get('measure_id') == measure_id, meta))
        if read_write:
            meta = list(filter(lambda item: item.get('read_write') == read_write, meta))
        return meta

    def store_meta(self, device_id, plugin_id, device_descr, mapping):
        if not mapping:
            return
        self.__ping()
        client_id = self.client_id
        meta = {}
        for row in mapping:
            (measure_id, measure_descr, measure_unit, measure_type, read_write, *_) = row
            meta[(client_id, device_id, measure_id)] = {'client_id': client_id, 
            'device_id': device_id, 'device_descr': device_descr, 'plugin_id': plugin_id, 
            'measure_id': measure_id, 'measure_descr': measure_descr, 'measure_unit': measure_unit, 
            'measure_type': measure_type, 'read_write': bool(read_write)}
        r = redis.Redis(connection_pool=self.pool)
        p = r.pipeline()
        p.multi()
        for row in meta.values():
            key = 'meta:%s:%s:%s' % (row.get('client_id'), row.get('device_id'), row.get('measure_id'))
            p.set(key, msgpack.packb(row))
        p.execute()
        self.meta.update(meta)

class WConfig(configparser.ConfigParser):

    def __init__(self):
        self.defaults = {'loglevel': 'INFO', 'interval': 3600, 'clientid': 1, 'description': '',
            'webaddr': '0.0.0.0', 'webport': 8888, 'webroot': 'wolfui',
            'username': 'admin', 'password': 'ISMvKXpXpadDiUoOSoAfww==', 'cors': '*'}
        for k, v in self.defaults.items():
            setattr(self, k, v)
        self.installed = True
        super(WConfig, self).__init__()

    def read(self, filename):
        logger.info('%s reading configuration file %s' % (__title__, filename))
        if not os.path.isfile(filename):
            logger.error('Configuration file %s does not exists, using default settings' % filename)
            self.create()
        try:
            super(WConfig, self).read(filename)
        except (configparser.ParsingError, configparser.DuplicateSectionError, configparser.DuplicateOptionError) as e:
            logger.critical(e)
            os._exit(1)
        self.loglevel = self.get(__name__, 'loglevel', fallback = self.loglevel)
        self.interval = self.getint(__name__, 'interval', fallback = self.interval)
        self.clientid = self.get(__name__, 'clientid', fallback = self.clientid)
        self.description = self.get(__name__, 'description', fallback = self.description)
        self.webaddr = self.get(__name__, 'webaddr', fallback = self.webaddr)
        self.webport = self.getint(__name__, 'webport', fallback = self.webport)
        self.webroot = self.get(__name__, 'webroot', fallback = self.webroot)
        self.cors = self.get(__name__, 'cors', fallback = self.cors)
        logger.info('%s processing interval %s seconds' % (__title__, self.interval))
        logger.info('%s web configuration at %s:%d' % (__title__, self.webaddr, self.webport))

    def write(self, filename):
        logger.info('%s writing configuration file %s' % (__title__, filename))
        with open(filename, 'w') as file:
            super(WConfig, self).write(file)

    def create(self):
        self.installed = False
        self.add_section(__name__)
        for key in self.defaults:
            self.set(__name__, key, str(self.defaults[key]))

    def json(self, section=None):
        data = self._sections
        if section:
            data = data[section]
        return json.dumps(data)

    def getenum(self, section, option, *, raw=False, vars=None, enum={}, fallback=object(), **kwargs):
        value = self.get(section, option, raw=raw, vars=vars, fallback=fallback, **kwargs)
        try:
            return enum[value]
        except KeyError as e:
            sep = ', '
            if hasattr(enum, '__members__'):
                valid = sep.join(enum.__members__.keys())
            else:
                valid = sep.join(enum.keys())
            raise ValueError('%s invalid value for %s; valid options are: %s' % (str(e), option, valid))

    def parse(self, section, params):
        items = {}
        for item in params:
            param = type('new', (object,), item)
            if not hasattr(param, 'name'):
                raise ValueError('Wrong plugin default configuration: missing name')
            if not hasattr(param, 'type'):
                raise ValueError('Wrong plugin default configuration: missing type for %s' % param.name)
            if (param.type not in ['string', 'int', 'float', 'boolean', 'enum']):
                raise ValueError('Wrong plugin default configuration: wrong type for %s' % param.name)
            if (param.type == 'string'):
                if hasattr(param, 'default'):
                    value = self.get(section, param.name, fallback = param.default)
                else:
                    value = self.get(section, param.name)
            if (param.type == 'int'):
                if hasattr(param, 'default'):
                    value = self.getint(section, param.name, fallback = param.default)
                else:
                    value = self.getint(section, param.name)
            if (param.type == 'float'):
                if hasattr(param, 'default'):
                    value = self.getfloat(section, param.name, fallback = param.default)
                else:
                    value = self.getfloat(section, param.name)
            if (param.type == 'boolean'):
                if hasattr(param, 'default'):
                    value = self.getboolean(section, param.name, fallback = param.default)
                else:
                    value = self.getboolean(section, param.name)
            if (param.type == 'enum'):
                if not hasattr(param, 'enum'):
                    raise ValueError('Wrong plugin default configuration: missing enum for %s' % param.name)
                if hasattr(param, 'default'):
                    value = self.getenum(section, param.name, fallback = param.default, enum = param.enum)
                else:
                    value = self.getenum(section, param.name, enum = param.enum)
            items[param.name] = value
        return items

class WInfluxDB():

    def __init__(self):
        self.setup()

    def setup(self):
        self.configured = False
        section = 'influxdb'
        if section not in config.sections():
            logger.info('No InfluxDB configured: alerting and webUI graph/export of measures not available.')
            return
        self.host = config.get(section, 'host', fallback = 'localhost')
        self.port = config.getint(section, 'port', fallback = '8086')
        self.db = config.get(section, 'db', fallback = 'wolf')
        self.username = config.get(section, 'username', fallback = 'root')
        self.password = config.get(section, 'password', fallback = 'root')
        self.ssl = config.getboolean(section, 'ssl', fallback = False)

        self.client = InfluxDBClient(host=self.host, port=self.port, username=self.username, password=self.password, ssl=self.ssl)
        logger.info('InfluxDB connection: %s:%d db "%s"' % (self.host, self.port, self.db))
        self.client.switch_database(self.db)
        self.configured = True

    def query(self, query):
        if not self.configured:
            return []
        try:
            logger.debug('InfluxDB host %s:%d query: %s' % (self.host, self.port, query))
            results = self.client.query(query)
        except (ConnectionError, InfluxDBClientError, InfluxDBServerError) as e:
            logger.error('InfluxDB host %s:%d cannot read: %s' % (self.host, self.port, str(e)))
            return []
        return results

    def write_points(self, points, **kwargs):
        try:
            self.client.write_points(points, **kwargs)
        except (ConnectionError, InfluxDBClientError, InfluxDBServerError) as e:
            logger.error('InfluxDB host %s:%d cannot write: %s' % (self.host, self.port, str(e)))
            return False
        return True


class WApp():

    def __init__(self):
        self.__threads = {}
        self.__plugins_field = {}
        self.__plugins_cloud = {}
        self.queue = queue.Queue()
        self.setup()

    def setup(self):
        if not config.installed:
            self.update(True)

        self.__wait_threads()

        for plugin in self.__plugins_field:
            self.__call_method(plugin, 'stop')
        for plugin in self.__plugins_cloud:
            self.__call_method(plugin, 'stop')
        self.__plugins_field = {}
        self.__plugins_cloud = {}

        path = os.path.join(config.path, __name__, 'plugins')
        logger.debug('%s plugins path %s' % (__title__, path))
        modules_field = pkgutil.iter_modules(path = [os.path.join(path, 'field')])
        modules_cloud = pkgutil.iter_modules(path = [os.path.join(path, 'cloud')])
        for loader, name, ispkg in modules_field:
            logger.info('Loading field plugin %s' % name)
            try:
                loaded_mod = __import__('wolf.plugins.field.%s' % name, fromlist=[name])
                loaded_mod.cache = cache
                loaded_mod.config = config
                loaded_mod.logger = logger
            except SyntaxError as e:
                logger.warning('Plugin %s disabled: %s' % (name, str(e)))
                continue
            if [s for s in config.sections() if s.startswith('%s.' % name)]:
                for section in config.sections():
                    if section.find('%s.' % name):
                        continue
                    else:
                        logger.debug('Section %s found in configuration' % section)
                        loaded_class = getattr(loaded_mod, name)
                        if config.getboolean(section, 'disabled', fallback = False):
                            logger.warning('%s administratively disabled' % section)
                            continue
                        try:
                            self.__plugins_field[section] = loaded_class(section)
                        except (configparser.NoOptionError, FileNotFoundError, ValueError) as e:
                            logger.error('Plugin %s poorly configured: %s' % (section, str(e)))
                            logger.warning('Plugin %s disabled' % section)
            else:
                logger.debug('Section %s not found in configuration' % name)
                logger.info('Plugin %s disabled: not configured' % name)
        cache.queues = []
        for loader, name, ispkg in modules_cloud:
            logger.info('Loading cloud plugin %s' % name)
            try:
                loaded_mod = __import__('wolf.plugins.cloud.%s' % name, fromlist=[name])
                loaded_mod.cache = cache
                loaded_mod.config = config
                loaded_mod.influx = influx
                loaded_mod.logger = logger
                loaded_mod.queue = self.queue
            except SyntaxError as e:
                logger.warning('Plugin %s disabled: %s' % (name, str(e)))
                continue
            if name in config.sections():
                logger.debug('Section %s found in configuration' % name)
            else:
                logger.debug('Section %s not found in configuration' % name)
                logger.info('Plugin %s disabled: not configured' % name)
                continue
            loaded_class = getattr(loaded_mod, name)
            try:
                self.__plugins_cloud[name] = loaded_class(name)
                cache.queues.append(name)
            except (configparser.NoOptionError, FileNotFoundError, ValueError) as e:
                logger.error('Plugin poorly configured: %s' % str(e))
                logger.warning('Plugin %s disabled' % name)
        if not len(self.__plugins_field):
            logger.critical('No field plugins enabled!')
        if not len(self.__plugins_cloud):
            logger.critical('No cloud plugins enabled!')
        schedule.clear()
        interval = config.interval
        if config.interval >= 60:
            interval = 60 * round(config.interval / 60) - 59
        schedule.every(interval).seconds.do(self.periodic)
        schedule.every().day.do(self.update)

    def update(self, force=False):
        update = WWebUpdate()
        if update.update(force=force):
            configfile = os.path.join(config.path, 'config', '%s.ini' % __package__)
            os.rename(configfile, '%s.bak' % configfile)
            config.write(configfile)
            logger.info('%s updated, exiting and waiting systemd restart...' % __title__)
            self.__wait_threads()
            os._exit(0)

    def __call_method(self, plugin, func, threaded = False):
        if hasattr(plugin, func):
            method = getattr(plugin, func)
            if callable(method):
                if threaded:
                    thread = threading.Thread(target=method, name=plugin.name)
                    thread.start()
                    return thread
                else:
                    return method()

    def run(self):
        thread = threading.Thread(target=self.worker, name='worker')
        thread.start()
        self.once()
        self.periodic()
        while True:
            schedule.run_pending()
            time.sleep(1)

    def once(self):
        for plugin_id in self.__plugins_cloud:
            plugin = self.__plugins_cloud[plugin_id]
            self.__call_method(plugin, 'post_config', True)
        for plugin_id in self.__plugins_field:
            plugin = self.__plugins_field[plugin_id]
            self.__call_method(plugin, 'run', True)

    def periodic(self):
        if not len(self.__plugins_field) or not len(self.__plugins_cloud):
            return
        self.__wait_minute()
        logger.debug('%s periodic threads running' % __title__)
        self.__threads = {}
        for plugin_id in self.__plugins_field:
            plugin = self.__plugins_field[plugin_id]
            self.__threads[plugin_id] = WFieldThread(plugin)
            self.__threads[plugin_id].start()
        for plugin_id in self.__plugins_cloud:
            plugin = self.__plugins_cloud[plugin_id]
            self.__threads[plugin_id] = WCloudThread(plugin)
            self.__threads[plugin_id].start()
        self.__wait_threads()
        logger.debug('%s periodic threads ended' % __title__)

    def __check_dir(self, directory):
        if not os.path.exists(directory):
            logger.warning('Directory %s does not exist' % directory)
            return False
        if not os.access(directory, os.R_OK | os.W_OK):
            logger.warning('Directory %s is not readable and writable by uid/gid %d:%d' % (directory, os.getuid(), os.getgid()))
            return False
        return True

    def __wait_threads(self):
        for thread in self.__threads:
            self.__threads[thread].join()

    def __wait_minute(self):
        if config.interval < 60:
            return
        ut = time.time()
        time.sleep (60 - ut % 60)

    def worker(self):
        while True:
            data = self.queue.get()
            meta = cache.load_meta(client_id=data.get('client_id'), device_id=data.get('device_id'), measure_id=data.get('measure_id'))
            try:
                plugin_id = meta[0].get('plugin_id')
                measure_id = meta[0].get('measure_id')
                value = data.get('value')
                if plugin_id in self.__plugins_field:
                    plugin = self.__plugins_field[plugin_id]
                    if hasattr(plugin, 'write'):
                        plugin.write(measure_id, value)
                    if hasattr(plugin, 'poll'):
                        logger.debug('Plugin %s polling thread started' % plugin.name)
                        data = plugin.poll()
                        cache.store_last(data)
                        logger.debug('Plugin %s polling thread ended' % plugin.name)
            except KeyError as e:
                logger.error('Invalid message received, missing %s' % str(e))
            except IndexError:
                logger.warning('Invalid client_id=%s device_id=%s measure_id=%s' % (data.get('client_id'), data.get('device_id'), data.get('measure_id')))

class WFieldThread(threading.Thread):

    def __init__(self, plugin):
        threading.Thread.__init__(self)
        self.plugin = plugin
        self.name = plugin.name

    def run(self):
        plugin = self.plugin
        if not hasattr(plugin, 'poll'):
            return
        logger.debug('Plugin %s polling thread started' % plugin.name)
        data = plugin.poll()
        cache.store(data)
        logger.debug('Plugin %s polling thread ended' % plugin.name)

class WCloudThread(threading.Thread):

    def __init__(self, plugin):
        threading.Thread.__init__(self)
        self.plugin = plugin
        self.name = plugin.name

    def run(self):
        plugin = self.plugin
        logger.debug('Plugin %s thread started' % plugin.name)
        cache.load(plugin.name, plugin.post)
        logger.debug('Plugin %s thread ended' % plugin.name)

# Signals handlers

def sighup(signum, frame):
    logger.info("SIGHUP received, reloading configuration and plugins")
    config.read(os.path.join(config.path, 'config', '%s.ini' % __name__))
    logger.setup()
    cache.setup()
    app.setup()
    app.once()

def terminate(signum=None, frame=None):
    cache.save()
    logger.info("SIGTERM received, exiting.")
    os._exit(0)

signal.signal(signal.SIGHUP, sighup)
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

# main app

def main(argv=None):

    global app, cache, config, influx, logger

    # Logging setup
    logging.setLoggerClass(WLogger)
    logger = logging.getLogger(__name__)
    logger.info('%s starting up as %s' % (__title__, __name__))

    # Check and read config file
    config = WConfig()
    config.path = Path(__file__).parent.parent
    config.read(os.path.join(config.path, 'config', '%s.ini' % __name__))

    # Logging level setup
    logger.setup()

    # self install missing dependencies
    if toupdate:
        logger.warning('%s missing modules, starting self update...' % __title__)
        update = WWebUpdate()
        update.update(force=True)
        os._exit(0)

    # Redis db backend
    cache = WCache()

    # InfluxDB backend
    influx = WInfluxDB()

    app = WApp()
    webconfig = WWebConfig()
    webconfig.start()
    app.run()

if __name__== "__main__":
    main()
