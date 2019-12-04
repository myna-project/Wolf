#!/usr/bin/env python3
""" IEnergyDa Wolf IoT gateway

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
__title__       = 'IEnergyDa Wolf'
__uri__         = 'https://github.com/myna-project/Wolf'
__version__     = 'v1.4.0'

import configparser
from datetime import datetime
import json
import logging
import os
import msgpack
import pkgutil
import redis
import schedule
import signal
import threading
import time
from wolf.webconfig import WWebConfig
from wolf.webmeasures import WWebMeasures
from wolf.webupdate import WWebUpdate

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
        self.host = 'localhost'
        self.port = 6379
        self.db = 0
        self.password = None
        self.expire = 0
        self.setup()

    def setup(self):
        if 'redis' in config.sections():
            self.host = config.get('redis', 'host', fallback = self.host)
            self.port = config.getint('redis', 'port', fallback = self.port)
            self.db = config.getint('redis', 'db', fallback = self.db)
            self.password = config.get('redis', 'password', fallback = self.password)
            self.expire = config.getint('redis', 'expire', fallback = self.expire)
        logger.debug('Redis connection pool: %s:%d db %d' % (self.host, self.port, self.db))
        if self.expire:
            logger.info('%s cache expire %s days' % (__title__, self.expire))
        self.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db, password=self.password)
        self.meta = []
        self.queues = []
        self.__ping()

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
        r = redis.Redis(connection_pool=self.pool, decode_responses=True)
        p = r.pipeline()
        p.multi()
        key = 'data:%s:%s:%s' % (data['client_id'], data['device_id'], int(data['ts']))
        p.set(key, msgpack.packb(data))
        if self.expire:
            p.expire(key, self.expire * 86400)
        for queue in self.queues:
            p.sadd('queue:%s' % queue, key)
        p.execute()

    def load(self, queue, callback):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool, decode_responses=True)
        for key in r.smembers('queue:%s' % queue):
            p = r.pipeline()
            p.watch('queue:*')
            p.multi()
            if not r.keys(key):
                logger.error('Expected cache key %s not found' % key)
                p.srem('queue:%s' % queue, key)
                p.execute()
                continue
            data = msgpack.unpackb(r.get(key), encoding = 'utf-8')
            if callback(data):
                # pop from current queue
                p.srem('queue:%s' % queue, key)
                # orphaned keys check and delete in other queues
                ism = False
                for kueue in self.queues:
                    if kueue == queue:
                        continue
                    ism |= r.sismember('queue:%s' % kueue, key)
                if not ism:
                    p.delete(key)
                p.execute()
            else:
                p.reset()

    def save(self):
        self.__ping()
        r = redis.Redis(connection_pool=self.pool, decode_responses=True)
        r.bgsave()

    def load_meta(self, **kwargs):
        self.__ping()
        self.meta = []
        r = redis.Redis(connection_pool=self.pool, decode_responses=True)
        keys = r.keys('meta:*')
        for key in keys:
            self.meta.append(msgpack.unpackb(r.get(key), encoding = 'utf-8'))
        meta = self.meta
        client_id = kwargs.get('client_id')
        device_id = kwargs.get('device_id')
        measure_id = kwargs.get('measure_id')
        if client_id:
            meta = list(filter(lambda item: item['client_id'] == client_id, meta))
        if device_id:
            meta = list(filter(lambda item: item['device_id'] == device_id, meta))
        if measure_id:
            meta = list(filter(lambda item: item['measure_id'] == measure_id, meta))
        return meta

    def store_meta(self, device_id, plugin_id, device_descr, mapping):
        self.__ping()
        client_id = config.clientid
        meta = []
        for row in mapping:
            (measure_id, measure_descr, measure_unit, measure_type, *_) = row
            meta.append({'client_id': client_id, 'device_id': device_id, 'measure_id': measure_id, 'plugin_id': plugin_id, 'device_descr': device_descr, 'measure_descr': measure_descr, 'measure_unit': measure_unit, 'measure_type': measure_type})
        r = redis.Redis(connection_pool=self.pool, decode_responses=True)
        for row in meta:
            key = 'meta:%s:%s:%s' % (row['client_id'],  row['device_id'],  row['measure_id'])
            r.set(key, msgpack.packb(row))

class WConfig(configparser.ConfigParser):

    def __init__(self):
        self.loglevel = 'INFO'
        self.interval = 3600
        self.host = 'localhost'
        self.port = 6379
        self.db = 0
        self.expire = 0
        self.clientid = '1'
        self.netslave = 'http://localhost:8085'
        self.webaddr = '0.0.0.0'
        self.webport = 8080
        self.webroot = 'wolfui'
        super(WConfig, self).__init__()

    def read(self, filename):
        logger.info('%s reading configuration file %s' % (__title__, filename))
        if not os.path.isfile(filename):
            logger.error('Configuration file %s does not exists, using default settings' % filename)
        try:
            super(WConfig, self).read(filename)
        except (configparser.ParsingError, configparser.DuplicateSectionError, configparser.DuplicateOptionError) as e:
            logger.critical(e)
            os._exit(1)
        self.loglevel = self.get(__name__, 'loglevel', fallback = self.loglevel)
        self.interval = self.getint(__name__, 'interval', fallback = self.interval)
        self.clientid = self.get(__name__, 'clientid', fallback = self.clientid)
        self.netslave = self.get(__name__, 'netslave', fallback = self.netslave)
        self.webaddr = self.get(__name__, 'webaddr', fallback = self.webaddr)
        self.webport = self.getint(__name__, 'webport', fallback = self.webport)
        self.webroot = self.get(__name__, 'webroot', fallback = self.webroot)
        if 'redis' in self.sections():
            self.host = self.get('redis', 'host', fallback = self.host)
            self.port = self.getint('redis', 'port', fallback = self.port)
            self.db = self.getint('redis', 'db', fallback = self.db)
        logger.info('%s processing interval %s seconds' % (__title__, self.interval))
        logger.info('%s network configuration daemon at %s' % (__title__, self.netslave))
        logger.info('%s web configuration at %s:%d' % (__title__, self.webaddr, self.webport))

    def write(self, filename):
        logger.info('%s writing configuration file %s' % (__title__, filename))
        with open(filename, 'w') as file:
            super(WConfig, self).write(file)

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

class WApp():

    def __init__(self):
        self.__threads = {}
        self.__plugins_in = []
        self.__plugins_out = []
        self.setup()

    def setup(self):
        self.__wait_threads()

        for plugin in self.__plugins_in:
            self.__call_method(plugin, 'stop')
        self.__plugins_in = []
        self.__plugins_out = []

        path = os.path.join(os.getcwd(), __name__, 'plugins')
        logger.debug('%s plugins path %s' % (__title__, path))
        modules_in = pkgutil.iter_modules(path = [os.path.join(path, 'in')])
        modules_out = pkgutil.iter_modules(path = [os.path.join(path, 'out')])
        for loader, name, ispkg in modules_in:
            logger.info('Loading input plugin %s' % name)
            try:
                loaded_mod = __import__('wolf.plugins.in.' + name, fromlist=[name])
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
                        loaded_class.name = section
                        if config.getboolean(section, 'disabled', fallback = False):
                            logger.warning('%s administratively disabled' % section)
                            continue
                        try:
                            self.__plugins_in.append(loaded_class())
                        except (configparser.NoOptionError, FileNotFoundError, ValueError) as e:
                            logger.error('Plugin %s poorly configured: %s' % (section, str(e)))
                            logger.warning('Plugin %s disabled' % section)
            else:
                logger.debug('Section %s not found in configuration' % name)
                logger.info('Plugin %s disabled: not configured' % name)
        cache.queues = []
        for loader, name, ispkg in modules_out:
            logger.info('Loading output plugin %s' % name)
            try:
                loaded_mod = __import__('wolf.plugins.out.' + name, fromlist=[name])
                loaded_mod.cache = cache
                loaded_mod.config = config
                loaded_mod.logger = logger
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
            loaded_class.name = name
            try:
                self.__plugins_out.append(loaded_class())
                cache.queues.append(name)
            except (configparser.NoOptionError, FileNotFoundError, ValueError) as e:
                logger.error('Plugin poorly configured: %s' % str(e))
                logger.warning('Plugin %s disabled' % name)
        if not len(self.__plugins_in):
            logger.critical('No input plugins enabled!')
        if not len(self.__plugins_out):
            logger.critical('No output plugins enabled!')
        schedule.clear()
        interval = config.interval
        if config.interval >= 60:
            interval = 60 * round(config.interval / 60) - 59
        schedule.every(interval).seconds.do(self.periodic)
        schedule.every().day.do(self.update)

    def update(self, force=False):
        update = WWebUpdate()
        if update.update(force=force):
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
                else:
                    method()

    def run(self):
        self.once()
        self.periodic()
        while True:
            schedule.run_pending()
            time.sleep(1)

    def once(self):
        for plugin in self.__plugins_out:
            self.__call_method(plugin, 'post_config', True)
        for plugin in self.__plugins_in:
            self.__call_method(plugin, 'run', True)

    def periodic(self):
        if not len(self.__plugins_in) or not len(self.__plugins_out):
            return
        self.__wait_minute()
        logger.debug('%s periodic threads running' % __title__)
        self.__threads = {}
        for plugin in self.__plugins_in:
            self.__threads[plugin] = IWThread(plugin)
            self.__threads[plugin].start()
        for plugin in self.__plugins_out:
            self.__threads[plugin] = OWThread(plugin)
            self.__threads[plugin].start()
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

class IWThread(threading.Thread):

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

class OWThread(threading.Thread):

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
    config.read(os.path.join(config.path, '%s.ini' % __name__))
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

    global app, cache, config, logger, webm

    # Logging setup
    logging.setLoggerClass(WLogger)
    logger = logging.getLogger(__name__)
    logger.info('%s starting up as %s' % (__title__, __name__))

    # Check and read config file
    config = WConfig()
    config.path = os.path.join(os.getcwd(), 'config')
    config.read(os.path.join(config.path, '%s.ini' % __name__))

    # Logging level setup
    logger.setup()

    # Redis db backend
    cache = WCache()

    app = WApp()
    webconfig = WWebConfig()
    webconfig.run()
    webm = WWebMeasures()
    app.run()

if __name__== "__main__":
    main()
