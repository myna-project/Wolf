#!/usr/bin/env python3
""" IEnergyDa Wolf IoT gateway - network configuration module

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
__title__       = 'IEnergyDa Wolf netconfig'
__uri__         = 'https://github.com/myna-project/Wolf'
__version__     = 'v1.4.0'

from bottle import app, hook, request, response, route, run
import configparser
from datetime import datetime
import dbus
import json
import logging
import netifaces
import os
import signal
import sys
from uritools import urisplit
import wolf
from wolf.configparsermulti import ConfigParserMultiOpt

NETCONFIG = '/etc/systemd/network/wired.network'

class WLogger(logging.Logger):

    def __init__(self, name, level=logging.INFO):
        FORMAT = '%(asctime)-s %(name)s %(threadName)s %(levelname)s %(message)s'
        logging.basicConfig(format=FORMAT)
        super(WLogger, self).__init__(name, level)

    def setup(self):
        levels = {'CRITICAL': logging.CRITICAL, 'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        level = levels.setdefault(config.loglevel, self.level)
        self.setLevel(level)
        logger.debug('%s logging level set to %s' % (__title__, config.loglevel))

class WConfig(ConfigParserMultiOpt):

    def __init__(self):
        self.loglevel = 'INFO'
        self.strict=False
        super(WConfig, self).__init__()

    def read(self, filename):
        # workaround to clear whole configuration and read it again
        self._defaults = {}
        self._sections = {}
        logger.info('%s reading configuration file %s' % (__name__, filename))
        if not os.path.isfile(filename):
            logger.error('Configuration file %s does not exists, using default settings' % filename)
        try:
            super(WConfig, self).read(filename)
        except configparser.ParsingError as e:
            logger.critical(e)
            os._exit(1)
        self.loglevel = self.get(__package__, 'loglevel', fallback = self.loglevel)

    def write(self, filename):
        logger.info('%s writing configuration file %s' % (__name__, filename))
        with open(filename, 'w') as file:
            super(WConfig, self).write(file)

    def json(self, section=None):
        sections = self._sections
        data = {}
        for cursect in sections:
            items = sections[cursect]
            for key in items:
                if isinstance(items[key], tuple):
                    items[key] = list(items[key])
            data[cursect] = items
        if section:
            data = data[section]
        return json.dumps(data)

class AccessLog(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, e, h):
        ret = self.app(e, h)
        logger.info('%s %s - [%s] "%s %s %s" %s %d' % (request.remote_addr, request.environ.get('REMOTE_USER', '-'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request.environ.get('REQUEST_METHOD'), request.path,
            request.environ.get('SERVER_PROTOCOL'), response.status_code, response.content_length))
        return ret

@hook('after_request')
def common_headers():
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'
    response.headers['Content-Type'] = 'application/json'
    response.headers['Cache-Control'] = 'no-cache'

@route ('/<:re:.*>', method='OPTIONS')
def options():
    response.status = 202;
    return;

@route('/config', method='GET')
def get_config():
    netconfig.read(NETCONFIG)
    return netconfig.json()

@route('/interfaces', method='GET')
def get_interfaces():
    return json.dumps(netifaces.interfaces())

@route('/gateways', method='GET')
def get_gateways():
    return json.dumps(netifaces.gateways())

@route('/ifaddresses/<iface>', method='GET')
def get_ifaddresses(iface):
    if iface in netifaces.interfaces():
        return json.dumps(netifaces.ifaddresses(iface))
    else:
        response.status = 404

@route('/config', method='POST')
def post_config():
    try:
        data = request.json
    except:
        response.status = 400
        return
    if data is None:
        response.status = 400
        return
    for section in netconfig.sections():
        if section in data:
            netconfig.remove_section(section)
            netconfig.add_section(section)
            for key in data[section].keys():
                if isinstance(data[section][key], list):
                    netconfig.set(section, key, tuple(data[section][key]))
                else:
                    netconfig.set(section, key, data[section][key])
        else:
            response.status = 409
            return
    netconfig.write(NETCONFIG)
    response.status = 201

    sysbus = dbus.SystemBus()
    systemd1 = sysbus.get_object('org.freedesktop.systemd1', '/org/freedesktop/systemd1')
    manager = dbus.Interface(systemd1, 'org.freedesktop.systemd1.Manager')
    logger.info('%s restarting %s' % (__name__, 'systemd-networkd.service'))
    job = manager.RestartUnit('systemd-networkd.service', 'fail')
    logger.info('%s restarting %s' % (__name__, 'systemd-resolved.service'))
    job = manager.RestartUnit('systemd-resolved.service', 'fail')
    logger.info('%s restarting %s' % (__name__, 'systemd-timesyncd.service'))
    job = manager.RestartUnit('systemd-timesyncd.service', 'fail')

    return netconfig.json()


# Signals handlers

def sighup(signum, frame):
    logger.info("SIGHUP received, reloading configuration and plugins")
    netconfig.read(NETCONFIG)
    config.read(os.path.join(config.path, '%s.ini' % __package__))
    logger.setup()

def terminate(signum=None, frame=None):
    logger.info("Exiting.")
    os._exit(0)

signal.signal(signal.SIGHUP, sighup)
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

# main app

def main():
    global config, logger, netconfig

    # Logging setup
    logging.setLoggerClass(WLogger)
    logger = logging.getLogger(__name__)
    logger.info('%s starting up as %s' % (__title__, __name__))

    # Check for root privileges
    if os.geteuid() != 0:
        logger.critical('%s requires root privileges' % __name__)
        os._exit(1)

    # Check and read wolf config file and systemd networking config
    netconfig = WConfig()
    netconfig.read(NETCONFIG)

    config = WConfig()
    config.read(os.path.join(config.path, '%s.ini' % __package__))

    wolf.config = config
    wolf.logger = logger

    # Logging level setup
    logger.setup()

    netslave = urisplit(config.get(__package__, 'netslave', fallback='http://localhost:8085'))
    logger.debug('Listening on %s:%s' % (netslave.host, netslave.port))
    run(host = netslave.host, port = netslave.port, quiet=True, app=AccessLog(app()))
