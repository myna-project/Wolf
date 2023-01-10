#!/usr/bin/env python3
import base64
from bottle import app, auth_basic, error, hook, mount, redirect, request, response, route, run, static_file
import copy
from datetime import datetime
from dbus.exceptions import DBusException
from dbus.mainloop.glib import DBusGMainLoop
import glob
import hashlib
import json
from NetworkManager import *
import os
import pkgutil
import threading
import wolf
# ImportError: cannot import name 'journal' from 'systemd' (unknown location)
# from systemd import journal

prefix = 'config'
static = 'wolfui'

devTypes = {0: 'unknown', 1: 'ethernet', 2: 'wifi', 8: 'modem', 10: 'bond', 11: 'vlan', 13: 'bridge', 16: 'tun', 23: 'ppp'}
devStates = {0: 'unknown', 10: 'unmamaged', 20: 'unavailable', 30: 'disconnected', 40: 'prepare', 50: 'config', 60: 'need_auth', 70: 'ip_config', 80: 'ip_check', 100: 'activated', 120: 'failed'}

DBusGMainLoop(set_as_default=True)

def auth(user, password):
    section = wolf.__name__.split('.')[0]
    if user == wolf.config.get(section, 'username', fallback=None) and \
        base64.b64encode(hashlib.md5(password.encode()).digest()).decode() == wolf.config.get(section, 'password', fallback=None):
        return True
    return False

def content_json(fn):
    def _content_json(*args, **kwargs):
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['Content-Type'] = 'application/json'
        return fn(*args, **kwargs)
    return _content_json

class AccessLog(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, e, h):
        ret = self.app(e, h)
        try:
            content_length = response.content_length
        except:
            try:
                content_length = len(response.body)
            except:
                content_length = 0
        remote_user = '-'
        try:
            if (auth(request.auth[0], request.auth[1])):
                remote_user = request.auth[0]
        except:
            pass
        wolf.logger.info('%s %s - [%s] "%s %s %s" %d %d' % (request.remote_addr, remote_user,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request.environ.get('REQUEST_METHOD'),
            request.path, request.environ.get('SERVER_PROTOCOL'), response.status_code, content_length))
        return ret

class WWebConfig(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.name = 'webconfig'

    def run(self):
        try:
            wolf.logger.debug('Listening on %s:%s' % (wolf.config.webaddr, wolf.config.webport))
            run(host=wolf.config.webaddr, port=wolf.config.webport, quiet=True, app=AccessLog(app()))
        except OSError as e:
            wolf.logger.error('Cannot start %s: %s' % (self.name, str(e)))

    @hook('after_request')
    def common_headers():
        origin = request.headers.get('Origin', None)
        for allow in wolf.config.cors.split(','):
            if origin == allow.strip():
                response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Authorization, Content-Type, X-Requested-With, X-CSRF-Token'
        response.headers['Access-Control-Allow-Credentials'] = 'true'

    @route ('/<:re:.*>', method='OPTIONS')
    def options():
        response.status = 202;

    @route('/', method='GET')
    @route('/%s/' % static, method='GET')
    def home():
        redirect('/%s/index.html' % static, 301)

    @route('/favicon.ico', method='GET')
    def favicon():
        return static_file("icons/favicon.ico", root=wolf.config.webroot)

    @route('/authenticate', method='POST')
    @auth_basic(auth)
    @content_json
    def authenticate():
        return {'status': 'ok'}

    @route('/%s/<filepath:path>' % static, method='GET')
    def server_static(filepath):
        return static_file(filepath, root=wolf.config.webroot)

    @route('/%s/plugins' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def plugins_available():
        path = os.path.join(os.getcwd(), __package__, 'plugins')
        wolf.logger.debug('%s plugins path %s' % (__package__, path))
        modules_field = pkgutil.iter_modules(path = [os.path.join(path, 'field')])
        modules_cloud = pkgutil.iter_modules(path = [os.path.join(path, 'cloud')])
        plugins_field = []
        plugins_cloud = []
        for loader, name, ispkg in modules_field:
            plugins_field.append(name)
        for loader, name, ispkg in modules_cloud:
            plugins_cloud.append(name)
        return json.dumps({'plugins': [{'field': plugins_field}, {'cloud': plugins_cloud}]})

    @route('/%s/plugins/<plugintype>/<plugin>' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def plugin_defaults(plugintype, plugin):
        plugin = plugin.split('.')[0]
        try:
            loaded_mod = __import__('wolf.plugins.%s.%s' % (plugintype, plugin), fromlist=[plugin])
            loaded_class = getattr(loaded_mod, plugin)
            if not hasattr(loaded_class, 'params'):
                return json.dumps({})
            params = copy.deepcopy(loaded_class.params)
            for item in params:
                if 'enum' in item:
                    item['enum'] = list(item['enum'].keys())
            return json.dumps(params)
        except ModuleNotFoundError:
            return json.dumps({})

    @route('/%s/sections' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def config_redis():
        return json.dumps(wolf.config.sections())

    @route('/%s/sections/<section>' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def section_get(section):
        if section in wolf.config.sections():
            return wolf.config.json(section=section)
        else:
            response.status = 404

    @route('/%s/sections' % prefix, method='POST')
    @auth_basic(auth)
    @content_json
    def section_create():
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        max = -1
        for sect in wolf.config.sections():
            s = sect.split('.')
            if (s[0] == data['plugin']):
                if (int(s[1]) > max):
                    max = int(s[1])
        max = max + 1
        section = data['plugin'] + '.' + str(max)
        wolf.config.add_section(section)
        for key in data.keys():
            wolf.config.set(section, key, data[key])
        configfile = os.path.join(wolf.config.path, 'config', '%s.ini' % __package__)
        os.rename(configfile, '%s.bak' % configfile)
        wolf.config.write(configfile)
        wolf.sighup(None, None);
        response.status = 201
        return wolf.config.json(section=section)

    @route('/%s/sections/<section>' % prefix, method='PUT')
    @auth_basic(auth)
    @content_json
    def section_update(section):
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        if section not in wolf.config.sections():
            response.status = 404
            return
        wolf.config.remove_section(section)
        wolf.config.add_section(section)
        for key in data.keys():
            wolf.config.set(section, key, data[key])
        configfile = os.path.join(wolf.config.path, 'config', '%s.ini' % __package__)
        os.rename(configfile, '%s.bak' % configfile)
        wolf.config.write(configfile)
        wolf.sighup(None, None);
        return wolf.config.json(section=section)

    @route('/%s/sections/<section>' % prefix, method='DELETE')
    @auth_basic(auth)
    @content_json
    def section_delete(section):
        if section not in wolf.config.sections():
            response.status = 404
            return
        wolf.config.remove_section(section)
        configfile = os.path.join(wolf.config.path, 'config', '%s.ini' % __package__)
        os.rename(configfile, '%s.bak' % configfile)
        wolf.config.write(configfile)
        wolf.sighup(None, None);

    @route('/%s/maps/<filename>' % prefix, method='DELETE')
    @auth_basic(auth)
    @content_json
    def maps_delete(filename):
        try:
            os.unlink(os.path.join(wolf.config.path, 'config', filename))
            wolf.logger.debug('File "%s" deleted' % filename)
        except FileNotFoundError:
            response.status = 404
        return {'deleted': filename}

    @route('/%s/maps/<filename>' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def maps_content(filename):
        f = open(os.path.join(wolf.config.path, 'config', filename))
        content = f.read()
        f.close()
        wolf.logger.debug('Getting "%s" file' % filename)
        return {'content': content}

    @route('/%s/maps' % prefix, method='POST')
    @auth_basic(auth)
    @content_json
    def maps_create():
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        try:
            filename = data['filename']
            content = data['content']
        except:
            response.status = 400
            return
        if os.path.exists(os.path.join(wolf.config.path, 'config', filename)):
            response.status = 409
            return
        f = open(os.path.join(wolf.config.path, 'config', filename), 'w')
        f.write(content)
        f.close()
        wolf.logger.debug('File "%s" created' % filename)
        response.status = 201
        return {'created': filename}

    @route('/%s/maps/<filename>' % prefix, method='PUT')
    @auth_basic(auth)
    @content_json
    def maps_update(filename):
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        try:
            content = data['content']
        except:
            response.status = 400
            return
        if not os.path.exists(os.path.join(wolf.config.path, 'config', filename)):
            response.status = 404
            return
        f = open(os.path.join(wolf.config.path, 'config', filename), 'w')
        f.write(content)
        f.close()
        wolf.logger.debug('File "%s" updated' % filename)
        wolf.sighup(None, None);
        return {'updated': filename}

    @route('/%s/maps' % prefix, method='GET')
    @auth_basic(auth)
    @content_json
    def maps_list():
        maps = []
        for csvmap in glob.glob(os.path.join(wolf.config.path, 'config', '*.csv')):
            maps.append(os.path.relpath(csvmap, os.path.join(wolf.config.path, 'config')))
        return json.dumps(maps)

# ImportError: cannot import name 'journal' from 'systemd' (unknown location)
#    @route('/%s/journal' % prefix, method='GET')
#    @auth_basic(auth)
#    @content_json
#    def journal_reader():
#        j = journal.Reader()
#        j.this_boot()
#        j.log_level(journal.LOG_DEBUG)
#        j.add_match(_SYSTEMD_UNIT='%s.service' % wolf.__name__.split('.')[0])
#        max_lines = 1000
#        lines = []
#        for entry in list(j)[-max_lines:]:
#            lines.append({'PRIORITY': entry['PRIORITY'], 'SYSLOG_FACILITY': entry['SYSLOG_FACILITY'], 'SYSLOG_IDENTIFIER': entry['SYSLOG_IDENTIFIER'], 'SYSLOG_PID': entry['SYSLOG_PID'], 'SYSLOG_TIMESTAMP': entry['SYSLOG_TIMESTAMP'], 'MESSAGE': entry['MESSAGE']})
#        return (json.dumps(lines))

    @route('/%s/user/<username>' % prefix, method='GET', content_type='application/json')
    @auth_basic(auth)
    @content_json
    def user_get(username):
        section = wolf.__name__.split('.')[0]
        if username != wolf.config.get(section, 'username'):
            response.status = 404
            return {}
        return json.dumps({'username': username})

    @route('/%s/user' % prefix, method='PUT')
    @auth_basic(auth)
    @content_json
    def user_set(username):
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        try:
            username = data['username']
            password = data['password']
        except:
            response.status = 400
            return
        if not username or not password:
            response.status = 400
            return
        section = wolf.__name__.split('.')[0]
        password = base64.b64encode(hashlib.md5(password.encode()).digest()).decode()
        wolf.config.set(section, 'username', password)
        wolf.config.set(section, 'password', password)
        configfile = os.path.join(wolf.config.path, 'config', '%s.ini' % __package__)
        os.rename(configfile, '%s.bak' % configfile)
        wolf.config.write(configfile)
        return json.dumps({'username': username})

    @route('/interfaces', method='GET')
    @auth_basic(auth)
    @content_json
    def interfaces_list():
        netconfigs = []
        try:
            devices = NetworkManager.GetAllDevices()
            for device in devices:
                if not device.Managed:
                    continue
                netconfig = {'interface': device.Interface, 'devicetype': devTypes.get(device.DeviceType, 'unknown'), 'state': devStates.get(device.State, 'unknown')}
                if device.Ip4Config:
                    netconfig['ip4config'] = {'address-data': device.Ip4Config.AddressData, 'addresses': device.Ip4Config.Addresses, 'gateway': device.Ip4Config.Gateway, 'dns': device.Ip4Config.Nameservers}
    #           if device.Ip6Config:
    #               netconfig['ip6config'] = {'address-data': device.Ip6Config.AddressData, 'addresses': device.Ip6Config.Addresses, 'gateway': device.Ip6Config.Gateway, 'dns': device.Ip6Config.Nameservers}
                netconfigs.append(netconfig)
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})
        return json.dumps(netconfigs)

    @route('/interfaces/<ifname>', method='GET')
    @auth_basic(auth)
    @content_json
    def interface_get(ifname):
        try:
            device = NetworkManager.GetDeviceByIpIface(ifname)
            if not device.Managed:
                return json.dumps({'error': 'unmanaged'})
            netconfig = {'interface': device.Interface, 'devicetype': devTypes.get(device.DeviceType, 'unknown'), 'state': devStates.get(device.State, 'unknown')}
            if device.Ip4Config:
                netconfig['ip4config'] = {'address-data': device.Ip4Config.AddressData, 'addresses': device.Ip4Config.Addresses, 'gateway': device.Ip4Config.Gateway, 'dns': device.Ip4Config.Nameservers}
    #       if device.Ip6Config:
    #           netconfig['ip6config'] = {'address-data': device.Ip6Config.AddressData, 'addresses': device.Ip6Config.Addresses, 'gateway': device.Ip6Config.Gateway, 'dns': device.Ip6Config.Nameservers}
            return json.dumps(netconfig)
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})

    @route('/interfaces/<ifname>/connection', method='GET')
    @auth_basic(auth)
    @content_json
    def connection_get(ifname):
        try:
            device = NetworkManager.GetDeviceByIpIface(ifname)
            if not device.Managed:
                return json.dumps({'error': 'unmanaged'})
            connection = device.ActiveConnection
            if connection:
                settings = connection.Connection.GetSettings()
                return json.dumps(settings)
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})
        return json.dumps({})

    @route('/accesspoints', method='GET')
    @auth_basic(auth)
    @content_json
    def ap_list():
        aps = []
        try:
            for ap in AccessPoint.all():
                try:
                    aps.append({'ssid': ap.Ssid, 'bssid': ap.HwAddress, 'frequency': ap.Frequency, 'signal': ap.Strength})
                except ObjectVanished:
                    pass
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})
        return json.dumps(aps)

    @route('/interfaces/<ifname>/connection', method=['POST'])
    @auth_basic(auth)
    @content_json
    def connection_create(ifname):
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        try:
            device = NetworkManager.GetDeviceByIpIface(ifname)
            if not device.Managed:
                return json.dumps({'error': 'unmanaged'})
            connection = device.ActiveConnection
            if connection:
                device.Disconnect()
            NetworkManager.AddAndActivateConnection(data, device, '/')
            connection = device.ActiveConnection
            return json.dumps(connection.Connection.GetSettings())
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})

    @route('/interfaces/<ifname>/connection/<uuid>', method=['PUT'])
    @auth_basic(auth)
    @content_json
    def connection_update(ifname, uuid):
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        try:
            device = NetworkManager.GetDeviceByIpIface(ifname)
            if not device.Managed:
                return json.dumps({'error': 'unmanaged'})
            for connection in device.AvailableConnections:
                settings = connection.GetSettings()
                if settings.get('connection').get('uuid') == uuid:
                    connection.Update(data)
                    NetworkManager.ActivateConnection(connection, device, '/')
                    return json.dumps(connection.GetSettings())
        except dbus.exceptions.DBusException as e:
            return json.dumps({'error': str(e)})
