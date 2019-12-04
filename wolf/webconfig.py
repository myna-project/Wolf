#!/usr/bin/env python3
from bottle import app, hook, mount, redirect, request, response, route, run, static_file
from datetime import datetime
import json
import os
import pkgutil
import threading
import wolf
from wsgiproxy import HostProxy

class AccessLog(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, e, h):
        ret = self.app(e, h)
        wolf.logger.info('%s %s - [%s] "%s %s %s" %s %d' % (request.remote_addr, request.environ.get('REMOTE_USER', '-'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), request.environ.get('REQUEST_METHOD'), request.path,
            request.environ.get('SERVER_PROTOCOL'), response.status_code, response.content_length))
        return ret

class WWebConfig():

    def run(self):
        wolf.logger.debug('Listening on %s:%s' % (wolf.config.webaddr, wolf.config.webport))
        thread = threading.Thread(target=run, name='webconfig', kwargs=dict(host = wolf.config.webaddr, port = wolf.config.webport, quiet=True,  app=AccessLog(app())))
        thread.start()
        wolf.logger.debug('Proxying /net to %s' % wolf.config.netslave)
        mount('/net', HostProxy(wolf.config.netslave))

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

    @route('/', method='GET')
    def home():
        redirect('/static/index.html', 301)

    @route('/static/<filepath:path>', method='GET')
    def server_static(filepath):
        return static_file(filepath, root=wolf.config.webroot)

    @route('/plugins/available', method='GET')
    def plugins_available():
        path = os.path.join(os.getcwd(), __package__, 'plugins')
        wolf.logger.debug('%s plugins path %s' % (__package__, path))
        modules_in = pkgutil.iter_modules(path = [os.path.join(path, 'in')])
        modules_out = pkgutil.iter_modules(path = [os.path.join(path, 'out')])
        plugins_in = []
        plugins_out = []
        for loader, name, ispkg in modules_in:
            plugins_in.append(name)
        for loader, name, ispkg in modules_out:
            plugins_out.append(name)
        return json.dumps({'plugins': [{'in': plugins_in}, {'out': plugins_out}]})

    @route('/config/sections', method='GET')
    def config_redis():
        return json.dumps(wolf.config.sections())

    @route('/config/<section>', method='GET')
    def section_get(section):
        if section in wolf.config.sections():
            return wolf.config.json(section=section)
        else:
            response.status = 404

    @route('/config/<section>', method='POST')
    def section_post(section):
        if section in wolf.config.sections():
            response.status = 400
            return
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        wolf.config.add_section(section)
        for key in data.keys():
            wolf.config.set(section, key, data[key])
        wolf.config.write(os.path.join(wolf.config.path, '%s.ini' % __package__))
        response.status = 201
        return wolf.config.json(section=section)

    @route('/config/<section>', method='PUT')
    def section_put(section):
        if section not in wolf.config.sections():
            response.status = 404
            return
        try:
            data = request.json
        except:
            response.status = 400
            return
        if data is None:
            response.status = 400
            return
        wolf.config.remove_section(section)
        wolf.config.add_section(section)
        for key in data.keys():
            wolf.config.set(section, key, data[key])
        wolf.config.write(os.path.join(wolf.config.path, '%s.ini' % __package__))
        response.status = 201
        return wolf.config.json(section=section)

    @route('/config/<section>', method='DELETE')
    def section_delete(section):
        if section not in wolf.config.sections():
            response.status = 404
            return
        wolf.config.remove_section(section)
        wolf.config.write(os.path.join(wolf.config.path, '%s.ini' % __package__))
        response.status = 201

    @route('/maps/<filename>', method='DELETE')
    def maps_download(filename):
        try:
            os.unlink('%s/%s' % (wolf.config.path, filename))
            wolf.logger.debug('File "%s" deleted' % upload.filename)
        except FileNotFoundError:
            response.status = 404

    @route('/maps/<filename>', method='GET')
    def maps_download(filename):
        return static_file(filename, root=wolf.config.path, download=filename)

    @route('/maps', method='POST')
    def maps_upload():
        plugin = request.forms.get('plugin')
        upload = request.files.get('upload')
        name, ext = os.path.splitext(upload.filename)
        if ext not in ('.csv'):
            response.status = 403
            return
        upload.save(os.path.join(wolf.config.path, upload.filename), overwrite=True)
        wolf.logger.debug('File "%s" successfully saved to "%s"' % (upload.filename, wolf.config.path))
        return "File successfully saved to %s." % wolf.config.path
