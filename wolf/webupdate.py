#!/usr/bin/env python3
import glob
import hashlib
import io
import json
import os
import re
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout, ReadTimeout
import shutil
import time
import zipfile
import zlib
import wolf

try:
    import queue
except ImportError:
    import Queue as queue

try:
    from types import SimpleNamespace as Namespace
except ImportError:
    from argparse import Namespace

class WWebUpdate():

    def __init__(self):
        self.__exclude = {'config'}
        self.api = 'https://api.github.com/repos'
        self.maxsize = 10485760

    def __unzip(self, buffer, dest):
        if not dest:
            return 0
        q = queue.Queue()
        try:
            zip=zipfile.ZipFile(buffer)
        except zipfile.BadZipFile as e:
            wolf.logger.error('Downloaded archive corrupted, cannot update!')
            return 0
        if zip.testzip():
            wolf.logger.error('Downloaded archive corrupted, cannot update!')
            return 0
        wolf.logger.info('Downloaded archive test OK')

        exclude = self.__exclude.copy()
        exclude.discard(dest)
        rootdir = zip.filelist[0].filename

        if not self.__check_access(dest):
            return 0

        # extract only missing or differing files, only of destination directory
        for f in zip.infolist():
            filename = f.filename.replace(rootdir, '')
            if not filename or not filename.startswith(dest):
                continue
            if any(filename.startswith(e) for e in exclude):
                wolf.logger.debug('Skipping %s' % filename)
                continue
            f.filename = filename
            if f.filename[-1] == '/':
                if self.__check_access(os.path.abspath(os.path.join(filename, os.pardir))) and not os.path.isdir(filename):
                    wolf.logger.debug('Extracting %s' % filename)
                    q.put(zip.extract(f))
            else:
                if os.path.isfile(filename):
                    crc32 = zlib.crc32(open(filename,"rb").read())
                    if self.__check_access(filename) and crc32 != f.CRC:
                        wolf.logger.debug('Overwriting %s' % filename)
                        q.put(zip.extract(f))
                else:
                    if self.__check_access(filename):
                        wolf.logger.debug('Extracting %s' % filename)
                        q.put(zip.extract(f))

        # cleanup destination directory of possible aliens/older files
        filelist = glob.glob('%s/**' % dest, recursive=True)
        namelist = [os.path.relpath(x) for x in zip.namelist()]
        for f in filelist:
            if not os.path.relpath(f) in namelist:
                if not self.__check_access(f):
                    continue
                wolf.logger.debug('Removing %s' % f)
                if os.path.isdir(f):
                    q.put(shutil.rmtree(f, ignore_errors=True))
                if os.path.isfile(f):
                    q.put(os.unlink(f))

        return q.qsize()

    def __check_access(self, path):
        if os.path.exists(path) and not os.access(path, os.W_OK):
            wolf.logger.warning('%s is not writable by uid/gid %d:%d, skipping!' % (path, os.getuid(), os.getgid()))
            return False
        return True

    def __mods(self, plugins, installed):
        return set(sum([plugin.need for plugin in plugins if plugin.installed == installed], []))

    def __plugs(self, plugins, installed):
        return set(['%s/plugins/%s' % (__package__, plugin.plugin) for plugin in plugins if plugin.installed == installed])

    def update(self, force=False):
        q = queue.Queue()
        wufile = os.path.join(wolf.config.path, 'config', 'webupdate.json')
        wolf.logger.debug('Reading webupdate configuration %s' % wufile)
        try:
            with open(wufile) as infile:
                wuconf = json.load(infile, object_hook=lambda d: Namespace(**d))
        except json.decoder.JSONDecodeError as e:
            wolf.logger.error('Webupdate configuration %s invalid: %s' % (wufile, str(e)))
            return
        except FileNotFoundError:
            wolf.logger.error('Webupdate configuration %s not found!' % wufile)
            return
        # excludes disabled plugins
        self.__exclude.update(self.__plugs(wuconf.plugins, False))
        notinst = self.__mods(wuconf.plugins, False)
        inst = self.__mods(wuconf.plugins, True)
        undeps = set([i for i in notinst if i not in inst])
        client = requests.session()
        for mod in wuconf.modules:
            # skip modules that are not required by disabled plugins
            if mod.module in undeps:
                wolf.logger.info('Does not updating %s, plugins not enabled.' % mod.module)
                continue
            # skip freezed modules
            if hasattr(mod, 'freeze') and mod.freeze:
                wolf.logger.info('Does not updating %s, module freezed.' % mod.module)
                continue
            try:
                response = client.get('%s/%s/%s' % (self.api, mod.repository, mod.channel))
            except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
                wolf.logger.error('Cannot check Github repository: %s' % str(e))
                return False
            gitresp = json.loads(response.text)
            if response.status_code == 404:
                wolf.logger.error('Repository %s not found, cannot update %s!' % (mod.repository, mod.module))
                continue
            if response.status_code != 200:
                wolf.logger.error('Cannot check Github repositories: %s' % gitresp['message'])
                return False
            if not gitresp:
                wolf.logger.error('Repository %s empty, cannot update %s!' % (mod.repository, mod.module))
                continue
            gitlast = None
            while gitresp:
                gitlast = gitresp.pop(0)
                if not gitlast.get('prerelease', False):
                    break
            if not gitlast:
                wolf.logger.error('In repository %s there aren\'t stable releases, cannot update %s!' % (mod.repository, mod.module))
                continue
            name = gitlast.get('name') or gitlast.get('tag_name')
            wolf.logger.debug('Repository %s (online version: %s installed version: %s)' % (mod.repository, re.sub('[A-Za-z]*\s*', '', name), re.sub('[A-Za-z]*\s*', '', mod.version)))
            if (re.sub('[A-Za-z]*\s*', '', name) > re.sub('[A-Za-z]*\s*', '', mod.version)) or force:
                wolf.logger.info('Updating %s from repository %s ...' % (mod.module, mod.repository))
                url = gitlast.get('zipball_url')
                wolf.logger.debug('Downloading %s' % url)
                try:
                    response = client.get(url, stream=True)
                except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
                    wolf.logger.error('Cannot download %s %s' % (url, str(e)))
                    return False
                if response.status_code != 200:
                    wolf.logger.error('Cannot download %s %s' % (url, response.text.splitlines()))
                    return False
                buffer = io.BytesIO()
                try:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            buffer.write(chunk)
                            if buffer.tell() > self.maxsize:
                                response.close()
                                raise ValueError()
                except ValueError:
                    wolf.logger.error('Downloading of %s exceeded max size of %d bytes, cannot update %s!' % (url, self.maxsize, mod.module))
                    continue
                wolf.logger.debug("Downloaded %s buffer size: %d bytes" % (mod.module, buffer.tell()))
                if self.__unzip(buffer, mod.module):
                    q.put(True)
                    mod.version = name
                    wolf.logger.info('%s up to date' % mod.module)
            else:
                wolf.logger.info('%s already up to date' % mod.module)
        if q.qsize():
            wolf.logger.debug('Updating webupdate configuration %s' % wufile)
            try:
                with open(wufile, 'w') as outfile:
                    json.dump(wuconf, outfile, sort_keys=True, indent=4, default=lambda o: o.__dict__)
            except IOError as e:
                wolf.logger.error('Cannot update webupdate configuration %s: %s' % (wufile, str(e)))
        return q.qsize()
