#!/usr/bin/env python3
import glob
import hashlib
import io
import json
import os
import requests
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

        # extract only missing or differing files, only of destination directory
        for f in zip.infolist():
            filename = f.filename.replace(rootdir, '')
            if not filename or not filename.startswith(dest):
                continue
            if any(filename.startswith(e) for e in exclude):
                wolf.logger.debug('Skipping %s' % filename)
                continue
            f.filename = filename
            if f.is_dir():
                if not os.path.isdir(filename):
                    wolf.logger.debug('Extracting %s' % filename)
                    q.put(zip.extract(f))
            if not f.is_dir():
                if not os.path.isfile(filename):
                    wolf.logger.debug('Extracting %s' % filename)
                    q.put(zip.extract(f))
                else:
                    crc32 = zlib.crc32(open(filename,"rb").read())
                    if crc32 != f.CRC:
                        wolf.logger.debug('Overwriting %s' % filename)
                        q.put(zip.extract(f))

        # cleanup destination directory of possible aliens/older files
        filelist = glob.glob('%s/**' % dest, recursive=True)
        namelist = [os.path.relpath(x) for x in zip.namelist()]
        for f in filelist:
            if not os.path.relpath(f) in namelist:
                wolf.logger.debug('Removing %s' % f)
                if os.path.isdir(f):
                    q.put(shutil.rmtree(f, ignore_errors=True))
                if os.path.isfile(f):
                    q.put(os.unlink(f))

        return q.qsize()

    def __mods(self, plugins, installed):
        return set(sum([plugin.need for plugin in plugins if plugin.installed == installed], []))

    def __plugs(self, plugins, installed):
        return set(['%s/plugins/%s' % (__package__, plugin.plugin) for plugin in plugins if plugin.installed == installed])

    def update(self, force=False):
        q = queue.Queue()
        wufile = os.path.join(wolf.config.path, 'webupdate.json')
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
        undeps = self.__mods(wuconf.plugins, False)
        client = requests.session()
        for mod in wuconf.modules:
            # skip modules that are not required by disabled plugins
            if mod.module in undeps:
                continue
            try:
                response = client.get('%s/%s/%s' % (self.api, mod.repository, mod.channel))
            except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
                wolf.logger.error('Cannot check Github repository: %s' % str(e))
                return False
            gitresp = json.loads(response.text)
            if response.status_code == 404:
                wolf.logger.error('Repository %s not found, cannot update!' % mod.repository)
                continue
            if response.status_code != 200:
                wolf.logger.error('Cannot check Github repositories: %s' % gitresp['message'])
                return False
            if not gitresp:
                wolf.logger.error('Repository %s empty, cannot update!' % mod.repository)
                continue
            gitlast = None
            while gitresp:
                gitlast = gitresp.pop()
                if not gitlast.get('prerelease', False):
                    break
            if not gitlast:
                wolf.logger.error('In repository %s there aren\'t stable releases, cannot update!' % mod.repository)
                continue
            name = gitlast.get('name') or gitlast.get('tag_name')
            wolf.logger.debug('Repository %s (online version: %s installed version: %s)' % (mod.repository, name, mod.version))
            if (name > mod.version) or force:
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
                    wolf.logger.error('Downloading of %s exceeded max size of %d bytes, cannot update!' % (mod.module, self.maxsize))
                    continue
                wolf.logger.debug("Downloaded %s buffer size: %d bytes" % (mod.module, buffer.tell()))
                if self.__unzip(buffer, mod.module):
                    q.put(True)
                    mod.version = name
                    wolf.logger.info('%s up to date' % mod.module)
            else:
                wolf.logger.info('%s already up to date' % repo['import'])
        if q.qsize():
            wolf.logger.debug('Updating webupdate configuration %s' % wufile)
            try:
                with open(wufile, 'w') as outfile:
                    json.dump(wuconf, outfile, sort_keys=True, indent=4, default=lambda o: o.__dict__)
            except IOError as e:
                wolf.logger.error('Cannot update webupdate configuration %s: %s' % (wufile, str(e)))
        return q.qsize()
