from datetime import datetime
from dateutil import tz
import os
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout
import urllib
import urllib3

urllib3.disable_warnings()

class rest():

    def __init__(self):
        self.__client = requests.session()
        self.baseurl = config.get(self.name, 'baseurl', fallback = None)
        self.username = config.get(self.name, 'username', fallback = None)
        self.password = config.get(self.name, 'password', fallback = None)
        self.retries = config.getint(self.name, 'retries', fallback = 3)
        self.backoff = config.getfloat(self.name, 'backoff', fallback = 0.3)
        self.timeout = config.getint(self.name, 'timeout', fallback = 30)
        self.token = '%s/token' % self.baseurl
        self.drain = '%s/organization/measures' % self.baseurl
        self.config = '%s/organization/measures/config' % self.baseurl
        self.__csrf = None
        self.__recursion = False
        self.proxies = {}
        logger.info('base REST API URL %s' % self.baseurl)
        try:
            self.proxies = urllib.request.getproxies()
        except:
            pass
        retry = Retry(
            total = self.retries,
            read = self.retries,
            connect = self.retries,
            backoff_factor = self.backoff,
            status_forcelist = (500, 502, 504),
        )
        for protocol in ['http://', 'https://']:
            self.__client.adapters[protocol].max_retries = retry

    def __get_token(self):
        try:
            response = self.__client.get(self.token, timeout=self.timeout, verify=False, proxies=self.proxies)
            logger.debug('GET %s' % self.token)
        except (ConnectionError, ConnectTimeout) as e:
            logger.error (str(e))
            return False
        except TypeError as e:
            # workaround for urllib3 Retry() bug
            logger.error (str(e.__context__))
            return False
        if 'x-csrf-token' in response.headers:
            self.__csrf = response.headers['x-csrf-token']
            logger.debug('Got token %s' % self.__csrf)
            return True
        return False

    def __post(self, data, url):
        if not self.__csrf:
            if not self.__get_token():
                logger.error('Server %s forbids GET token requests. Check plugin and server configuration.' % (self.baseurl))
                return False
        headers = {}
        headers['X-CSRF-TOKEN'] = self.__csrf
        try:
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            response = self.__client.post(url, auth=auth, json=data, headers=headers, timeout=self.timeout, verify=False, proxies=self.proxies)
            logger.debug('POST %s JSON: %s' % (url, data))
        except (ConnectionError, ConnectTimeout) as e:
            logger.error (str(e))
            return False
        except TypeError as e:
            # workaround for urllib3 Retry() bug
            logger.error (str(e.__context__))
            return False
        if response.status_code == 403:
            if not self.__recursion:
                self.__recursion = True
                self.__csrf = None
                return self.__post(data, url)
            else:
                logger.error('Server %s forbids POST requests. Check plugin and server configuration (eg. authentication).' % (self.baseurl))
                self.__recursion = False
        return (response.status_code, response.text)

    def post(self, rawdata):
        data = rawdata.copy()
        dt = datetime.fromtimestamp(data['ts'])
        dd = dt.replace(tzinfo=tz.gettz())
        ts = dd.strftime('%Y-%m-%dT%H:%M:%S%z')
        data['at'] = ts
        del data['ts']
        measures = []
        for measure in data['measures']:
            measures.append({'measure_id': measure, 'value': data['measures'][measure]})
        data['measures'] = measures
        response = self.__post(data, self.drain)
        if response:
            status, text = response
            logger.info('POST client id: %s device id: %s timestamp: %s HTTP status code: %s %s' % (data['client_id'], data['device_id'], data['at'], status, text.splitlines()))
        else:
            return False
        if status in (200, 201, 409):
            return True
        return False

    def post_config(self):
        data = cache.load_meta()
        response = self.__post(data, self.config)
        if response:
            status, text = response
            logger.info('POST configuration HTTP status code: %s %s' % (status, text.splitlines()))
            return True
        return False
