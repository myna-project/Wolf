import time
from pyHS100 import SmartDevice, SmartPlug, SmartBulb

class tplink():

    def __init__(self, name):
        self.name = name
        self.clientid = config.clientid
        self.deviceid = config.get(self.name, 'deviceid')
        self.descr = config.get(self.name, 'descr', fallback = '')
        self.host = config.get(self.name, 'host')

        self.mapping = [['voltage', 'Voltage', 'mV', 'I', 0, 0.001, 0, 'voltage_mv'],
                   ['current', 'Current', 'mA', 'I', 0, 0.001, 0, 'current_ma'],
                   ['power', 'Power', 'mW', 'I', 0, 0.001, 0, 'power_mw'],
                   ['total', 'Total energy', 'Wh', 'I', 0, 0.001, 0, 'total_wh'],
                   ['relay', 'Relay state', '', 'c', 1, 1, 0, 'relay_state']]
        cache.store_meta(self.deviceid, self.name, self.descr, self.mapping)

    def poll(self):
        p = SmartPlug(self.host)
        measures = {}
        ut = time.time()
        for row in self.mapping:
            (name, descr, unit, datatype, rw, scale, offset, key) = row
            scale = float(scale)
            if key in p.sys_info.keys():
                value = p.sys_info[key]
                measures[name] = value
                logger.debug('Device: %s host: %s %s: %s' % (p.sys_info['model'], self.host, name, value))
        if p.has_emeter:
#            json['realtime'] = p.get_emeter_realtime()
#            json['daystat'] = p.get_emeter_daily()
#            json['monthstat'] = p.get_emeter_monthly()
            realtime = p.get_emeter_realtime()
            for row in self.mapping:
                (name, descr, unit, datatype, rw, scale, offset, key) = row
                scale = float(scale)
                if key in realtime.keys():
                    value = realtime[key]
                    if datatype not in ('c', 's'):
                        value = round(value * scale, 14) + offset
                    measures[name] = value
                    logger.debug('Device: %s host: %s %s: %s' % (p.sys_info['model'], self.host, name, value))
        sys_info = dict(p.sys_info)

        logger.debug('Device: %s host: %s sysinfo: %s' % (p.sys_info['model'], self.host, dict(p.sys_info)))
        data = {'ts': ut, 'client_id': self.clientid, 'device_id': self.deviceid, 'measures': measures}
        return data

    def write(self, name, value):
        if name == 'relay':
            p = SmartPlug(self.host)
            p.state = value
            logger.debug('Device: %s host: %s %s: %s' % (p.sys_info['model'], self.host, 'relay_state', p.sys_info['relay_state']))
