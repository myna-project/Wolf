#!/usr/bin/env python3
import csv
import enum
from jsonpath_rw import parse, lexer
from lxml import etree
import wolf

class WCSVType(enum.Enum):
    Raw = 1
    Modbus = 2
    JSON = 3
    XML = 4
    MQTT = 5

class WCSVMap():
    def __init__(self):
        pass

    def load(self, csvfile, csvtype):
        with open(csvfile, 'r') as f:
            lines = csv.reader(self.__decomment(f), quoting=csv.QUOTE_NONNUMERIC)
            lines = list(lines)
        f.close()
        if not lines:
            raise ValueError('%s empty (or only invalid/comments lines)' % csvfile);
        tree = etree.fromstring('<O/>')
        self.mapping = lines
        for ln, row in enumerate(lines):
            try:
                self.__check_type(row, 1, str)
                self.__check_type(row, 2, str)
                self.__check_type(row, 3, str)
                self.__check_type(row, 4, str)
                self.__check_type(row, 5, float)
                self.__check_type(row, 6, float)
                if csvtype == WCSVType.Modbus:
                    (name, descr, unit, datatype, scale, offset, register) = row
                    self.__check_type(row, 7, float)
                if csvtype == WCSVType.JSON:
                    (name, descr, unit, datatype, scale, offset, jsonpath) = row
                    self.__check_type(row, 7, str)
                    try:
                        path = parse(jsonpath)
                    except (AttributeError, ValueError, Exception, lexer.JsonPathLexerError) as e:
                        raise ValueError('Invalid JSONPath filter "%s" for "%s": %s' % (jsonpath, name, str(e)))
                if csvtype == WCSVType.XML:
                    (name, descr, unit, datatype, scale, offset, xpath) = row
                    self.__check_type(row, 7, str)
                    try:
                        path = tree.xpath(xpath)
                    except etree.XPathEvalError as e:
                        raise ValueError('Invalid XPath filter "%s" for "%s": %s' % (xpath, name, str(e)))
                if csvtype == WCSVType.MQTT:
                    (name, descr, unit, datatype, scale, offset, topic, qos) = row
                    self.__check_type(row, 7, str)
                    self.__check_type(row, 8, float)
            except Exception as e:
                wolf.logger.error('"%s" line %d: %s' % (csvfile, ln + 1, str(e)))
                del self.mapping[ln]
        return self.mapping

    def __check_type(self, arr, num, typ):
        val = arr[num - 1]
        if not isinstance(val, typ):
            raise TypeError("parameter %d ('%s') must be %s, not %s" % (num, val, typ, type(val)))

    def __decomment(self, csvfile):
        for row in csvfile:
            raw = row.split('#')[0].strip()
            if raw: yield raw

