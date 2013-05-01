#-*- coding: utf-8 -*-

"""
class MyRRD(RRD):
    ds1 = Gauge(...)
    ds2 = Counter(...)

    rra1 = Average(...)
    rra2 = Average(...)
    rra2 = Max(...)

r = MyRRD(filename)
r.create(...)
r.fetch(...)
    [{'time': ..., 'ds1': ..., 'ds2': ...}, ...]
r.update(...,ds1=...,ds2=...)
r.write()

MyNewRRDClass = RRDUtils.create_from_info(filename, ...)
"""

import re
import os
import datetime
import time
import locale
from subprocess import Popen, PIPE

_dsname_re = re.compile('[^a-zA-Z0-9_]')
def _convert_ds_name(name):
    # ds-names are only allowed to be 1-19 characters
    # long and contain [a-Z0-9_] (according to
    # documentation)
    return _dsname_re.sub('', name)[:19]

def _convert_time(timeinfo):
    # converts a (date)time object into a timestamp
    # or just get the string representation of whatever
    # was passed
    if isinstance(timeinfo, datetime.datetime):
        timeinfo = time.time.mktime(timeinfo.timetuple())
    return repr(timeinfo)

_time_localoffset = datetime.timedelta(seconds=
    -(time.timezone if (time.localtime().tm_isdst == 0) else time.altzone))
def _convert_utc_time(timestamp):
    # RRDTool always converts it's times into UTC
    # so we convert back to local time
    timestamp = locale.atoi(timestamp)
    d = datetime.datetime.fromtimestamp(timestamp)
    return d + _time_localoffset

class RRDError(Exception):
    def __init__(self, errorcode, message):
        self.errorcode = errorcode
        self.message = message

    def __str__(self):
        return "rrdtool returned '%d': %s" % (
            self.errorcode, self.message)

    def __repr__(self):
        return str(self)

class DataSource(object):
    def __init__(self, heartbeat, min='U', max='U'):
        self.heartbeat = repr(heartbeat)
        self.min = repr(min)
        self.max = repr(max)

    def __str__(self):
        return "DS:%s:%s:%s:%s:%s" % (
            self.name,
            self._DST,
            self.heartbeat,
            self.min,
            self.max
        )

    def __repr__(self):
        return str(self)

class Gauge(DataSource):
    _DST = "GAUGE"

class Counter(DataSource):
    _DST = "COUNTER"

class RRA(object):
    def __init__(self, xff, steps, rows):
        self.xff = repr(xff)
        self.steps = repr(steps)
        self.rows = repr(rows)

    def __repr__(self):
        return "RRA:%s:%s:%s:%s" % (
            self._CF, self.xff, self.steps, self.rows)

class Average(RRA):
    _CF = "AVERAGE"

class Min(RRA):
    _CF = "MIN"

class Max(RRA):
    _CF = "MAX"

class Last(RRA):
    _CF = "LAST"

def _rrdtool_impl(filename, command, options):
    env = os.environ
    process = Popen('rrdtool %s %s %s' % (command, filename, " ".join(options)),
        env=env, shell=True, stdout=PIPE, stderr=PIPE)
    process.wait()

    if process.returncode != 0:
        raise RRDError(process.returncode, process.stderr.readline()[:-1])

    return process.stdout

def _rrd_init(obj, filename):
    obj.filename = filename

def _rrd_setattr(obj, key, value):
    if key in obj._meta['datasources_list'] or key in obj._meta['rras_list']:
        raise Exception("trying to write read-only attribute")
    object.__setattr__(obj, key, value)

def _rrd_create(obj, start='N', step=300, overwrite=False):
    """
        @param start can be one of the following
                        * integer: epoch time
                        * string: at-time
                        * datetime object
    """
    options = ["--start", _convert_time(start), "--step", repr(step)]
    if not overwrite:
        options += ["--no-overwrite"]

    for name, datasource in obj._meta['datasources'].items():
        options += [repr(datasource)]
    for name, rra in obj._meta['rras'].items():
        options += [repr(rra)]

    stdout = obj._meta['implementation'](obj.filename, "create", options)

def _rrd_first(obj, index=0):
    options = ["--rraindex", repr(index)]
    stdout = obj._meta['implementation'](obj.filename, "first", options)
    return _convert_utc_time(stdout.readline()[:-1])

class RRDMeta(type):
    def __new__(cls, name, base, attrs):
        super_new = super(RRDMeta, cls).__new__

        if name == 'RRD' and attrs == {}:
            super_class = super_new(cls, name, base, attrs)

            super_class.add_to_class('create', _rrd_create)
            super_class.add_to_class('first', _rrd_first)
            super_class.add_to_class('__init__', _rrd_init)
            super_class.add_to_class('__setattr__', _rrd_setattr)

            return super_class

        module = attrs.pop('__module__')
        new_class = super_new(cls, name, base, {'__module__': module})

        new_class.add_to_class('_meta', {
            'datasources': {},
            'datasources_list': [],
            'rras': {},
            'rras_index': {},
            'rras_list' : [],
            'implementation': _rrdtool_impl
        })
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        return new_class

    def add_to_class(cls, name, value):
        if isinstance(value, DataSource):
            cls._meta['datasources'][name] = value
            cls._meta['datasources_list'].append(name)
            setattr(value, 'name', _convert_ds_name(name))
        elif isinstance(value, RRA):
            cls._meta['rras'][name] = value
            cls._meta['rras_list'].append(name)

            index = len(cls._meta['rras_index'])
            cls._meta['rras_index'][name] = index
            value.index = index
        elif name == "_impl":
            cls._meta['implementation'] = value

        setattr(cls, name, value)

RRD = RRDMeta("RRD", (object,), {})

