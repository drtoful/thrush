#-*- coding: utf-8 -*-

"""
    :copyright: (c) 2013 by Tobias Heinzen
    :license: BSD, see LICENSE for more details
"""

import re
import os
import datetime
import time
import locale
import functools
import math
import contextlib
from subprocess import Popen, PIPE, STDOUT

_dsname_re = re.compile('[^a-zA-Z0-9_]')
_fetch_re = re.compile('[0-9]+: .+')


def _convert_to_dsname(name):
    # ds-names are only allowed to be 1-19 characters
    # long and contain [a-Z0-9_] (according to
    # documentation)
    return _dsname_re.sub('', name)[:19]


def _convert_to_timestamp(timeinfo):
    # converts a (date)time object into a timestamp
    # or just get the string representation of whatever
    # was passed
    if isinstance(timeinfo, datetime.datetime):
        timeinfo = int(time.mktime(timeinfo.timetuple()))
    return repr(timeinfo)


def _convert_from_timestamp(timestamp):
    # convert timestamp to datetime object
    timestamp = locale.atoi(timestamp)
    date = datetime.datetime.fromtimestamp(timestamp)
    return date


def _convert_float(unknown, value):
    try:
        value = locale.atof(value)
    except ValueError:
        value = float("nan")

    if math.isnan(value):
        return unknown
    return value


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
    """
        Base class for all Data Source Types.
    """
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

class Derive(DataSource):
    """
        .. versionadded:: 0.2
    """
    _DST = "DERIVE"


class Absolute(DataSource):
    """
        .. versionadded:: 0.2
    """
    _DST = "ABSOLUTE"


class Compute(DataSource):
    """
        .. versionadded:: 0.2
    """
    _DST = "COMPUTE"

    def __init__(self, rpn_expression):
        self.rpn = rpn_expression

    def __str__(self):
        return "DS:%s:%s:%s" % (
            self.name,
            self._DST,
            self.rpn
        )

    def __repr__(self):
        return str(self)


class RRA(object):
    """
        Base class for all Round Robin Archives.
    """

    def __init__(self, xff, steps, rows):
        self.xff = repr(xff)
        self.steps = repr(steps)
        self.rows = repr(rows)

    def __repr__(self):
        return "RRA:%s:%s:%s:%s" % (
            self._CF, self.xff, self.steps, self.rows)

    @property
    def cf(self):
        return self._CF


class Average(RRA):
    """
        Implements the **AVERAGE** consolidation function
    """
    _CF = "AVERAGE"


class Min(RRA):
    """
        Implements the **MIN** consolidation function
    """
    _CF = "MIN"


class Max(RRA):
    """
        Implements the **MAX** consolidation function
    """
    _CF = "MAX"


class Last(RRA):
    """
        Implements the **LAST** consolidation function
    """
    _CF = "LAST"


class RRDFetchResult(object):
    """
        An object of this class can be iterated. On every iteration
        you will be returned a tuple containing a :py:class:`datetime`
        object and a dictionary.

        The dictionary will contain the values for every datasource
        at the given time. The key to the dictionary will be the
        datasource names, as stored in the RRD.

        As this object will contain an open file descriptor to the
        output of ``rrdfetch`` it is advised to close it after
        use. This can be achieved automatically by using this
        object within a ``with`` statement.

        *Example*:

        .. sourcecode:: python

            class MyRRD(rrd.RRD):
                ds = rrd.Gauge(heartbeat=600)
                rra = rrd.Max(xff=0.5, steps=1, rows=24)

            myrrd = MyRRD("my.rrd")
            with myrrd.fetch(myrrd.rra.cf) as result:
                for timestamp, values in result:
                    print timestamp, values[myrrd.ds.name]
    """
    def __init__(self, stdout, dsnames, unknown=None):
        self.stdout = stdout
        self.dsnames = [_convert_to_dsname(name) for name in dsnames]
        self.unknown = unknown

    def __iter__(self):
        for line in self.stdout:
            match = _fetch_re.match(line)
            if match is None:
                continue

            timestamp, values = line.split(":", 1)
            func = functools.partial(_convert_float, self.unknown)
            converted_values = map(func, values.strip().split(' '))
            yield _convert_from_timestamp(timestamp), dict(
                zip(self.dsnames, converted_values)
            )

    def close(self):
        """
            .. versionadded:: 0.3

            Manually closes the open file descriptor associated with this
            result object.
        """
        self.stdout.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def _rrdtool_impl(filename, command, options, wait=True):
    class RRDOutput(object):
        """
        Wrapping around subprocesses output streams to get
        a more "unbuffered" version that should prevent
        errors when having large outputs from rrdtool.

        Based upon:
            https://gist.github.com/thelinuxkid/5114777
        """
        def __init__(self, process):
            self.process = process
            self._check_stderr()

        def _unbuffered(self, stream):
            newlines = ['\n', '\r\n', '\r']

            stream = getattr(self.process, stream)
            with contextlib.closing(stream):
                while True:
                    out = []
                    last = stream.read(1)

                    if last == "" and self.process.poll() is not None:
                        break

                    while last not in newlines:
                        if last == "" and self.process.poll() is not None:
                            break

                        out.append(last)
                        last = stream.read(1)

                    yield "".join(out)

        def _check_stderr(self):
            time.sleep(0.01)

            # check stderr for some text. if so we shall raise error
            code = self.process.poll()
            if not code is None and code != 0:
                raise RRDError(
                    code, "\n".join(
                        [x for x in self._unbuffered("stderr")]
                    )
                )

        def __iter__(self):
            self._check_stderr()
            for line in self._unbuffered("stdout"):
                yield line

        def close(self):
            pass

    env = os.environ
    process = Popen(
        'rrdtool %s %s %s' % (command, filename, " ".join(options)),
        env=env, shell=True, stdout=PIPE, stderr=PIPE, universal_newlines=True
    )

    if wait:
        process.wait()

    return RRDOutput(process)


def _rrd_init(self, filename):
    """
        :param filename: A string containing an absolute or
                         relative path to a file, that is accessed
                         for all operations.
    """
    self.filename = filename


def _rrd_create(self, start='N', step=300, overwrite=False):
    """
        Creates a new RRD with the given `filename`.
        This implements the rrdcreate_ command and thus takes similar
        arguments.

        As the database scheme (i.e. datasources and RRAs) is known,
        this will automatically convert these settings into valid
        parameters for rrdcreate_.

        :param start: Is either an integer or string containing the
                      number of seconds since the epoch,
                      a :py:class:`datetime` object
                      or a string containing an at-style time reference.
        :param step: The number of seconds between each sample within
                     the RRD.
        :param overwrite: When set to True it will overwrite an existing
                          RRD given by the filename upon object creation.
                          This is the opposite of the ``--no-overwrite``
                          flag.

        :raises: :py:class:`thrush.rrd.RRDError`

        .. _rrdcreate: http://oss.oetiker.ch/rrdtool/doc/rrdcreate.en.html
    """
    options = ["--start", _convert_to_timestamp(start), "--step", repr(step)]
    if not overwrite:
        options += ["--no-overwrite"]
    options += [
        repr(self._meta['datasources'][ds])
        for ds in self._meta['datasources_list']
    ]
    options += [
        repr(self._meta['rras'][rra])
        for rra in self._meta['rras_list']
    ]
    stdout = self._meta['implementation'](self.filename, "create", options)


def _rrd_update(self, timestamp, **kwargs):
    """
        Updates a RRD file with the given samples. This implements the
        rrdupdate_ command.

        :param timestamp: Is either an integer or string containing the
                          number of seconds since the epoch or a
                          :py:class:`datetime` object.
        :param kwargs: This is a dictionary were the key is the name of
                       a datasource (i.e. the name of the field of the
                       defined class) and the value, the value for the sample.
                       Not specified datasources will automatically assume
                       the value 'U' for unknown.

        :raises: :py:class:`thrush.rrd.RRDError`

        *Example*: Consider a class ``MyRRD`` that has two datasources
        ds1 and ds2.

        .. sourcecode:: python

            class MyRRD(rrd.RRD):
                ds1 = rrd.Gauge(heartbeat=600)
                ds2 = rrd.Counter(hearbeat=600)
                rra1 = rrd.Max(xff=0.5, steps=1, rows=24)

            myrrd = MyRRD("my.rrd")
            myrrd.update(1234, ds1=5.4, ds2=3)
            myrrd.update(5678, ds2=4)

        These updates will be converted in the following ``rrdupdate``
        executions.

        .. sourcecode:: bash

            rrdupdate my.rrd -t ds1:ds2 1234:5.4:3
            rrdupdate my.rrd -t ds1:ds2 5678:U:4

        .. _rrdupdate: http://oss.oetiker.ch/rrdtool/doc/rrdupdate.en.html
    """
    options = ["--template", ":".join(self._meta['datasources_list']), "--"]
    data = [_convert_to_timestamp(timestamp)]
    data += [
        "U" if not ds in kwargs else str(kwargs[ds])
        for ds in self._meta['datasources_list']
    ]
    options += [":".join(data)]
    stdout = self._meta['implementation'](self.filename, "update", options)


def _rrd_fetch(self, cf, start="end-1day", end="now", resolution=None,
               unknown=None):
    """
        Fetches samples from RRD. This implements the rrdfetch_ command
        and thus takes similar arguments.

        :param cf: The string representation of a consolidation function
        :param start: Is either an integer or string containing the number
                      of seconds since the epoch, a :py:class:`datetime` object
                      or a string containing an at-style time reference.
        :param end: Same as *start*.
        :param resolution: A resolution in seconds either as string or
                           integer. If set to ``None`` rrdfetch_ will
                           determine the best resolution.
        :param unknown: Converts all unknown values in the RRD to the
                        value specified.

        :returns: :py:class:`thrush.rrd.RRDFetchResult`

        :raises: :py:class:`thrush.rrd.RRDError`

        .. versionadded:: 0.3
            *unknown* parameter

        *Example*:

        .. sourcecode:: python

            class MyRRD(rrd.RRD):
                ds1 = rrd.Counter(heartbeat=120)
                rra = rrd.Max(xff=0.5, steps=1, rows=24)

            myrrd = MyRRD("my.rrd")
            myrrd.fetch(cf=myrrd.rra.cf)

        .. _rrdfetch: http://oss.oetiker.ch/rrdtool/doc/rrdfetch.en.html
    """
    options = [
        repr(cf), "--start", _convert_to_timestamp(start), "--end",
        _convert_to_timestamp(end)
    ]
    if not resolution is None:
        options += ['--resolution', repr(resolution)]
    stdout = self._meta['implementation'](
        self.filename, "fetch", options, wait=False
    )
    return RRDFetchResult(stdout, self._meta['datasources_list'], unknown)


def _rrd_last(self):
    """
        .. versionadded:: 0.2

        Fetches the last sample in the RRD (i.e. the data saved by the
        last ``update``). This implements the rrdlastupdate_ command.
        rrdlast_ would only return the timestamp of the last update. This
        information can also be obtained using this method.

        :return: :py:class:`thrush.rrd.RRDFetchResult`

        :raises: :py:class:`thrush.rrd.RRDError`

        .. _rrdlast: http://oss.oetiker.ch/rrdtool/doc/rrdlast.en.html
        .. _rrdlastupdate: http://oss.oetiker.ch/rrdtool/doc/rrdlastupdate.en.html
    """
    stdout = self._meta['implementation'](
        self.filename, "lastupdate", [], wait=False
    )
    return RRDFetchResult(stdout, self._meta['datasources_list'])


def _rrd_first(self, index=0):
    """
        Fetches the timestamp of the first entry in an archive from
        the RRD. This implements the rrdfirst_ command.

        :param index: An index of the list of available archives within
                      the RRD.

        :returns: a :py:class:`datetime` object

        :raises: :py:class:`thrush.rrd.RRDError`

        *Example*:

        .. sourcecode:: python

            class MyRRD(rrd.RRD):
                ds1 = rrd.Counter(heartbeat=120)
                rra = rrd.Max(xff=0.5, steps=1, rows=24)

            myrrd = MyRRD("my.rrd")
            myrrd.first(cf=myrrd.rra.index)

        .. _rrdfirst: http://oss.oetiker.ch/rrdtool/doc/rrdfirst.en.html
    """
    options = ["--rraindex", repr(index)]
    stdout = self._meta['implementation'](self.filename, "first", options)
    return _convert_from_timestamp(stdout.readline()[:-1])


def _rrd_exists(self):
    """
        .. versionadded:: 0.2

        :returns: True if the RRD file already exists, False otherwise

        You can also use a RRD object directly for comparision in
        boolean expression, to check whether the RRD file exists
        or not. Thus ``MyRRD("my.rrd").exists() == MyRRD("my.rrd")``.
    """
    return os.path.isfile(self.filename)


class RRDMeta(type):
    def __new__(cls, name, base, attrs):
        super_new = super(RRDMeta, cls).__new__

        if name == 'RRD' and attrs == {}:
            super_class = super_new(cls, name, base, attrs)

            super_class.add_to_class('create', _rrd_create)
            super_class.add_to_class('update', _rrd_update)
            super_class.add_to_class('last', _rrd_last)
            super_class.add_to_class('first', _rrd_first)
            super_class.add_to_class('fetch', _rrd_fetch)
            super_class.add_to_class('exists', _rrd_exists)
            super_class.add_to_class('__bool__', _rrd_exists)
            super_class.add_to_class('__nonzero__', _rrd_exists)
            super_class.add_to_class('__init__', _rrd_init)

            return super_class

        module = attrs.pop('__module__')
        new_class = super_new(cls, name, base, {'__module__': module})

        new_class.add_to_class('_meta', {
            'datasources': {},
            'datasources_list': [],
            'rras': {},
            'rras_index': {},
            'rras_list': [],
            'implementation': _rrdtool_impl
        })
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)
        new_class._meta['datasources_list'] = \
            sorted(new_class._meta['datasources_list'])
        new_class._meta['rras_list'] = \
            sorted(new_class._meta['rras_list'])
        for i in xrange(0, len(new_class._meta['rras_list'])):
            name = new_class._meta['rras_list'][i]
            new_class._meta['rras_index'][name] = i
            new_class._meta['rras'][name].index = i

        return new_class

    def add_to_class(cls, name, value):
        if isinstance(value, DataSource):
            dsname = _convert_to_dsname(name)
            cls._meta['datasources'][name] = value
            cls._meta['datasources_list'].append(name)
            setattr(value, 'name', dsname)
        elif isinstance(value, RRA):
            cls._meta['rras'][name] = value
            cls._meta['rras_list'].append(name)
        elif name == "_impl":
            cls._meta['implementation'] = value

        setattr(cls, name, value)


RRD = RRDMeta("RRD", (object,), {})
