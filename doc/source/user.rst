Introduction
============

Current implementations of rrdtool produce a lot of
unnecessary code. Oftentimes you will create
helper methods, to create RRDs with the same schema.

That's where thrush comes in. Similar to SQLAlchemy_
and WTForms_, you can define the schema of your
RRDs.

I also tried to use the same interface for methods,
as the command line tool. This should make it easier for
those, that have been using RRD for years on the command
line.

.. _SQLAlchemy: http://www.sqlalchemy.org
.. _WTForms: http://wtforms.simplecodes.com

.. _installation:

Installation
============

Thrush is available on PyPi_ and can thus be installed
by entering the following command.

.. sourcecode:: bash

    pip install thrush

For a more up-to-date version, you should clone the git
repository of thrush.

.. _PyPi: http://pypi.python.org

.. _quickstart:

Quickstart
==========

Here is a simple example on how to use thrush.

.. sourcecode:: python

    from thrush import rrd
    from datetime import datetime

    class MyRRD(rrd.RRD):
        ds = rrd.Counter(heartbeat=120)
        rra = rrd.Average(xff=0.5, steps=1, rows=2)

    db = MyRRD("my.rrd")
    db.create()

    db.update(datetime.now(), ds=0.4)

    with db.fetch(db.rra.cf) as res:
        for timestamp, values in res:
            print timestamp, values

See the :ref:`api` for more detailed options.
