.. _api:

API
===

Basics
------

.. autoclass:: thrush.rrd.RRDError
    :show-inheritance:


RRD Object
----------

.. autoclass:: thrush.rrd.RRD
    :members: create, exists, update, fetch, last, first

.. autoclass:: thrush.rrd.RRDFetchResult()

Datasources
-----------

.. autoclass:: thrush.rrd.DataSource()

    .. attribute:: name

        The name of the Data Source that is stored in
        the RRD. A valid name only contains 12 characters
        of the set {a,...,z,A,...,Z,0,...9,_}.

.. autoclass:: thrush.rrd.Counter
    :show-inheritance:

.. autoclass:: thrush.rrd.Gauge
    :show-inheritance:

.. autoclass:: thrush.rrd.Derive
    :show-inheritance:

.. autoclass:: thrush.rrd.Absolute
    :show-inheritance:

.. autoclass:: thrush.rrd.Compute
    :show-inheritance:

Archives (RRA)
--------------

.. autoclass:: thrush.rrd.RRA()

    .. attribute:: cf

        The string representation of the consolidation
        function of this archive.

    .. attribute:: index

        The index of this archive within the list of
        available archives in the RRD.

.. autoclass:: thrush.rrd.Min
    :show-inheritance:

.. autoclass:: thrush.rrd.Max
    :show-inheritance:

.. autoclass:: thrush.rrd.Average
    :show-inheritance:

.. autoclass:: thrush.rrd.Last
    :show-inheritance:
