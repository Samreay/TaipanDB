*****
Setup
*****

The following is a (fairly exhaustive) guide to what is required to
successfully stand up a :any:`TaipanDB`-backed system.

Suggested computing architecture
================================

Notionally, :any:`TaipanDB` can operate on any computer system, from a personal
laptop to a large server. However, for best performance (and to take advantage
of database and code performance improvements), we recommend a system with at
least the following specifications:

- Minimum of eight processor cores;
- Minimum of 32 GB RAM;
- Depending on the number of fields, up to 2TB of disk space

This code (as well as operations with the :any:`taipan` package) has been tested
on a Linux virtual machine hosted at AAO with approximately these
specifications.

Preparing the database engine
=============================

:any:`TaipanDB` is currently back-ended by a Postgres_ database. Although not
specifically required, we *strongly* recommend a recent version of Postgres (9.6
or later) for the following reasons:

- Versions of Postgres after 9.6 have the ability to perform some basic
  multithreading of queries internally, providing a performance boost;
- Latest versions of Postgres are able to perform UPSERT_ queries, which
  :any:`TaipanDB` has some limited ability to take advantage of;
- Later versions of Postgres (9.2 and up) support something called `index-only
  queries <https://wiki.postgresql.org/wiki/Index-only_scans>`_. This gives
  Postgres the ability to satisfy the query by only looking at a table's
  indices (basically, their reference arrays) rather than having to access the
  table data themselves. Such queries offer an enormous performance
  improvement by avoiding the I/O penalties associated with reading table data
  off the disk. :any:`TaipanDB` configures its tables in such a way that major,
  high-volume queries it performs may be satisfied using an index-only scan.

.. _Postgres: https://www.postgresql.org/
.. _UPSERT: https://www.postgresql.org/docs/9.5/static/sql-insert.html

Configuring PostGres
--------------------

.. _ref-config-user:

User
^^^^

You will require a user account to be set up on the Postgres database in order
to access the data. The account details, as well as the database name, need to
be provided whenever attempting to connect to the database. The easiest way
to do this is to place a file named ``config.json`` at the same place in the
directory structure as the :any:`TaipanDB` main folder. It looks something
like this::

    {
      "host": "localhost",
      "port": 5432,
      "user": "taipan",
      "password": "password",
      "database": "taipandb"
    }

Settings
^^^^^^^^

The following settings should be applied to the database:

.. list-table::
    :header-rows: 1

    * - Setting
      - Value
      - Explanation
    * - ``max_stack_depth``
      - 7680 kB
      - Allows the database to run large queries that involve a large amount
        of table inheritance queries
    * - ``constraint_exclusion``
      - ``partition``
      - Improves performance for queries where the query has to examine a
        collection of partitioned child tables.
    * - ``max_locks_per_transaction``
      - ``512``
      - Ups the number of table locks a single query is allowed to have; this
        is important for queries accessing a large number of child tables.
    * - ``shared_buffers``
      - 500 MB
      -
    * - ``work_mem``
      - 1 GB
      -

Maintenance
^^^^^^^^^^^

By far the most helpful command in maintaining the database is
``VACUUM ANALYZE``. This can be run on a per-table basis, or on the database
as a whole by simply leaving out the table name. The ``VACUUM`` portion of the
command cleans dead tuples away from the table and re-allocated disk space;
the (optional) ``ANALYZE`` portion of the command updates the query planner
about the disk state of the table, providing much improved performance.

It is recommended that tables which undergo frequent addition and deletion of
data (e.g. ``tile``, ``target_field``, ``tiling_*``) undergo ``VACUUM
ANALYZE`` at least weekly, if not more regularly. Tables which have data
constantly updated (e.g. ``science_target``) should be done roughly monthly to
improve the query planner's performance. Mostly static tables (e.g. ``target``,
``observability`` and its child tables ``obs_*``) should only need this
command run at creation, and thereafter only if a significant portion of the
data changes.

Configuring :any:`TaipanDB`
---------------------------

The main configuration required of the code is the setting up of the database
configuration JSON file, as per :ref:`ref-config-user`.

Loading initial data to the database
====================================

The script :any:`taipandb.resources.stable_load` is provided to handle the
initial
data load. This script should be run as such (i.e., not imported and executed
within another Python script).

Because using the script should only really be a one-off thing, elements of
the loading sequence are added or removed by commenting out/in the relative
code block. The code is well-commented, so it should be fairly clear what each
block is doing.

Depending on how you choose to organise your computer file system, there are
some lines that you will need to modify in order to let the script find the
input catalogues. These are the ``data_dir`` path (where the catalogues are
stored) and the ``table_dir`` path (where the definition files for the tables
are stored). ``table_dir`` can usually be left as the default, which will
read in the table definition files packaged with :any:`TaipanDB`. However,
if you only need to stand up a few tables (e.g. these are the only tables that
you have deleted and need to resurrect), you can copy the relevant table
definition files into another directory and set that path to be ``table_dir``.

Catalogues should be prepared as FITS tables that satisfy the ingest mapping
specified in :ref:`doc-prep-catalogues`.

Note that initial data load can take a significant amount of time, up to the
order of a week if Almanac calculation and ingest is required. It is
*strongly* recommended that ``stable_load.py`` be run in an ``at``
environment (or its non-Unix equivalent).
