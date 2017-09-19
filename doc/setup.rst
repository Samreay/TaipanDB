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

User
^^^^

Settings
^^^^^^^^

Maintenance
^^^^^^^^^^^

Configuring :any:`TaipanDB`
---------------------------

Loading initial data to the database
====================================
