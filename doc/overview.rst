***************
System Overview
***************

:mod:`taipandb` is the Python package which handles interactions between the
:mod:`taipan` tiling and scheduling package, and the backend database which
provides data storage.

The aim behind having :mod:`taipandb` as a separate package is to allow for
plug-and-play replacement of database functions (and, indeed, plug-and-play
replacement of the database itself) without having to make modifications to
the :mod:`taipan` code. Although the :mod:`taipan` code is dependent upon
:mod:`taipandb`, it is designed so that individual methods can be swapped out
without affecting how the rest of the code packages function. Conceivably,
an entirely new code base could be developed for a different database
backend, and substituted in for the existing code without causing any issues.

:mod:`taipandb` abstracts database interactions away from the end user. This is
to protect the database against integrity errors which may be caused by users
directly manipulating data. In particular, *no-one except an expert user*
should directly interact with the database behind :mod:`taipandb`.