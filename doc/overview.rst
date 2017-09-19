***************
System Overview
***************

:any:`TaipanDB` is the Python package which handles interactions between the
:any:`taipan` tiling and scheduling package, and the backend database which
provides data storage.

The aim behind having :any:`TaipanDB` as a separate package is to allow for
plug-and-play replacement of database functions (and, indeed, plug-and-play
replacement of the database itself) without having to make modifications to
the :any:`taipan` code. Although the :any:`taipan` code is dependent upon
:any:`TaipanDB`, it is designed so that individual methods can be swapped out
without affecting how the rest of the code packages function. Conceivably,
an entirely new code base could be developed for a different database
backend, and substituted in for the existing code without causing any issues.

:any:`TaipanDB` abstracts database interactions away from the end user. This is
to protect the database against integrity errors which may be caused by users
directly manipulating data. In particular, *no-one except an expert user*
should directly interact with the database behind :any:`TaipanDB`.