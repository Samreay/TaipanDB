"""
``resources`` contains the various versions of the core TaipanDB code. The
current stable version (``stable``) and current development version (``dev``)
are maintained using symlinks, to allow multiple distributions of the code.

Importing functions from TaipanDB should be done referring to either ``stable``
or ``dev``, rather than importing directly from a numbered version. Within each
version, the code should only import from that numbered version for internal
consistency.
"""