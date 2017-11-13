"""
Extract information from the database.

These routines are designed to extract useful subsets of information from
the attached database. These data can be manipulated in Python, and then
returned to the database using :any:`manipulate`.
"""

import re
from taipan.core import JSON_DTFORMAT_NAIVE

# -- OBSERVING DEFINITION FILE - FILE NAME STRUCTURE
# If you change one of these, make sure you update all the others to match!
# Observing DT, tile PK, field ID
OBS_DEF_FILENAME = '%s_tile%07d_field%05d_config.json'
OBS_DEF_FILENAME_REGEX = re.compile(r'^*\/(?P<dt>[0-9]{4}-[0-9]{2}-[0-9]{2}T'
                                    r'[0-9]{2}:[0-9]{2}:[0-9]{2})_'
                                    r'tile(?P<tilepk>[0-9]{7})_'
                                    r'field(?P<fieldid>[0-9]{5})_'
                                    r'config.json$')
OBS_DEF_FILENAME_DTFMT = JSON_DTFORMAT_NAIVE

# Best time, earliest time, last time, tile_pk, config file name
OBS_IND_FILE_LINE = '%s %s %s %06d %s\n'
