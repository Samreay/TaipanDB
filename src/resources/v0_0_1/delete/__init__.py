"""
Functions for removing objects for the database.

Unless you know exactly what you're doing, you should use the submodules
of :any:`resources.v0_0_1.delete` to remove records from the database.
The database has a series
of CASCADEs built in to ake life easier for the code, which could lead to
unexpected/disasterous behaviour if you start deleting rows from the database
manually yourself.
"""