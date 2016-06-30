import logging
from ....scripts.extract import extract_from, extract_from_joined


def execute(cursor, conditions, tables_to_join=None):
    """
    Read out a simple list of tile PKs according to certain conditions
    Parameters
    ----------
    cursor:
        psycopg2 cursor for interacting with the database
    conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column. Defaults to None,
        so all rows will be returned.
    tables_to_join:
        Optional. List of other tables required to join to the tile table
        in order to evaluate conditions. Defaults to None.

    Returns
    -------
    tile_pks:
        List of tile primary keys matching the conditions passed.
    """
    # Input checking
    if tables_to_join is not None:
        if len(tables_to_join) == 0:
            tables_to_join = None
        else:
            if not np.all([isinstance(c, str) for c in conditions]):
                raise ValueError('tables_to_join must be a list of table '
                                 'names (i.e. strings)')

    if tables_to_join:
        return extract_from_joined(cursor, ['tile'] + tables_to_join,
                                   conditions=conditions, columns=['tile_pk'])
    else:
        return extract_from(cursor, 'tile', conditions=conditions,
                            columns=['tile_pk'])


