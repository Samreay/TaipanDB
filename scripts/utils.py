# Helper function for getting string inputs to PSQL to work correctly


def str_psql(x):
    if isinstance(x, str):
        return "'%s'" % (x,)
    return str(x)


def generate_conditions_string(conditions):
    """
    Generate a conditions string for use with making PSQL queries.

    Parameters
    ----------
    conditions:
        A list of three-tuples defining conditions, e.g.:
        [(column, condition, value), ...]
        Column must be a table column name. Condition must be a *string* of a
        valid PSQL comparison (e.g. '=', '<=', 'LIKE' etc.). Value should be in
        the correct Python form relevant to the table column.

    Returns
    -------
    conditions_string:
        The string of PSQL conditions which can be inserted into a query string.
        Note that the string will *not* contain a leading WHERE clause, to allow
        multiple conditions strings (some possibly not generated by this
        function) to be combined more easily.
    """

    for i in range(len(conditions)):
        x = conditions[i]
        conditions[i] = (str(x[0]), str(x[1]), str_psql(x[2]))

    conditions_string = ' AND '.join([' '.join(x)
                                      for x in conditions])
    return conditions_string
