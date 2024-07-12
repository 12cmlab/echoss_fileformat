import pandas as pd


def print_table(df: pd.DataFrame, method, index=False, max_cols=20, max_rows=10, col_space=16, max_colwidth=24):
    method(table_to_string(df, index=index, max_cols=max_cols, max_rows=max_rows, col_space=col_space, max_colwidth=max_colwidth))


def table_to_string(df: pd.DataFrame, index=False, max_cols=20, max_rows=5, col_space=16, max_colwidth=24):
    """

    Args:
        df:

    Returns:

    """
    return '\n'+df.to_string(index=index, index_names=index, max_cols=max_cols, max_rows=max_rows, justify='left',
                             show_dimensions=True, col_space=col_space, max_colwidth=max_colwidth)+'\n'
