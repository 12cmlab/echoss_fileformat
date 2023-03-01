import pandas as pd
from tabulate import tabulate
from wcwidth import wcswidth


def get_col_widths(df: pd.DataFrame):
    """
    Get the width of each column in a pandas dataframe.
    """
    col_widths = []
    for col in df.columns:
        # Get the width of the column name
        width = wcswidth(str(col))
        # Get the maximum width of the column values
        max_width = df[col].astype(str).apply(wcswidth).max()
        # Set the column width to the maximum of the column name and values
        col_widths.append(max(width, max_width))
    return col_widths


def print_dataframe0(df: pd.DataFrame, method):
    """
    Print a pandas dataframe in a table-like format.
    """
    # Get the column widths
    col_widths = get_col_widths(df)

    # Output the header row
    header = '  '.join([f'{col:<{col_widths[i]}}' for i, col in enumerate(df.columns)])
    method(header)

    # Output the separator row
    separator = '-' * (sum(col_widths) + len(col_widths) * 2 - 1)
    method(separator)

    # Output the data rows
    for row in df.values:
        row_str = '  '.join([f'{cell:<{col_widths[i]}}' for i, cell in enumerate(row)])
        method(row_str)


def print_dataframe(df, method):
    """
        다른 대안
    """
    # Get column widths
    # widths = [max(map(len, df[col].astype(str))) for col in df.columns]

    # Get the column widths - 비교
    widths = get_col_widths(df)

    sep = '|'
    # Print column headers
    # for i, col in enumerate(df.columns):
    #    method(f"{col:<{widths[i]}}", end='|')
    # method()
    head_line = sep.join([f"{col:<{width}}" for width, col in zip(widths, df.columns)])

    # Output the separator row
    plus = '+'
    separator = plus.join([f"{'-'*(width+1)}" for width, col in zip(widths, df.columns)])
    # separator = '-' * (sum(widths) + len(widths) * 2 - 1)
    method(plus+separator+plus)
    method(sep+head_line+sep)
    method(plus + separator + plus)

    # Print rows
    for row in df.itertuples(index=False):
        # for i, col in enumerate(df.columns):
        #   print(f"{row[i]:<{widths[i]}}", end=' ')
        row_line = sep.join([f"{row[i]:<{widths[i]}}" for i, col in enumerate(df.columns)])
        method(sep+row_line+sep)
    method(plus + separator + plus)


def print_table(df: pd.DataFrame, method, index=False, max_cols=20, max_rows=5, col_space=16, max_colwidth=24):
    method(table_to_string(df, index=index, max_cols=max_cols, max_rows=max_rows, col_space=col_space, max_colwidth=max_colwidth))


def table_to_string(df: pd.DataFrame, index=False, max_cols=20, max_rows=5, col_space=16, max_colwidth=24):
    """

    Args:
        df:

    Returns:

    """
    return '\n'+df.to_string(index=index, index_names=index, max_cols=max_cols, max_rows=max_rows, justify='left',
                             show_dimensions=True, col_space=col_space, max_colwidth=max_colwidth)+'\n'


def print_taburate(df: pd.DataFrame, method):
    # Convert DataFrame to list of lists
    data = df.values.tolist()

    # Print table
    method(tabulate(data, headers=df.columns, tablefmt='pipe'))