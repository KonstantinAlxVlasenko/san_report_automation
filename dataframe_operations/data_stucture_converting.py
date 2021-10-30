"""Module with functions converting data structures to DataFrame or from DataFrame"""

import pandas as pd

from common_operations_servicefile import columns_import


def dct_from_dataframe(df, *args) -> dict:
    """Function to create dictionary from DataFrame columns. Args is column names.
    If only one column passed then dictionary with keys and empty lists as values created.
    If several columns imported then first column is keys of dictionary and others are values
    or list of values)
    """

    absent_columns = [column for column in args if column not in df.columns]    

    if absent_columns:
        print(f"{', '.join(absent_columns)} column{'s are' if len(absent_columns)>1 else ' is'} not in DataFrame" )

    current_df = df[list(args)].dropna(how='all')
    # if any values missing in DataFrame
    if current_df.isna().values.any():
        print(f'{args} columns have different length. Not able to create dictionary.')
        exit()

    keys = current_df[args[0]].tolist()

    # if one column is passed then create dictionary with keys and empty lists as values for each key
    if len(args) == 1:
        dct = dict((key, []) for key in keys)
    # if two columns passed then create dictionary of keys with one value for each key
    elif len(args) == 2:
        values = current_df[args[1]].tolist()
        dct ={key: value for key, value in zip(keys, values)}
    # if morte than two columns passed then create dictionary of keys with list of values for each key
    else:
        values = [current_df[arg].tolist() for arg in args[1:]]
        dct ={key: value for key, *value in zip(keys, *values)}
    return dct


def series_from_dataframe(df, index_column: str, value_column: str=None):
    """"Function to convert DataFrame to Series"""

    if len(df.columns) > 2:
        df = df[[index_column, value_column]].copy()
    else:
        df = df.copy()
    df.set_index(index_column, inplace=True)
    sr = df.squeeze()
    sr.name = value_column
    return  sr


def list_to_dataframe(data_lst, max_title, sheet_title_import=None, 
                        columns=columns_import, columns_title_import='columns'):
    """Function to export list to DataFrame and then save it to excel report file
    returns DataFrame
    """

    # checks if columns were passed to function as a list
    if isinstance(columns, list):
        columns_title = columns
    # if not (default) then import columns from excel file
    else:
        columns_title = columns(sheet_title_import, max_title, columns_title_import)
    data_df = pd.DataFrame(data_lst, columns=columns_title)
    return data_df