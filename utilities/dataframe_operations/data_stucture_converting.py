"""Module with functions converting data structures to DataFrame or from DataFrame"""

import pandas as pd
from utilities.servicefile_operations import columns_import


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


def list_to_dataframe(header_lst, *args):
    """Function to export lists (args) to DataFrame with column titles from header_lst"""

    if len(args) == 1:
        header_lst = [header_lst]

    if len(args) != len(header_lst):
        print('Number of lists to convert to DataFrame and length of list of column titles are not equal')
        exit()
    return [pd.DataFrame(lst, columns=columns) for lst, columns in zip(args, header_lst)]

   
def list_from_dataframe(df, *args, drop_na=False):
    """Function to convert DataFrame columns to list of lists. 
    drop_na removes nan values from the list.
    if column doesn't exist list is empty"""

    
    if not drop_na:
        missing_columns = [column for column in args if column not in df.columns]
        if missing_columns:
            print(f"\nERROR. Column(s) {', '.join(missing_columns)} {'is' if len(missing_columns) == 1 else 'are'} missing in dataframe")
            exit()

            
            
    result = [df[column].dropna().tolist() for column in args]

    # if drop_na:
    #     result = [df[column].dropna().tolist() if column in df.columns else [] for column in args]
    # else:
    #     result = [df[column].tolist() if column in df.columns else [] for column in args]
    return result if len(args)>1 else result[0]


