"""Module with auxiliary functions to perform operations on DataFrames to change it's presentation
(slice, move columns). All operations on DataFrame which don't add data to existing columns, 
create new columns with processed data or create new DataFrame"""

import pandas as pd


def dataframe_slice_concatenate(df, column: str, char: str=' '):
    """Function to create comparision DataFrame. 
    Initial DataFrame df is sliced based on unique values in designated column.
    Then sliced DataFrames concatenated horizontally which indexes were previously reset.
    char is the symbol used to rename columns by adding value from column sliced on to the name
    of the Dataframe columns"""
    
    if not column in df.columns:
        print('\n')
        print(f"Column {column} doesn't exist")
        return df

    column_values = sorted(df[column].unique().tolist())
    # list with Dataframes sliced on column
    sorted_df_lst = []
    for value in column_values:
        mask_value = df[column] == value
        tmp_df = df.loc[mask_value].copy()
        tmp_df.reset_index(inplace=True, drop=True)
        # add value to all column names of sliced DataFrame
        rename_dct = {column: column + char + str(value) for column in tmp_df.columns}
        tmp_df.rename(columns=rename_dct, inplace=True)
        # add sliced DataFrame to the list
        sorted_df_lst.append(tmp_df.copy())
    return pd.concat(sorted_df_lst, axis=1)


def move_all_down(df):
    """Function to move total row All to the bottom of the DataFrame"""
    
    mask_all = df['Fabric_name'] == 'All'
    df = df[~mask_all].append(df[mask_all]).reset_index(drop=True)
    return df


def move_column(df, cols_to_move, ref_col: str, place='after'):
    """Function to move column or columns in DataFrame after or before
    reference column"""
    
    if isinstance(cols_to_move, str):
        cols_to_move = [cols_to_move]

    # verify if relocated columns are in df
    cols_to_move = [column for column in cols_to_move if column in df.columns]
    
    cols = df.columns.tolist()    
    if place == 'after':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]
    
    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]
    return df[seg1 + seg2 + seg3].copy()


