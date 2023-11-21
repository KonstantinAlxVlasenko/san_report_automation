"""Module with auxiliary functions to perform operations on DataFrames to change it's presentation
(slice, move columns). All operations on DataFrame which don't add data to existing columns, 
create new columns with processed data or create new DataFrame"""

import re
import pandas as pd

from .dataframe_details import verify_columns_in_dataframe
import utilities.data_structure_operations as dsop 

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
    # df = df[~mask_all].append(df[mask_all]).reset_index(drop=True)
    df = pd.concat([df[~mask_all], df[mask_all]], ignore_index=True)
    return df


def move_column(df, cols_to_move, ref_col: str, place='after'):
    """Function to move column or columns in DataFrame after or before
    reference column"""
    
    if isinstance(cols_to_move, str):
        cols_to_move = [cols_to_move]

    # verify if relocated columns are in df
    cols_to_move = [column for column in cols_to_move if column in df.columns]
    
    if not cols_to_move:
        return df
    
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


def swap_columns(df, column1, column2):
    """Function to swap two columns locations on DataFrame"""
     
    if not verify_columns_in_dataframe(df, columns=[column1, column2]):
        return df
    
    columns_lst = df.columns.to_list()
    # find columns indexes
    column1_idx, column2_idx = columns_lst.index(column1), columns_lst.index(column2)
    # swap column names in list
    columns_lst[column2_idx], columns_lst[column1_idx] = columns_lst[column1_idx], columns_lst[column2_idx]
    # reorder DataFrame columns
    df = df[columns_lst].copy()
    return df


def rename_columns(df, column_name_pattern):
    """Function to rename df columns according to column_name_pattern.
    If name duplication occurs '_' symbol is added until column name is unique"""
    
    original_columns = [column for column in df.columns if re.search(column_name_pattern, column)]
    renamed_columns = [re.search(column_name_pattern, column).group(1) for column in original_columns]
    validation_lst = []
    for i, column in enumerate(renamed_columns):
        column_name_changed = False
        while column in validation_lst:
            column = column + "_"
            column_name_changed = True
        validation_lst.append(column)
        if column_name_changed:
            renamed_columns[i] = column
            
    rename_dct = dict(zip(original_columns, renamed_columns))
    df.rename(columns=rename_dct, inplace=True)
    return renamed_columns


def sort_fabric_swclass_swtype_swname(switch_df, switch_columns, fabric_columns=['Fabric_name', 'Fabric_label']):
    """Function to sort swithes in fabric. SwitchType (model) is sorted in descending order
    so newest models are on the top of the list"""
    
    sort_columns = fabric_columns + ['switchClass_weight', 'switchType'] + switch_columns
    ascending_flags = [True] * len(fabric_columns) + [True, False] + [True] * len(switch_columns)
    switch_df.sort_values(by=sort_columns, ascending=ascending_flags, inplace=True)


def concatenate_dataframes_vertically(*args):
    """Function to concatenate DataFrames vertically.
    Summary DataFrame have columns of all concatenated DataFrames"""

    # concatenate all non empty DataFrames
    non_empty_dfs_lst = [df for df in args if not df.empty]
    if non_empty_dfs_lst:
        concatenated_df = pd.concat([df for df in args if not df.empty])
        concatenated_df.reset_index(drop=True, inplace=True)
    else:
        concatenated_df = pd.DataFrame()

    # get columns of all DataFrames
    dfs_columns_lst = [df.columns.to_list() for df in args]
    dfs_columns_lst = dsop.flatten(dfs_columns_lst)
    dfs_columns_lst = dsop.remove_diplicates_from_list(dfs_columns_lst)

    # add absent columns if any DataFrame from args is empty
    concatenated_df = concatenated_df.reindex(columns=dfs_columns_lst)
    return concatenated_df