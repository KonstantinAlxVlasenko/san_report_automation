""""""

import pandas as pd
import numpy as np

from .dataframe_presentation import move_column


def drop_column_if_all_na(df, columns: list):
    """Function to drop columns if all values are nan"""

    clean_df = df.copy()
    if isinstance(columns, str):
        columns = [columns]

    for column in columns:
        if column in df.columns and df[column].isna().all():
            clean_df.drop(columns=[column], inplace=True)
    return clean_df


def drop_all_identical(df, columns_values: dict, dropna=False):
    """Function to drop columns where all values are equal to certian value.
    dropna parameter defines if nan values for each column should be droppped 
    or not before its checking"""

    clean_df = df.copy()    

    for column, value in columns_values.items():
        if column in df.columns:
            if dropna and (df[column].dropna() == value).all():
                clean_df.drop(columns=[column], inplace=True)
            elif not dropna and (df[column] == value).all():
                clean_df.drop(columns=[column], inplace=True)
    return clean_df


def drop_equal_columns(df, columns_pairs: list):
    """Function to drop one of two columns if both have equal values.
    Parameter columns_pairs is a list of tuples. Each tuple contains 
    two columns to check"""

    clean_df = df.copy()
    columns_dropped_lst = []
    for column_main, column_dropped in columns_pairs:
        if column_main in df.columns and column_dropped in df.columns:
            if df[column_main].equals(df[column_dropped]):
                columns_dropped_lst.append(column_dropped)

    if columns_dropped_lst:
        clean_df.drop(columns=columns_dropped_lst, inplace=True)   
    return clean_df


def drop_equal_columns_pairs(df, columns_main: list, columns_droped: list, dropna=False):
    """Function to check if values from columns_main and columns_droped columns are respectively equal to each other.
    If they are then drop columns_droped columns. dropna parameter defines if nan values in columns_droped
    columns should be dropped or not before checking"""

    clean_df = df.copy()
    # create DataFrame copy in case if dropna is required
    check_df = df.copy()
    # check if columns are in the DataFrame
    columns_main = [column for column in columns_main if column in check_df.columns]
    columns_droped = [column for column in columns_droped if column in check_df.columns]
    
    if len(columns_main) != len(columns_droped):
        print('Checked and main columns quantity must be equal')
        exit()

    if dropna:
        check_df.dropna(subset = columns_droped, inplace=True)

    # by default columns are droped
    drop_columns = True
    # if any pair of checked columns are not equal then columns are not droped
    for column_main, column_droped in zip(columns_main, columns_droped):
        if not check_df[column_main].equals(check_df[column_droped]):
            drop_columns = False

    if drop_columns:
        clean_df.drop(columns=columns_droped, inplace=True)
    
    return clean_df


def remove_duplicates_from_column(df, column: str, duplicates_subset: list=None, 
                                    duplicates_free_column_name: str=None, place='after', drop_orig_column=False):
    """Function to create column with dropped duplicates based on subset.
    Column name with dropped duplicates is derived from original column name
    plus duplicates_free_suffix. New column is rellocated to the position
    next to original column"""

    if duplicates_subset is None:
        duplicates_subset = df.columns.tolist()
    if duplicates_free_column_name is None:
        duplicates_free_column_name = column + '_duplicates_free'

    # create zone_duplicates_free column with no duplicated values
    df[duplicates_free_column_name] = np.nan
    mask_duplicated = df.duplicated(subset=duplicates_subset, keep='first')
    df[duplicates_free_column_name] = df[duplicates_free_column_name].where(mask_duplicated, df[column])

    # rellocate duplicates free column right after original column
    df = move_column(df, cols_to_move=duplicates_free_column_name, ref_col=column, place=place)

    if drop_orig_column:
        df.drop(columns=[column], inplace=True)
    return df


def drop_zero(df):
    """Function to remove zeroes from DataFrame for clean view"""

    df.replace({0: np.nan}, inplace=True)
    return df


def remove_duplicates_from_string(df, *args, sep=', '):
    """Function to remove duplicates from strings in column"""
    
    for column in args:
        if df[column].notna().any() and df[column].str.contains(sep).any():
            df[column].fillna('nan_value', inplace=True)
            df[column] = df[column].str.split(sep).apply(set).str.join(sep)
            df[column].replace({'nan_value': np.nan}, inplace=True)
    return df


def remove_value_from_string(df, removed_value: str, *args, sep=', '):
    """Function to remove removed_value from strings in columns (args)"""

    for column in args:
        if df[column].notna().any():
            df[column].replace(f'{removed_value}(?:{sep})?', value='', regex=True, inplace=True)
            df[column] = df[column].str.rstrip(sep)
    return df


