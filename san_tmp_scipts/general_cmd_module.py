# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 16:19:32 2022

@author: vlasenko
"""

import os
import sqlite3
import pandas as pd
import numpy as np






def merge_columns(df, summary_column: str, merge_columns: list, sep=', ', drop_merge_columns=True, sort_summary=False):
    """Function to concatenate values in several columns (merge_columns) into summary_column 
    with separator. If drop flag is True all merged columns except summary column are dropped"""
    
    df.reset_index(drop=True, inplace=True)
    merge_columns = [column for column in merge_columns if column in df.columns]
    if not merge_columns:
        return df
    df[summary_column] = df[merge_columns].stack().groupby(level=0).agg(sep.join)
    # drop merge_columns
    if drop_merge_columns:
        drop_columns = [column for column in merge_columns if column != summary_column]
        df.drop(columns=drop_columns, inplace=True)

    if sort_summary:
        sort_cell_values(df, summary_column, sep=sep)
    return df


def read_database(db_path, db_file, *args):
    """Function to read data from SQL.
    Args are comma separated DataFrames names.
    Returns list of loaded DataFrames or None if no data found.
    """

    db_filepath = os.path.join(db_path, db_file)

    # list to store loaded data
    data_imported = []
    conn = sqlite3.connect(db_filepath)

    for data_name in args:


        info = f'Reading {data_name} from database................'
        print(info, end="")
        data_name_in_db = conn.execute(
            f"""SELECT name FROM sqlite_master WHERE type='table' 
            AND name='{data_name}'; """).fetchall()
        if data_name_in_db:
            df = pd.read_sql(f"select * from {data_name}", con=conn)
            substitute_names(df)
            # revert single column DataFrame to Series
            if 'index' in df.columns:
                df.set_index('index', inplace=True)
                df = df.squeeze('columns')
            data_imported.append(df)
            print('ok')
        else:
            data_imported.append(None)
            print('no data')
    conn.close()
    return data_imported #if len(data_imported)>1 else data_imported[0]



def verify_read_data(max_title, data_names, *args,  show_status=True):
    """
    Function to verify if loaded DataFrame or Series contains 'NO DATA FOUND' information string.
    If yes then data converted to empty DataFrame otherwise remains unchanged.
    Function implemented to avoid multiple collection and analysis of parameters not applicable
    for the current SAN (fcr, ag, porttrunkarea) 
    """

    # *_, max_title = report_constant_lst
    
    # list to store verified data
    verified_data_lst = []
    for data_name, data_verified in zip(data_names, args):
        if show_status:
            info = f'Verifying {data_name}'
            print(info, end =" ")

        if not isinstance(data_verified, (pd.DataFrame, pd.Series)):
            if show_status:
                status_info('fail', max_title, len(info))
            print('\nWrong datatype for verification')
            exit()
        
        first_row = data_verified.iloc[0] if isinstance(data_verified, pd.DataFrame) else data_verified
        
        if (len(data_verified.index) == 1 and # have single row
            first_row.nunique() == 1 and # have single unique value
            'NO DATA FOUND' in data_verified.values): # and this value is 'NO DATA FOUND'
            if isinstance(data_verified, pd.DataFrame):
                columns = data_verified.columns
                data_verified = pd.DataFrame(columns=columns) # for DataFrame use empty DataFrame with column names only
                # data_verified = data_verified.iloc[0:0] # for DataFrame use empty DataFrame with column names only
            else:
                name = data_verified.name
                data_verified = pd.Series(name=name, dtype='object') # for Series use empty Series
            if show_status:
                status_info('empty', max_title, len(info))
        else:
            if show_status:
                status_info('ok', max_title, len(info))

        verified_data_lst.append(data_verified)
    return verified_data_lst


def status_info(status, max_title, len_info_string, shift=0):
    """Function to print current operation status ('OK', 'SKIP', 'FAIL')"""

    # information + operation status string length in terminal
    str_length = max_title + 80 + shift
    status = status.upper()
    # status info aligned to the right side
    # space between current operation information and status of its execution filled with dots
    print(status.rjust(str_length - len_info_string, '.'))
    return status


def substitute_names(df):
    """Function to avoid column names duplication in sql.
    Capital and lower case letters don't differ iin sql.
    Function adds tag to duplicated column name when writes to the database
    and removes tag when read from the database."""

    masking_tag = '_sql'
    duplicated_names = ['SwitchName', 'Fabric_Name', 'SwitchMode', 'Memory_Usage', 'Flash_Usage', 'Speed']
    replace_dct = {orig_name + masking_tag: orig_name for orig_name in duplicated_names}
    df.rename(columns=replace_dct, inplace=True)
    
    
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



def sort_cell_values(df, *args, sep=', '):
    """Function to sort values in cells of columns (args)"""
    
    for column in args:
        mask_notna = df[column].notna()
        df[column] = df.loc[mask_notna, column].str.split(sep).apply(sorted).str.join(sep).str.strip(',')


def dataframe_fillna(left_df, right_df, join_lst, filled_lst, remove_duplicates=True, drop_na=True):
    """
    Function to fill null values with values from another DataFrame with the same column names.
    Function accepts left Dataframe with null values, right DataFrame with filled values,
    list of columns join_lst used to join left and right DataFrames on,
    list of columns filled_lst where null values need to be filled. join_lst
    columns need to be present in left and right DataFrames. filled_lst must be present in right_df.
    If some columns from filled_lst missing in left_df it is added and the filled with values from right_df.
    If drop duplicate values in join columns of right DataFrame is not required pass remove_duplicates as False.
    If drop nan values in join columns in right DataFrame is not required pass drop_na as False.
    Function returns left DataFrame with filled null values in filled_lst columns 
    """

    # add missing columns to left_df from filled_lst if required
    left_df_columns_lst = left_df.columns.to_list()
    add_columns_lst = [column for column in filled_lst if column not in left_df_columns_lst]
    if add_columns_lst:
        left_df = left_df.reindex(columns = [*left_df_columns_lst, *add_columns_lst])

    # cut off unnecessary columns from right DataFrame
    right_join_df = right_df.loc[:, join_lst + filled_lst].copy()
    # drop rows with null values in columns to join on
    if drop_na:
        right_join_df.dropna(subset = join_lst, inplace = True)
    # if required (deafult) drop duplicates values from join columns 
    # to avoid rows duplication in left DataDrame
    if remove_duplicates:
        right_join_df.drop_duplicates(subset = join_lst, inplace = True)
    # rename columns with filled values for right DataFrame
    filled_join_lst = [name+'_join' for name in filled_lst]
    right_join_df.rename(columns = dict(zip(filled_lst, filled_join_lst)), inplace = True)
    # left join left and right DataFrames on join_lst columns
    left_df = left_df.merge(right_join_df, how = 'left', on = join_lst)
    # for each columns pair (w/o (null values) and w _join prefix (filled values)
    for filled_name, filled_join_name in zip(filled_lst, filled_join_lst):
        # copy values from right DataFrame column to left DataFrame if left value ios null 
        left_df[filled_name].fillna(left_df[filled_join_name], inplace = True)
        # drop column with _join prefix
        left_df.drop(columns = [filled_join_name], inplace = True)
    return left_df

def verify_columns_in_dataframe(df, columns):
    """Function to verify if columns are in DataFrame"""

    if not isinstance(columns, list):
        columns = [columns]
    return set(columns).issubset(df.columns)


def verify_value_occurence_in_series(value, series):
    """Function to count value occurrence in Series"""
     
    series_values_occurence = series.value_counts()
    if value in series_values_occurence:
        return series_values_occurence[value]
    
    
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

def concatenate_columns(df, summary_column: str, merge_columns: list, sep=', ', drop_merge_columns=True):
    """Function to concatenate values in several columns (merge_columns) into summary_column 
    as comma separated values"""
    
    # create summary column if not exist
    if summary_column not in df.columns:
        df[summary_column] = np.nan

    df['separator_symbol'] = sep
    merge_columns = [column for column in merge_columns if column in df.columns]
    
    if not merge_columns:
        return df

    for column in merge_columns:
        # value in summary column is empty
        mask_summary_note_empty = df[summary_column].isna()
        # value in current column is empty
        mask_current_note_empty = df[column].isna()
        """if value in summary column is empty take value from column to add (if it's not nan)
        if value in column to add is empty take value from summary note column (if it's not nan)
        if both values are empty use nan value
        if both values in summary and current columns exist then cancatenate them"""
        df[summary_column] = np.select(
            [mask_summary_note_empty, mask_current_note_empty, mask_summary_note_empty & mask_current_note_empty],
            [df[column], df[summary_column], np.nan],
            default=df[summary_column] + df['separator_symbol'] + df[column])
    # drop merge_columns
    if drop_merge_columns:
        df.drop(columns=merge_columns, inplace=True)
    df.drop(columns='separator_symbol', inplace=True)
    return df


def verify_group_symmetry(statistics_df, symmetry_grp, symmetry_columns, summary_column='Asymmetry_note'):
    """Function to verify if rows are symmetric in each symmetry_grp from
    symmetry_columns values point of view. Function adds Assysmetric_note to statistics_df.
    Column contains parameter name(s) for which symmetry condition is not fullfilled"""

    # drop invalid fabric labels
    mask_not_valid = statistics_df['Fabric_label'].isin(['x', '-'])
    # drop fabric summary rows (rows with empty Fabric_label)
    mask_fabric_label_notna = statistics_df['Fabric_label'].notna()
    statistics_cp_df = statistics_df.loc[~mask_not_valid & mask_fabric_label_notna].copy()
    
    # find number of unique values in connection_symmetry_columns
    symmetry_df = \
        statistics_cp_df.groupby(by=symmetry_grp)[symmetry_columns].agg('nunique')

    # temporary ineqaulity_notes columns for  connection_symmetry_columns
    symmetry_notes = [column + '_inequality' for column in symmetry_columns]
    for column, column_note in zip(symmetry_columns, symmetry_notes):
        symmetry_df[column_note] = np.nan
        # if fabrics are symmetric then number of unique values in groups should be equal to one 
        # mask_values_nonuniformity = symmetry_df[column] == 1
        mask_values_uniformity = symmetry_df[column].isin([0, 1])
        # use current column name as value in column_note for rows where number of unique values exceeds one 
        symmetry_df[column_note].where(mask_values_uniformity, column.lower(), inplace=True)
        
    # merge temporary ineqaulity_notes columns to Asymmetry_note column and drop temporary columns
    symmetry_df = concatenate_columns(symmetry_df, summary_column, 
                                                 merge_columns=symmetry_notes)
    # drop columns with quantity of unique values
    symmetry_df.drop(columns=symmetry_columns, inplace=True)
    # add Asymmetry_note column to statistics_df
    statistics_df = statistics_df.merge(symmetry_df, how='left', on=symmetry_grp)
    # clean notes for dropped fabrics
    if mask_not_valid.any():
        statistics_df.loc[mask_not_valid, summary_column] = np.nan
    return statistics_df


def find_df_differences(data_before_lst, data_after_lst, data_names):
    print('\nChecking equality ...\n')
    for df_name, before_df, after_df in zip(data_names, data_before_lst, data_after_lst):
        df_equality = after_df.equals(before_df)
        print(f"\n{df_name} {'EMPTY' if 'NO DATA FOUND' in before_df.iloc[0].values else 'FULL'}")
        print(f"{df_name} equals {df_equality}")
        if not df_equality:
            print("   column names are equal: ", before_df.columns.equals(after_df.columns))
            print("      Unmatched columns:")
            for column in before_df.columns:
                if not before_df[column].equals(after_df[column]):
                    print("        ", column)
                    


def remove_duplicates_from_string(df, *args, sep=', '):
    """Function to remove duplicates from strings in column"""
    
    for column in args:
        if df[column].notna().any() and df[column].str.contains(sep).any():
            df[column].fillna('nan_value', inplace=True)
            df[column] = df[column].str.split(sep).apply(set).str.join(sep)
            df[column].replace({'nan_value': np.nan}, inplace=True)
    return df

def load_and_compare(data_before_lst, db_path, db_file, data_names):

    data_after_lst = read_database(db_path, db_file, *data_names)
    find_df_differences(data_before_lst, data_after_lst, data_names)