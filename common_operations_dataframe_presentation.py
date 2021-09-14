"""Module with auxiliary functions to perform operations on DataFrames to change it's presentation
(slice, translate, drop uniformative columns). All operations on DataFrame which don't add data to existing columns, 
create new columns with processed data or create new DataFrame"""


import pandas as pd
import numpy as np

from common_operations_servicefile import dct_from_columns, columns_import
from common_operations_dataframe import dct_from_dataframe

def dataframe_segmentation(dataframe_to_segment_df, dataframes_to_create_lst, report_columns_usage_dct, max_title):
    """Function to split aggregated table to required DataFrames
    As parameters function get DataFrame to be partitioned and
    list of allocated DataFrames names. Returns list of segmented DataFrames. 
    """

    # sheet name with customer report columns
    customer_report_columns_sheet = 'customer_report'
    # construct columns titles from data_names to use in dct_from_columns function
    if isinstance(dataframes_to_create_lst, str):
        dataframes_to_create_lst = [dataframes_to_create_lst]
    tables_names_lst = [
        [data_name.rstrip('_report') + '_eng', data_name.rstrip('_report')+'_ru'] 
        for data_name in dataframes_to_create_lst
        ]      

    chassis_column_usage = report_columns_usage_dct['chassis_info_usage']
    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']
    group_name_usage = report_columns_usage_dct.get('group_name_usage')

    # dictionary used to rename DataFrame english columns names to russian
    data_columns_names_dct = {}
    # for each data element from data_names list import english and russian columns title
    # data_name is key and two lists with columns names are values for data_columns_names_dct
    for dataframe_name, eng_ru_columns in zip(dataframes_to_create_lst, tables_names_lst):
        data_columns_names_dct[dataframe_name]  = \
            dct_from_columns(customer_report_columns_sheet, max_title, *eng_ru_columns, init_file = 'san_automation_info.xlsx')
    # construct english columns titles from tables_names_lst to use in columns_import function
    tables_names_eng_lst = [table_name_lst[0] for table_name_lst in tables_names_lst]
    # dictionary to extract required columns from aggregated DataFrame
    data_columns_names_eng_dct = {}
    # for each data element from data_names list import english columns title to slice main DataFrame 
    for dataframe_name, df_eng_column in zip(dataframes_to_create_lst, tables_names_eng_lst):
        # dataframe_name is key and list with columns names is value for data_columns_names_eng_dct
        data_columns_names_eng_dct[dataframe_name] = \
            columns_import(customer_report_columns_sheet, max_title, df_eng_column, init_file = 'san_automation_info.xlsx', display_status=False)
        # if no need to use chassis information in tables
        if not chassis_column_usage:
            if 'chassis_name' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_name')
            if 'chassis_wwn' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_wwn')
        # if there is only one Fabric no need to use Fabric name
        if not fabric_name_usage:
            for column in dataframe_to_segment_df.columns:
                if 'Fabric_name' in column and column in data_columns_names_eng_dct[dataframe_name]:
                    data_columns_names_eng_dct[dataframe_name].remove(column)
        # if device names correction applied then no need to use alias group name column
        if not group_name_usage:
            if 'Group_Name' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('Group_Name')
            
    # list with partitioned DataFrames
    segmented_dataframes_lst = []
    for dataframe_name in dataframes_to_create_lst:

        # df_columns_names_eng_lst = data_columns_names_eng_dct[dataframe_name]
        columns = dataframe_to_segment_df.columns.to_list()
        df_columns_names_eng_lst = [column for column in data_columns_names_eng_dct[dataframe_name] if column in columns]

        # get required columns from aggregated DataFrame
        # sliced_dataframe = dataframe_to_segment_df[data_columns_names_eng_dct[dataframe_name]].copy() # remove
        sliced_dataframe = dataframe_to_segment_df.reindex(columns = df_columns_names_eng_lst).copy()

        # translate columns to russian
        sliced_dataframe.rename(columns = data_columns_names_dct[dataframe_name], inplace = True)
        # add partitioned DataFrame to list
        segmented_dataframes_lst.append(sliced_dataframe)
    
    return segmented_dataframes_lst




def aggregated_to_report_dataframe(aggregated_df, df_name_to_create_lst, report_creation_info_lst):
    """Function to split aggregated table to required DataFrames
    As parameters function get DataFrame to be partitioned and
    list of allocated DataFrames names. Returns list of segmented DataFrames. 
    """

    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    *_, report_headers_df, report_columns_usage_dct = report_creation_info_lst

    # if single df_name in parameters
    if isinstance(df_name_to_create_lst, str):
        df_name_to_create_lst = [df_name_to_create_lst]
    
    # column names in report_headers_df containg header titles for each df_name
    header_names_eng_lst = [df_name + '_eng' for df_name in df_name_to_create_lst]
    
    # dictionary containing columns for each df_name from aggregated DataFrame
    report_header_eng_dct = {}
    for df_name, header_name_eng in zip(df_name_to_create_lst, header_names_eng_lst):
        report_header_eng_dct[df_name] = header_cleanup(report_headers_df, header_name_eng, report_columns_usage_dct)

    # list with partitioned DataFrames
    report_df_lst = []
    for df_name in df_name_to_create_lst:
        # identify columns which are in DataFrame
        df_header_eng = [column for column in report_header_eng_dct[df_name] if column in aggregated_df.columns]
        # get required columns from aggregated DataFrame
        report_df = aggregated_df.reindex(columns=df_header_eng).copy()
        # translate header to russian
        report_df = translate_report(report_df, report_headers_df, df_name)
        report_df_lst.append(report_df)
    return report_df if len(report_df_lst) == 1 else report_df_lst



def translate_report(df, report_headers_df, df_name=None, translate_header=True, 
                        translate_values=False, translate_columns=None):
    """Function to translate DataFrame header and values. By default translate header only.
    df_name identifies columns in report_headers_df used to create translate dictionary"""

    translated_df = df.copy()

    if df_name:
        translate_dct = dct_from_dataframe(report_headers_df, df_name + '_eng', df_name + '_ru')
    else:
        translate_dct = {'Yes': 'Да', 'No': 'Нет'}

    if translate_header:
        translated_df.rename(columns=translate_dct, inplace=True)
    if translate_values:
        if not translate_columns:
            translate_columns = translated_df.columns
        # columns which values need to be translated
        # translate values in column if column in DataFrame
        for column in translate_columns:
            if column in translated_df.columns:
                translated_df[column] = translated_df[column].replace(to_replace=translate_dct) 
    return translated_df



def header_cleanup(report_headers_df, header_name: str, report_columns_usage_dct) -> list:
    """Function to get DataFrame header from report_headers_df and drop excessive columns
    if they are not required"""

    column_usage_flags = [
        ('chassis_info_usage', ['chassis_name', 'chassis_wwn']),
        ('fabric_name_usage', ['Fabric_name']),
        ('group_name_usage', ['Group_Name'])
        ]

    header_sr = report_headers_df[header_name].dropna()

    # verify if any header titles need to be dropped
    dropped_columns = []
    for usage_flag, column in column_usage_flags:
        if report_columns_usage_dct.get(usage_flag):
            dropped_columns.extend(column)
    if dropped_columns:
        mask_dropped_columns = ~header_sr.isin(dropped_columns)
        header_sr = header_sr.loc[mask_dropped_columns]
    return header_sr.tolist()    


def translate_values(translated_df, translate_dct={'Yes': 'Да', 'No': 'Нет'}, translate_columns = None):
    """Function to translate values in corresponding columns"""

    if not translate_columns:
        translate_columns = translated_df.columns

    # columns which values need to be translated
    # translate values in column if column in DataFrame
    for column in translate_columns:
        if column in translated_df.columns:
            translated_df[column] = translated_df[column].replace(to_replace=translate_dct)

    return translated_df


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


def dataframe_slice_concatenate(df, column: str):
    """Function to create comparision DataFrame. 
    Initial DataFrame df is sliced based on unique values in designated column.
    Then sliced DataFrames concatenated horizontally which indexes were previously reset."""
    
    if not column in df.columns:
        print('\n')
        print(f"Column {column} doesn't exist")
        return df

    column_values = sorted(df[column].unique().tolist())
    sorted_df_lst = []
    for value in column_values:
        mask_value = df[column] == value
        tmp_df = df.loc[mask_value].copy()
        tmp_df.reset_index(inplace=True, drop=True)
        sorted_df_lst.append(tmp_df.copy())
    
    return pd.concat(sorted_df_lst, axis=1)


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


def move_column(df, cols_to_move, ref_col: str, place='after'):
    """Function to move column or columns in DataFrame after or before
    reference column"""
    
    if isinstance(cols_to_move, str):
        cols_to_move = [cols_to_move]
    
    cols = df.columns.tolist()    
    if place == 'after':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]
    
    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]
    
    return df[seg1 + seg2 + seg3]

