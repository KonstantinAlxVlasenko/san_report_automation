"""Module with auxiliary finctions to perform operations on DataFrames"""

import os
import pandas as pd
from files_operations import columns_import, dct_from_columns, dataframe_import


def report_entry_values(max_title):
    """
    Function to import entry report values:
    customer_name, hardware configuration files, directory to save report 
    """

    report_entry_df = dataframe_import('report', max_title, 'report_info.xlsx', ['name', 'value'], 'name')

    customer_name = report_entry_df.loc['customer_name', 'value']
    project_folder = os.path.normpath(report_entry_df.loc['project_folder', 'value'])
    ssave_folder = os.path.normpath(report_entry_df.loc['supportsave_folder', 'value'])
    blade_folder = os.path.normpath(report_entry_df.loc['blade_showall_folder', 'value'])

    return customer_name, project_folder, ssave_folder, blade_folder


def dataframe_join(left_df, right_df, columns_lst, columns_join_index = None):
    """Auxiliary function to join DataFrames
    Function take as parameters two DataFrames, 
    list with names in right DataFrame with, index which used to separate columns names which join operation performed on
    from columns with infromation to join 
    """

    right_join_df = right_df.loc[:, columns_lst].copy()
    # left join on switch columns
    left_df = left_df.merge(right_join_df, how = 'left', on = columns_lst[:columns_join_index])
    # columns names for connected switch 
    columns_connected_lst = ['Connected_' + column_name for column_name in columns_lst]
    # dictionary to rename columns in right DataFrame
    rename_dct = dict(zip(columns_lst, columns_connected_lst))
    # rename columns in right DataFrame
    right_join_df.rename(columns = rename_dct, inplace = True)
    # left join connected switch columns
    left_df = left_df.merge(right_join_df, how = 'left', on = columns_connected_lst[:columns_join_index])
    
    return left_df


def dataframe_segmentation(dataframe_to_segment_df, dataframes_to_create_lst, report_columns_usage_dct, max_title):
    """Function to split aggregated table to required DataFrames
    As parameters function get DataFrame to be partitioned and
    list of allocated DataFrames names. Returns list of segmented DataFrames 
    """

    # sheet name with customer report columns
    customer_report_columns_sheet = 'customer_report'
    # construct columns titles from data_names to use in dct_from_columns function
    tables_names_lst = [
        [data_name.rstrip('_report') + '_eng', data_name.rstrip('_report')+'_ru'] 
        for data_name in dataframes_to_create_lst
        ]      

    chassis_column_usage = report_columns_usage_dct['chassis_info_usage']
    fabric_name_usage = report_columns_usage_dct['fabric_name_usage']

    # dictionary used to rename DataFrame english columns names to russian
    data_columns_names_dct = {}
    # for each data element from data_names list import english and russian columns title
    # data_name is key and two lists with columns names are values for data_columns_names_dct
    for dataframe_name, eng_ru_columns in zip(dataframes_to_create_lst, tables_names_lst):
        data_columns_names_dct[dataframe_name]  = \
            dct_from_columns(customer_report_columns_sheet, max_title, *eng_ru_columns, init_file = 'san_automation_info.xlsx')

    # construct english columns titles from tables_names_lst to use in columns_import function
    tables_names_eng_lst = [table_name_lst[0] for table_name_lst in tables_names_lst]
    # dictionary to extract required columns from aggregated DataFrame f_s_c_m_i
    data_columns_names_eng_dct = {}
    # for each data element from data_names list import english columns title
    for dataframe_name, df_eng_column in zip(dataframes_to_create_lst, tables_names_eng_lst):
        # dataframe_name is key and list with columns names is value for data_columns_names_eng_dct
        data_columns_names_eng_dct[dataframe_name] = columns_import(customer_report_columns_sheet, max_title, df_eng_column, init_file = 'san_automation_info.xlsx')
        # if no need to use chassis information in tables
        if not chassis_column_usage:
            if 'chassis_name' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_name')
            if 'chassis_wwn' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_wwn')
        # if there is only one Fabric no need to use Fabric name
        if not fabric_name_usage:
            if 'Fabric_name' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('Fabric_name')
            
    # list with partitioned DataFrames
    segmented_dataframes_lst = []
    for dataframe_name in dataframes_to_create_lst:
        # get required columns from aggregated DataFrame
        sliced_dataframe = dataframe_to_segment_df[data_columns_names_eng_dct[dataframe_name]].copy()

        # translate columns to russian
        sliced_dataframe.rename(columns = data_columns_names_dct[dataframe_name], inplace = True)
        # add partitioned DataFrame to list
        segmented_dataframes_lst.append(sliced_dataframe)

    return segmented_dataframes_lst