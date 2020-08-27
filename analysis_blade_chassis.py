"""
Module to create Blade IO modules report table
and add Device_Location column to blade modules DataFrame
"""


import pandas as pd

from common_operations_dataframe import dataframe_segmentation
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dataframe_import


def blademodule_analysis(blade_module_df, report_data_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['blade_module_loc', 'Blade_шасси']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    blade_module_loc_df, blade_module_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['blade_interconnect']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating blade modules location table'
        print(info, end =" ") 

        # create DataFrame with Device_Location column
        blade_module_loc_df = blademodule_location(blade_module_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # create Blade chassis report table
        blade_module_report_df = blademodule_report(blade_module_df, data_names, max_title)
        # create list with partitioned DataFrames
        data_lst = [blade_module_loc_df, blade_module_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        blade_module_loc_df, blade_module_report_df = \
            verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [blade_module_loc_df, blade_module_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return blade_module_loc_df


# auxiliary lambda function to combine two columns in DataFrame
# it combines to columns if both are not null and takes second if first is null
# str1 and str2 are strings before columns respectively (default is whitespace between columns)
wise_combine = lambda series, str1='', str2=' ': \
    str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
        if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]


def blademodule_location(blade_module_df):
    """Function to add Device_Location column to Blade chassis DataFrame"""

    # add Device_Location column to DataFrame
    columns_lst = [*blade_module_df.columns.to_list(), 'Device_Location']
    blade_module_loc_df = blade_module_df.reindex(columns = columns_lst)

    if not blade_module_df.empty:
        # combine 'Enclosure_Name' and 'Bay' columns
        blade_module_loc_df['Device_Location'] = \
            blade_module_loc_df[['Enclosure_Name', 'Interconnect_Bay']].apply(wise_combine, axis=1, args=('Enclosure ', ' bay '))

    return blade_module_loc_df


def blademodule_report(blade_module_df, data_names, max_title):
    """Function to create Blade IO modules report table"""

    report_columns_usage_dct = {'fabric_name_usage': False, 'chassis_info_usage': False}

    columns_lst = [*blade_module_df.columns.to_list(), 'FW_Supported', 'Recommended_FW'] # remove
    blade_modules_prep_df = blade_module_df.reindex(columns = columns_lst) # remove

    # pylint: disable=unbalanced-tuple-unpacking
    blade_module_report_df, = dataframe_segmentation(blade_modules_prep_df, data_names[1:], report_columns_usage_dct, max_title)

    return blade_module_report_df
