"""Module to create sensor related DataFrames"""

import numpy as np
import pandas as pd


from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dct_from_columns
from common_operations_dataframe import dataframe_segmentation


def sensor_analysis_main(sensor_df, switch_params_aggregated_df, report_columns_usage_dct, report_data_lst):
    """Main function to analyze zoning configuration"""
        
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['sensor_aggregated', 'Датчики']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    sensor_aggregated_df, sensor_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['switch_params_aggregated', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating sensor readings table'
        print(info, end =" ") 

        # aggregated DataFrames
        sensor_aggregated_df = sensor_aggregation(sensor_df, switch_params_aggregated_df, report_data_lst)

        # after finish display status
        status_info('ok', max_title, len(info))

        # report tables
        sensor_report_df = sensor_report(sensor_aggregated_df, data_names, report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [sensor_aggregated_df, sensor_report_df]
        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        sensor_aggregated_df, sensor_report_df = verify_data(report_data_lst, data_names, *data_lst)

        data_lst = [sensor_aggregated_df, sensor_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return sensor_aggregated_df


def sensor_aggregation(sensor_df, switch_params_aggregated_df, report_data_lst):
    """
    Function to label switches in portshow_aggregated_df with Fabric names and labels.
    Add switchState, switchMode and Generation information
    """

    # add Fabric labels from switch_params_aggregated_df Fataframe
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 
                        'Fabric_name', 'Fabric_label', 'switchType']
    # create left DataFrame for join operation
    switchparams_aggregated_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    # portshow_aggregated_df and switchparams_join_df DataFrames join operation
    sensor_aggregated_df = sensor_df.merge(switchparams_aggregated_join_df, how = 'left', on = switchparams_lst[:5])

    sort_sensor_lst = ['Fabric_label', 'Fabric_name', 'switchType', 'switchName']
    sensor_aggregated_df.sort_values(by=sort_sensor_lst, \
        ascending=[True, True, False, True], inplace=True)

    return sensor_aggregated_df


def sensor_report(sensor_aggregated_df, data_names, report_columns_usage_dct, max_title):
    
    
    # loading values to translate
    translate_dct = dct_from_columns('customer_report', max_title, 'Датчики_перевод_eng', 
                                        'Датчики_перевод_ru', init_file = 'san_automation_info.xlsx')
    sensor_report_df = translate_values(sensor_aggregated_df, translate_dct, max_title)

    sensor_report_df, = dataframe_segmentation(sensor_aggregated_df, data_names[1:], report_columns_usage_dct, max_title)

    return sensor_report_df


def translate_values(translated_df, translate_dct, max_title):
    """Function to translate values in corresponding columns"""

    # columns which values need to be translated
    translate_columns = ['Type', 'Status', 'Vlaue', 'Unit']
    # translate values in column if column in DataFrame
    for column in translate_columns:
        if column in translated_df.columns:
            translated_df[column] = translated_df[column].replace(to_replace=translate_dct)

    return translated_df