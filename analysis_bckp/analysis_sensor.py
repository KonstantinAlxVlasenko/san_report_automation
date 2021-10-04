"""Module to create sensor related DataFrames"""

import numpy as np
import pandas as pd

from common_operations_dataframe_presentation import (
    generate_report_dataframe,
    translate_values, translate_dataframe)
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db


def sensor_analysis_main(sensor_df, switch_params_aggregated_df, report_creation_info_lst):
    """Main function to analyze zoning configuration"""
        
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['sensor_aggregated', 'Датчики']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # # unpacking DataFrames from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # sensor_aggregated_df, sensor_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['switch_params_aggregated', 'fabric_labels', 'sensor']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating sensor readings table'
        print(info, end =" ") 

        # aggregated DataFrames
        sensor_aggregated_df = sensor_aggregation(sensor_df, switch_params_aggregated_df)

        # after finish display status
        status_info('ok', max_title, len(info))

        # report tables
        sensor_report_df = sensor_report(sensor_aggregated_df, report_headers_df, report_columns_usage_dct, data_names)

        # create list with partitioned DataFrames
        data_lst = [sensor_aggregated_df, sensor_report_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst) 

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        sensor_aggregated_df, sensor_report_df = verify_data(report_constant_lst, data_names, *data_lst)

        data_lst = [sensor_aggregated_df, sensor_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)

    return sensor_aggregated_df

# TO_REMOVE function is changed to avoid duplication entries
# def sensor_aggregation(sensor_df, switch_params_aggregated_df, report_constant_lst):
#     """
#     Function to label switches in portshow_aggregated_df with Fabric names and labels.
#     Add switchState, switchMode and Generation information
#     """

#     # add Fabric labels from switch_params_aggregated_df Fataframe
#     # columns labels reqiured for join operation
#     switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
#                         'switchName', 'switchWwn', 
#                         'Fabric_name', 'Fabric_label', 'switchType']
#     # create left DataFrame for join operation
#     switchparams_aggregated_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
#     # portshow_aggregated_df and switchparams_join_df DataFrames join operation
#     sensor_aggregated_df = sensor_df.merge(switchparams_aggregated_join_df, how = 'left', on = switchparams_lst[:5])

#     sort_sensor_lst = ['Fabric_label', 'Fabric_name', 'switchType', 'switchName']
#     sensor_aggregated_df.sort_values(by=sort_sensor_lst, \
#         ascending=[True, True, False, True], inplace=True)

#     return sensor_aggregated_df


def sensor_aggregation(sensor_df, switch_params_aggregated_df):
    """
    Function to label switches in portshow_aggregated_df with Fabric names and labels.
    Add switchState, switchMode and Generation information
    """

    # add Fabric labels from switch_params_aggregated_df Fataframe
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 
                        'Fabric_name', 'Fabric_label', 'Generation', 'switchType']
    
    # dictionary of functions to use for aggregating the data.
    agg_fn_dct = {key: lambda x: ', '.join(sorted(set(x))) for key in switchparams_lst[3:-1]}
    agg_fn_dct['switchType'] = 'first'
    
    # group switch information for each chassis to avoid sensor information duplication 
    switchparams_aggregated_join_df = switch_params_aggregated_df.groupby(by=switchparams_lst[:3]).agg(agg_fn_dct)
    switchparams_aggregated_join_df.reset_index(inplace=True)
    
    # add chassis and switch information to sensor DataFrame
    sensor_aggregated_df = sensor_df.merge(switchparams_aggregated_join_df, how = 'left', on = switchparams_lst[:3])

    sort_sensor_lst = ['Fabric_label', 'Fabric_name', 'Generation', 'switchType', 'chassis_name']
    sensor_aggregated_df.sort_values(by=sort_sensor_lst, \
        ascending=[True, True, False, False, True], inplace=True)

    return sensor_aggregated_df



# def sensor_report(sensor_aggregated_df, data_names, report_columns_usage_dct, max_title):
#     """Function to create report Datafrmae from sensor_aggregated_df 
#     (slice and reorder columns, translate values in columns)"""
    
#     # loading values to translate
#     translate_dct = dct_from_columns('customer_report', max_title, 'Датчики_перевод_eng', 
#                                         'Датчики_перевод_ru', init_file = 'san_automation_info.xlsx')
#     sensor_report_df = translate_values(sensor_aggregated_df, translate_dct=translate_dct, 
#                                             translate_columns = ['Type', 'Status', 'Value', 'Unit']) 
#     # translate_values(sensor_aggregated_df, translate_dct, max_title)
#     sensor_report_df, = dataframe_segmentation(sensor_aggregated_df, data_names[1:], report_columns_usage_dct, max_title)
#     return sensor_report_df



def sensor_report(sensor_aggregated_df, report_headers_df, report_columns_usage_dct, data_names):
    """Function to create report Datafrmae from sensor_aggregated_df 
    (slice and reorder columns, translate values in columns)"""

    # # report_headers_df contains column titles, 
    # *_, report_headers_df, _ = report_creation_info_lst

    # sensor_report_df = aggregated_to_report_dataframe(sensor_aggregated_df,  data_names[1], report_headers_df, report_columns_usage_dct)

    sensor_report_df = generate_report_dataframe(sensor_aggregated_df, report_headers_df, report_columns_usage_dct, data_names[1])

    sensor_report_df = translate_values(sensor_report_df, report_headers_df, data_names[1], 
                                        translated_columns = ['Type', 'Status', 'Value', 'Unit'])

    
    return sensor_report_df