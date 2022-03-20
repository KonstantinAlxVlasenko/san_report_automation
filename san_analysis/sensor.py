"""Module to create sensor related DataFrames"""

import numpy as np
import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop

# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def sensor_analysis(sensor_df, switch_params_aggregated_df, project_constants_lst):
    """Main function to analyze zoning configuration"""
        
    # # report_steps_dct contains current step desciption and force and export tags
    # # report_headers_df contains column titles, 
    # # report_columns_usage_sr show if fabric_name, chassis_name and group_name of device ports should be used
    # report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_sr = report_creation_info_lst
    # # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    # *_, max_title = report_constant_lst

    project_steps_df, max_title, data_dependency_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # names to save data obtained after current module execution
    data_names = ['sensor_aggregated', 'Датчики']
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    
    # reade data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # list of data to analyze from report_info table
    analyzed_data_names = ['switch_params_aggregated', 'fabric_labels', 'sensor']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating sensor readings table'
        print(info, end =" ") 

        # aggregated DataFrames
        sensor_aggregated_df = sensor_aggregation(sensor_df, switch_params_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # report tables
        sensor_report_df = sensor_report(sensor_aggregated_df, report_headers_df, report_columns_usage_sr, data_names)
        # create list with partitioned DataFrames
        data_lst = [sensor_aggregated_df, sensor_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst) 

    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        sensor_aggregated_df, *_ = data_lst


    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)

    return sensor_aggregated_df


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


def sensor_report(sensor_aggregated_df, report_headers_df, report_columns_usage_sr, data_names):
    """Function to create report Datafrmae from sensor_aggregated_df 
    (slice and reorder columns, translate values in columns)"""

    sensor_report_df = dfop.generate_report_dataframe(sensor_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[1])
    sensor_report_df = dfop.translate_values(sensor_report_df, report_headers_df, data_names[1], 
                                        translated_columns = ['Type', 'Status', 'Value', 'Unit'])
    return sensor_report_df
