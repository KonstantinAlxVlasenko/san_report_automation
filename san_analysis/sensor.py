"""Module to create sensor related DataFrames"""


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report


def sensor_analysis(sensor_df, switch_params_aggregated_df, project_constants_lst):
    """Main function to analyze zoning configuration"""
        
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'sensor_analysis_out', 'sensor_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data 
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
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
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

    sensor_report_df = report.generate_report_dataframe(sensor_aggregated_df, report_headers_df, report_columns_usage_sr, data_names[1])
    sensor_report_df = dfop.translate_values(sensor_report_df, report_headers_df, data_names[1], 
                                        translated_columns = ['Type', 'Status', 'Value', 'Unit'])
    return sensor_report_df
