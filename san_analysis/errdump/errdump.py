"""Module to parse errdump log messages and find events which frequenly appeared (more then 3 times per month) 
within a period of six month prior to the switch configuration collection date """


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .errdump_aggregation import errdump_aggregated
from .errdump_statistics import errdump_statistics


def errdump_analysis(errdump_df, switchshow_df, switch_params_aggregated_df, 
                portshow_aggregated_df, project_constants_lst):
    """Main function to get most frequently appeared log messages"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'errorlog_analysis_out', 'errorlog_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('raslog_split', max_title)
        raslog_message_details_df = sfop.dataframe_import('raslog_details', max_title)
        raslog_message_id_details_df = sfop.dataframe_import('raslog_id_details', max_title, columns=['Message_ID', 'Details', 'Recommended_action'])

        # current operation information string
        info = f'Counting RASLog messages statistics'
        print(info, end =" ")

        # get aggregated DataFrames
        errdump_aggregated_df = errdump_aggregated(errdump_df, switchshow_df, switch_params_aggregated_df, 
                                                    portshow_aggregated_df, pattern_dct)
        # count how many times event appears during one month for the last six months 
        raslog_counter_df, raslog_frequent_df = errdump_statistics(errdump_aggregated_df, raslog_message_details_df, raslog_message_id_details_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))      
        # partition aggregated DataFrame to required tables
        raslog_report_df = raslog_report(raslog_frequent_df, data_names, report_headers_df, report_columns_usage_sr)

        # create list with partitioned DataFrames
        data_lst = [errdump_aggregated_df, raslog_counter_df, raslog_report_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        errdump_aggregated_df, raslog_counter_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return errdump_aggregated_df, raslog_counter_df


def raslog_report(raslog_frequent_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to check if it is required to use chassis_name columns. RASLog sometimes uses it's own
    chname not equal to switchname or chassis name thus it's better to keep default chassis names
    for visibility even if it was allowed to drop chassiss_name column before"""

    # make copy of default report_columns_usage_sr in order to avoid change it
    report_columns_usage_upd_sr = report_columns_usage_sr.copy()
    chassis_column_usage = report_columns_usage_upd_sr['chassis_info_usage']
    # if chassis_name column to be dropped
    if not chassis_column_usage:
        # if all switchnames and chassis names are not identical
        if not all(raslog_frequent_df.chassis_name == raslog_frequent_df.switchName):
            # change keep chassis_name column tag to True 
            report_columns_usage_upd_sr['chassis_info_usage'] = 1

    raslog_report_df = report.generate_report_dataframe(raslog_frequent_df, report_headers_df, report_columns_usage_upd_sr, data_names[2])
    raslog_report_df.dropna(axis=1, how = 'all', inplace=True)
    report.drop_slot_value(raslog_report_df, report_columns_usage_sr)
    return raslog_report_df



