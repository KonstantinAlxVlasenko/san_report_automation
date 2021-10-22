"""Module to count Fabric statistics"""

import pandas as pd
from common_operations_database import read_db, write_db
from common_operations_dataframe_presentation import (drop_all_identical,
                                                      drop_equal_columns,
                                                      drop_zero,
                                                      translate_dataframe)
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_table_report import dataframe_to_report

from .port_statistics_aggregation import port_statisctics_aggregated


def port_statistics_analysis(portshow_aggregated_df, report_creation_info_lst):
    """Main function to count Fabrics statistics"""

    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_sr = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['port_statistics', 'Статистика_портов']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # # unpacking DataFrames from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # fabric_statistics_df, fabric_statistics_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['portcmd', 'switchshow_ports', 'switch_params_aggregated', 'portshow_aggregated',
        'switch_parameters', 'chassis_parameters', 'fdmi', 'nscamshow', 'nsshow', 
            'alias', 'blade_servers', 'fabric_labels']

    # chassis_column_usage = report_columns_usage_dct['chassis_info_usage']
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Counting up Fabrics statistics'
        print(info, end =" ")  

        port_statistics_df = port_statisctics_aggregated(portshow_aggregated_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # get report DataFrame
        port_statistics_report_df = port_statistics_report(port_statistics_df, report_headers_df, report_columns_usage_sr)
        # create list with partitioned DataFrames
        data_lst = [port_statistics_df, port_statistics_report_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)      
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = verify_data(report_constant_lst, data_names, *data_lst)
        port_statistics_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
    return port_statistics_df


def port_statistics_report(port_statistics_df, report_headers_df, report_columns_usage_sr):
    """Function to create report table out of statistics_df DataFrame"""

    port_statistics_report_df = port_statistics_df.copy()
    port_statistics_report_df.drop(columns = ['switchWwn', 'N:E_int', 'N:E_bw_int'], inplace=True)
    # drop Not Licensed ports column if there are no any
    port_statistics_report_df = drop_all_identical(port_statistics_report_df, 
                                            columns_values={'Not_licensed': 0, 'Disabled': 0, 'Offline': 0}, dropna=False)
    # drop Licensed column if all ports are licensed
    port_statistics_report_df = drop_equal_columns(port_statistics_report_df, columns_pairs = [('Total_ports_number', 'Licensed')])
    # drop column 'chassis_name' if it is not required
    if not report_columns_usage_sr['chassis_info_usage']:
        port_statistics_report_df.drop(columns = ['chassis_name'], inplace=True)
    # port_statistics_report_df = translate_header(port_statistics_report_df, report_headers_df, 'Статистика_портов')
    port_statistics_report_df = translate_dataframe(port_statistics_report_df, report_headers_df, 'Статистика_портов')
    # drop visual uninformative zeroes
    drop_zero(port_statistics_report_df)
    return port_statistics_report_df
