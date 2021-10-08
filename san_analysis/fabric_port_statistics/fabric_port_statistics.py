"""Module to count Fabric statistics"""

import pandas as pd

from .fabric_port_statistics_aggregation import fabric_port_statisctics_aggregated
from common_operations_dataframe_presentation import (drop_all_identical,
                                                      drop_equal_columns,
                                                      drop_zero,
                                                      translate_header)
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db


def fabric_port_statistics_analysis(portshow_aggregated_df, switchshow_ports_df, fabricshow_ag_labels_df, 
                            nscamshow_df, portshow_df, report_creation_info_lst):
    """Main function to count Fabrics statistics"""

    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['fabric_ports_statistics', 'Статистика_портов']
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

        fabric_statistics_df = fabric_port_statisctics_aggregated(portshow_aggregated_df, switchshow_ports_df, 
                                                    fabricshow_ag_labels_df, nscamshow_df, portshow_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # get report DataFrame
        fabric_statistics_report_df = statistics_report(fabric_statistics_df, report_headers_df, report_columns_usage_dct)
        # create list with partitioned DataFrames
        data_lst = [fabric_statistics_df, fabric_statistics_report_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)      
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        fabric_statistics_df, fabric_statistics_report_df = \
            verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [fabric_statistics_df, fabric_statistics_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
        
    return fabric_statistics_df


def statistics_total(statistics_df, report_headers_df):
    """Function to get Fabric level statistics"""

    # calculating statistics for each fabric
    # Fabric_label level statistics
    statistics_total_df = statistics_df.copy()
    # Fabric_name level statistics
    statistics_subtotal_df = statistics_df.copy()
    # grouping all switches by fabric names and labels 
    # and apply sum function to each group
    statistics_total_df = statistics_total_df.groupby([statistics_total_df.Fabric_name, statistics_total_df.Fabric_label]).sum()
    statistics_subtotal_df = statistics_subtotal_df.groupby([statistics_subtotal_df.Fabric_name]).sum()
    # reset index to aviod Multi indexing after groupby operation
    statistics_total_df.reset_index(inplace = True)
    statistics_subtotal_df.reset_index(inplace = True)
    
    statistics_total_df = pd.concat([statistics_total_df, statistics_subtotal_df], ignore_index=True)
    statistics_total_df.drop(statistics_total_df.index[statistics_total_df['Fabric_name'] == 'Итого:'], inplace = True)

    # droping columns with N:E ration data due to it pointless on fabric level
    statistics_total_df.drop(columns=['N:E_int', 'N:E_bw_int'], inplace=True)
    # re-calculate percentage of occupied ports
    statistics_total_df['%_occupied'] = round(statistics_total_df.Online.div(statistics_total_df.Licensed)*100, 1)
    statistics_total_df = statistics_total_df.astype('int64', errors = 'ignore')
    
    # statistics_total_report_df = statistics_total_df.rename(columns = statistic_columns_names_dct)
    statistics_total_report_df = translate_header(statistics_total_df, report_headers_df, 'Статистика_фабрики')

    return statistics_total_report_df


def statistics_report(statistics_df, report_headers_df, report_columns_usage_dct):
    """Function to create report table out of statistics_df DataFrame"""

    chassis_column_usage = report_columns_usage_dct.get('chassis_info_usage')

    # create statitics report DataFrame
    statistics_report_df = statistics_df.copy()
    # drop columns 'switch_index', 'switchWwn', 'N:E_int', 'N:E_bw_int'
    statistics_report_df.drop(columns = ['switch_index', 'switchWwn', 'N:E_int', 'N:E_bw_int'], inplace=True)
    # drop Not Licensed ports column if there are no any
    statistics_report_df = drop_all_identical(statistics_report_df, 
                                            columns_values={'Not_licensed': 0}, dropna=True)
    # drop Licensed column if all ports are licensed
    statistics_report_df = drop_equal_columns(statistics_report_df, columns_pairs = [('Total_ports_number', 'Licensed')])
    # drop column 'chassis_name' if it is not required
    if not chassis_column_usage:
        statistics_report_df.drop(columns = ['chassis_name'], inplace=True)
    
    statistics_report_df = translate_header(statistics_report_df, report_headers_df, 'Статистика_фабрики')

    # get summary statistics for each fabric
    statistics_total_report_df = statistics_total(statistics_df, report_headers_df)
    # add summary fabric level statistics to switch level statistics
    statistics_report_df = pd.concat([statistics_report_df, statistics_total_report_df], ignore_index=True)
    statistics_report_df.sort_values(by=['Фабрика', 'Подсеть', 'Имя коммутатора'], inplace=True)
    # drop visual uninformative zeroes
    drop_zero(statistics_report_df)
    return statistics_report_df
