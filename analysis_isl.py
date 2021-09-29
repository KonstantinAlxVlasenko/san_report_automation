"""Module to generate 'InterSwitch links', 'InterFabric links' customer report tables"""


import numpy as np
import pandas as pd

from analysis_isl_aggregation import isl_aggregated
from analysis_isl_statistics import isl_statistics
from common_operations_dataframe_presentation import (drop_column_if_all_na,
                                                      generate_report_dataframe,
                                                      translate_dataframe,
                                                      translate_values, drop_zero)
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (force_extract_check, status_info,
                                             verify_data, verify_force_run)
from common_operations_servicefile import (data_extract_objects,
                                           dct_from_columns)
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db


def isl_main(fabricshow_ag_labels_df, switch_params_aggregated_df,  
            isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, 
            portcfgshow_df, switchshow_ports_df, report_creation_info_lst):
    """Main function to create ISL and IFR report tables"""
    
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['isl_aggregated', 'isl_statistics', 'Межкоммутаторные_соединения', 'Межфабричные_соединения', 'Статистика_ISL']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # # unpacking DataFrames from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # isl_aggregated_df, isl_statistics_df, isl_report_df, ifl_report_df, isl_statistics_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['isl', 'trunk', 'fcredge', 'lsdb', 'sfpshow', 'portcfgshow', 
                            'chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:

        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('common_regex', max_title)

        # current operation information string
        info = f'Generating ISL and IFL tables'
        print(info, end =" ")

        # get aggregated DataFrames
        isl_aggregated_df, fcredge_df = \
            isl_aggregated(fabricshow_ag_labels_df, switch_params_aggregated_df, 
            isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, re_pattern_lst)
        isl_statistics_df = isl_statistics(isl_aggregated_df, re_pattern_lst, report_constant_lst)
        # after finish display status
        status_info('ok', max_title, len(info))      

        # partition aggregated DataFrame to required tables
        
        # isl_report_df, = dataframe_segmentation(isl_aggregated_df, [data_names[2]], report_columns_usage_dct, max_title)
        isl_report_df = generate_report_dataframe(isl_aggregated_df, report_headers_df, report_columns_usage_dct, data_names[2]) 

        isl_report_df = translate_values(isl_report_df, translate_dct={'Yes': 'Да', 'No': 'Нет'})
        isl_report_df = drop_column_if_all_na(isl_report_df, columns=['Идентификатор транка', 'Deskew', 'Master', 'Идентификатор IFL'])
        # check if IFL table required
        if not fcredge_df.empty:
            # ifl_report_df, = dataframe_segmentation(fcredge_df, [data_names[3]], report_columns_usage_dct, max_title)
            ifl_report_df = generate_report_dataframe(fcredge_df, report_headers_df, report_columns_usage_dct, data_names[3]) 
        else:
            ifl_report_df = fcredge_df.copy()

        isl_statistics_report_df = isl_statistics_report(isl_statistics_df, report_headers_df, report_columns_usage_dct)

        # create list with partitioned DataFrames
        data_lst = [isl_aggregated_df, isl_statistics_df, isl_report_df, ifl_report_df, isl_statistics_report_df]
        # saving data to json or csv file
        # save_data(report_constant_lst, data_names, *data_lst)
        # writing data to sql
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        isl_aggregated_df, isl_statistics_df, isl_report_df, ifl_report_df, isl_statistics_report_df = \
            verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [isl_aggregated_df, isl_statistics_df, isl_report_df, ifl_report_df, isl_statistics_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)

    return isl_aggregated_df, isl_statistics_df


def isl_statistics_report(isl_statistics_df, report_headers_df, report_columns_usage_dct):
    """Function to create report table out of isl_statistics_df DataFrame"""

    # isl_statistics_df_report_df = pd.DataFrame('Фабрика', 'Подсеть',	'Имя шасси', 'Имя коммутатора')
    isl_statistics_df_report_df = pd.DataFrame()

    if not isl_statistics_df.empty:
        chassis_column_usage = report_columns_usage_dct.get('chassis_info_usage')
        # translate_dct = dct_from_columns('customer_report', max_title, 'Статистика_ISL_перевод_eng', 
        #                                 'Статистика_ISL_перевод_ru', init_file = 'san_automation_info.xlsx')
        isl_statistics_df_report_df = isl_statistics_df.copy()
        # identify columns to drop and drop columns
        drop_columns = ['switchWwn', 'Connected_switchWwn', 'sort_column_1', 'sort_column_2']
        if not chassis_column_usage:
            drop_columns.append('chassis_name')
        drop_columns = [column for column in drop_columns if column in isl_statistics_df.columns]
        isl_statistics_df_report_df.drop(columns=drop_columns, inplace=True)

        # translate values in columns and headers
        translated_columns = [column for column in isl_statistics_df.columns if 'note' in column and isl_statistics_df[column].notna().any()]
        translated_columns.extend(['Fabric_name', 'Trunking_lic_both_switches'])
        isl_statistics_df_report_df = translate_dataframe(isl_statistics_df_report_df, report_headers_df, 
                                                            'Статистика_ISL_перевод', translated_columns)
        # drop empty columns
        isl_statistics_df_report_df.dropna(axis=1, how='all', inplace=True)
        # remove zeroes to clean view
        # isl_statistics_df_report_df.replace({0: np.nan}, inplace=True)
        drop_zero(isl_statistics_df_report_df)
    return isl_statistics_df_report_df
