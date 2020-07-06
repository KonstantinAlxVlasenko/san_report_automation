"""
Module to create porterr, SFP and portcfg report tables
and add this information to aggregated_portcmd_df DataFrame
"""

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_segmentation
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import dataframe_import


def err_sfp_cfg_analysis_main(portshow_aggregated_df, sfpshow_df, portcfgshow_df, report_columns_usage_dct, report_data_lst):
    """Main function to add porterr, transceiver and portcfg information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['port_complete', 'Ошибки', 'Параметры_SFP', 'Параметры_портов']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    port_complete_df, error_report_df, sfp_report_df, portcfg_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['portshow_aggregated', 'sfpshow', 'portcfgshow', 'portcmd', 
                            'switchshow_ports', 'switch_params_aggregated', 'fdmi', 
                            'nscamshow', 'nsshow', 'alias', 'blade_servers', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title, analyzed_data_names)

    if force_run:
        # import transeivers information from file
        sfp_model_df = dataframe_import('sfp_models', max_title)        
        # current operation information string
        info = f'Updating connected devices table'
        print(info, end =" ") 
        # add sfpshow, transceiver information and portcfg to aggregated portcmd DataFrame
        port_complete_df = port_complete(portshow_aggregated_df, sfpshow_df, sfp_model_df, portcfgshow_df)
        # after finish display status
        status_info('ok', max_title, len(info))
        # create reaport tables from port_complete_df DataFrtame
        error_report_df, sfp_report_df, portcfg_report_df = \
            create_report_tables(port_complete_df, data_names[1:], report_columns_usage_dct, max_title)
        # saving data to json or csv file
        data_lst = [port_complete_df, error_report_df, sfp_report_df, portcfg_report_df]
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and reset DataFrame if yes
    else:
        port_complete_df, error_report_df, sfp_report_df, portcfg_report_df \
            = verify_data(report_data_lst, data_names, *data_lst)
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return port_complete_df


def port_complete(portshow_aggregated_df, sfpshow_df, sfp_model_df, portcfgshow_df):
    """Function to add sfpshow, transceiver information and portcfg to aggregated portcmd DataFrame """

    join_columns_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 'slot', 'port']
    # change column names and switch_index data type to correspond portshow_aggregated_df
    # pylint: disable=unbalanced-tuple-unpacking
    sfp_join_df, portcfg_join_df = align_dataframe(sfpshow_df, portcfgshow_df)

    # # in case of portshow_aggregated_df is loaded from saved file
    # # switch_index data type change from object to int required
    # portshow_aggregated_df.switch_index = portshow_aggregated_df.switch_index.astype('int64')

    # add sfpshow and transceiver model information to port_complete_df
    port_complete_df = portshow_aggregated_df.merge(sfp_join_df, how='left', on=join_columns_lst)
    port_complete_df = port_complete_df.merge(sfp_model_df, how='left', on=['Transceiver_PN'])
    # verify if transceiver supported in the switch
    port_complete_df['Transceiver_Supported'] = port_complete_df.apply(lambda series: verify_sfp_support(series), axis='columns')
    # add portcfgshow to port_complete_df
    port_complete_df = port_complete_df.merge(portcfg_join_df, how='left', on=join_columns_lst)

    return port_complete_df


def verify_sfp_support(series):
    """
    Function to check if transceiver is supported based 
    on transceiver part number and switch generation
    """
    
    # no transceiver installed
    if pd.isna(series['Transceiver_PN']):
        return np.nan
    # transceiver not found in imported 
    # transceiver information table
    if pd.isna(series['Transceiver_switch_gen']):
        return 'Unknown SFP'
    # switch generation is unknown
    if pd.isna(series['Generation']):
        return 'Unknown switch'
    # switch generation is in the supported list
    if series['Generation'] in series['Transceiver_switch_gen']:
        return 'Да'
    
    return 'Нет'


def align_dataframe(*args):
    """
    Function to rename switchname column and change switch_index 
    data type to int64 to merge changed DataFrame with aggregated DataFrame
    """
    
    join_df_lst = []
    for arg in args:
        join_df = arg.copy()
        # # convert switch_index data type to int
        # join_df.switch_index = join_df.switch_index.astype('int64')
        # rename switchname column
        join_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)
        join_df_lst.append(join_df)
    return join_df_lst


def create_report_tables(port_complete_df, data_names, report_columns_usage_dct, max_title):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    # partition aggregated DataFrame to required tables
    # pylint: disable=unbalanced-tuple-unpacking
    errors_report_df, sfp_report_df, portcfg_report_df = \
        dataframe_segmentation(port_complete_df, data_names, report_columns_usage_dct, max_title)
    # drop empty columns
    errors_report_df.dropna(axis=1, how = 'all', inplace=True)
    sfp_report_df.dropna(axis=1, how = 'all', inplace=True)
    portcfg_report_df.dropna(axis=1, how = 'all', inplace=True)

    return errors_report_df, sfp_report_df, portcfg_report_df
