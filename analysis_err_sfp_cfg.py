"""
Module to create porterr, SFP and portcfg report tables
and add this information to aggregated_portcmd_df DataFrame
"""

import numpy as np
import pandas as pd

from analysis_portshow_npiv import npiv_link_aggregated, npiv_statistics
from analysis_portshow_maps_ports import maps_db_ports
from common_operations_dataframe_presentation import (dataframe_segmentation,
                                                      drop_all_identical,
                                                      drop_equal_columns,
                                                      translate_values)
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (reply_request, status_info,
                                             verify_data, verify_force_run)
from common_operations_servicefile import (data_extract_objects,
                                           dataframe_import, dct_from_columns)
from common_operations_switch import statistics_report


def err_sfp_cfg_analysis_main(portshow_aggregated_df, switch_params_aggregated_df, sfpshow_df, portcfgshow_df, 
                                report_columns_usage_dct, report_data_lst):
    """Main function to add porterr, transceiver and portcfg information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst
    portshow_sfp_force_flag = False
    portshow_sfp_export_flag, *_ = report_steps_dct['portshow_sfp_aggregated']

    # names to save data obtained after current module execution
    data_names = ['portshow_sfp_aggregated', 'MAPS_ports', 'NPIV_', 'NPIV_statistics', 
                    'Ошибки', 'Параметры_SFP', 'Параметры_портов', 'MAPS_порты', 
                    'NPIV_порты', 'Статистика_NPIV']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    portshow_sfp_aggregated_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, \
        error_report_df, sfp_report_df, portcfg_report_df, maps_ports_report_df, \
            npiv_report_df, npiv_statistics_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['portshow_aggregated', 'sfpshow', 'portcfgshow', 'portcmd', 
                            'switchshow_ports', 'switch_params_aggregated', 'maps_parameters', 'fdmi', 
                            'device_rename', 'report_columns_usage_upd', 'nscamshow', 
                            'nsshow', 'alias', 'blade_servers', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title, analyzed_data_names)

    if force_run:

        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('common_regex', max_title)

        # import transeivers information from file
        sfp_model_df = dataframe_import('sfp_models', max_title)        
        # current operation information string
        info = f'Updating connected devices table and searching NPIV links'
        print(info, end =" ") 
        # add sfpshow, transceiver information and portcfg to aggregated portcmd DataFrame


        portshow_sfp_aggregated_df = port_complete(portshow_aggregated_df, sfpshow_df, sfp_model_df, portcfgshow_df)
        portshow_npiv_df = npiv_link_aggregated(portshow_sfp_aggregated_df, switch_params_aggregated_df)
        maps_ports_df = maps_db_ports(portshow_sfp_aggregated_df, switch_params_aggregated_df, re_pattern_lst)
        # after finish display status
        status_info('ok', max_title, len(info))

        info = f'Counting NPIV link statistics'
        print(info, end =" ") 
        npiv_statistics_df = npiv_statistics(portshow_npiv_df, re_pattern_lst)
        # after finish display status
        status_info('ok', max_title, len(info))

        # warning if UKNOWN SFP present
        if (portshow_sfp_aggregated_df['Transceiver_Supported'] == 'Unknown SFP').any():
            info_columns = ['Fabric_name', 'Fabric_label', 'configname', 'chassis_name', 'chassis_wwn', 'slot',	'port', 'Transceiver_Supported']
            portshow_sfp_info_df = portshow_sfp_aggregated_df.drop_duplicates(subset=info_columns).copy()
            unknown_count = len(portshow_sfp_info_df[portshow_sfp_info_df['Transceiver_Supported'] == 'Unknown SFP'])
            info = f'{unknown_count} {"port" if unknown_count == 1 else "ports"} with UNKNOWN supported SFP tag found'
            print(info, end =" ")
            status_info('warning', max_title, len(info))
            # ask if save portshow_aggregated_df
            if not portshow_sfp_export_flag:
                reply = reply_request("Do you want to save 'portshow_sfp_aggregated'? (y)es/(n)o: ")
                if reply == 'y':
                    portshow_sfp_force_flag = True

        # create reaport tables from port_complete_df DataFrtame
        error_report_df, sfp_report_df, portcfg_report_df, maps_ports_report_df, \
            npiv_report_df, npiv_statistics_report_df = \
            create_report_tables(portshow_sfp_aggregated_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, 
                                    data_names, report_columns_usage_dct, max_title)
        # saving data to json or csv file
        data_lst = [portshow_sfp_aggregated_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, 
                    error_report_df, sfp_report_df, portcfg_report_df, maps_ports_report_df,
                    npiv_report_df, npiv_statistics_report_df]
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and reset DataFrame if yes
    else:
        portshow_sfp_aggregated_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, error_report_df, \
            sfp_report_df, portcfg_report_df, maps_ports_report_df, npiv_report_df, npiv_statistics_report_df \
            = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [portshow_sfp_aggregated_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, 
                    error_report_df, sfp_report_df, portcfg_report_df, 
                    maps_ports_report_df, npiv_report_df, npiv_statistics_report_df]
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'portshow_sfp_aggregated':
            force_flag = portshow_sfp_force_flag
        save_xlsx_file(data_frame, data_name, report_data_lst, force_flag=force_flag)

    return portshow_sfp_aggregated_df


def port_complete(portshow_aggregated_df, sfpshow_df, sfp_model_df, portcfgshow_df):
    """Function to add sfpshow, transceiver information and portcfg to aggregated portcmd DataFrame """

    join_columns_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 'slot', 'port']

    # rename columns in portcfgshow_df duplicated in portshow_aggregated_df DataFrame except columns to merge on
    duplicate_columns = [column for column in portcfgshow_df.columns if 
                            (column in portshow_aggregated_df.columns and not column in join_columns_lst)]
    duplicate_columns_rename = [column + '_cfg' for column in duplicate_columns]
    rename_dct = {k:v for k, v in zip(duplicate_columns, duplicate_columns_rename)}
    portcfgshow_df.rename(columns=rename_dct, inplace=True)
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
    port_complete_df.drop_duplicates(inplace=True)
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
        join_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)
        join_df_lst.append(join_df)
    return join_df_lst


def create_report_tables(port_complete_df, maps_ports_df, portshow_npiv_df, npiv_statistics_df, 
                            data_names, report_columns_usage_dct, max_title):
    """Function to create required report DataFrames out of aggregated DataFrame"""


    data_names = ['portshow_sfp_aggregated', 'MAPS_ports', 'NPIV_', 'NPIV_statistics', 
                    'Ошибки', 'Параметры_SFP', 'Параметры_портов', 'MAPS_порты', 
                    'NPIV_порты', 'Статистика_NPIV']

    # partition aggregated DataFrame to required tables
    # pylint: disable=unbalanced-tuple-unpacking
    errors_report_df, sfp_report_df, portcfg_report_df = \
        dataframe_segmentation(port_complete_df, data_names[4:7], report_columns_usage_dct, max_title)
    
    # drop empty columns
    errors_report_df.dropna(axis=1, how = 'all', inplace=True)
    sfp_report_df.dropna(axis=1, how = 'all', inplace=True)
    portcfg_report_df.dropna(axis=1, how = 'all', inplace=True)

    maps_ports_report_df = drop_all_identical(maps_ports_df, 
                                                {'portState': 'Online', 'Connected_through_AG': 'No'},
                                                dropna=True)                   
    maps_ports_report_df, = dataframe_segmentation(maps_ports_report_df, data_names[7], report_columns_usage_dct, max_title)
    maps_ports_report_df.dropna(axis=1, how = 'all', inplace=True)
    maps_ports_report_df = translate_values(maps_ports_report_df)

    # remove rows with no sfp installed
    mask_sfp = ~sfp_report_df['Vendor Name'].str.contains('No SFP module', na=False)
    sfp_report_df = sfp_report_df.loc[mask_sfp]

    npiv_report_df = portshow_npiv_df.copy()
    # drop allna columns
    npiv_report_df.dropna(axis=1, how='all', inplace=True)
    # drop columns where all values after dropping NA are equal to certian value
    possible_identical_values = {'Slow_Drain_Device': 'No'}
    npiv_report_df = drop_all_identical(npiv_report_df, possible_identical_values, dropna=True)
    # if all devices connected to one fabric_label only
    npiv_report_df = drop_equal_columns(npiv_report_df, columns_pairs=[
                                                                ('Device_Host_Name_per_fabric_name_and_label', 'Device_Host_Name_per_fabric_label'),
                                                                ('Device_Host_Name_total_fabrics', 'Device_Host_Name_per_fabric_name')])

    npiv_report_df = translate_values(npiv_report_df)
    npiv_report_df, = dataframe_segmentation(npiv_report_df, data_names[8], report_columns_usage_dct, max_title)
    translate_dct = dct_from_columns('customer_report', max_title, 'Статистика_ISL_перевод_eng', 
                                'Статистика_ISL_перевод_ru', init_file = 'san_automation_info.xlsx')
    npiv_statistics_report_df = statistics_report(npiv_statistics_df, translate_dct, report_columns_usage_dct, 
                                                    drop_columns=['switchWwn', 'NodeName'])
    return errors_report_df, sfp_report_df, portcfg_report_df, maps_ports_report_df, npiv_report_df, npiv_statistics_report_df


# def npiv_statistics_report(npiv_statistics_df, report_columns_usage_dct, max_title):
#     """Function to create report table out of npiv_statistics_df DataFrame"""

#     npiv_statistics_report_df = pd.DataFrame()

#     if not npiv_statistics_df.empty:
#         chassis_column_usage = report_columns_usage_dct.get('chassis_info_usage')
#         translate_dct = dct_from_columns('customer_report', max_title, 'Статистика_ISL_перевод_eng', 
#                                         'Статистика_ISL_перевод_ru', init_file = 'san_automation_info.xlsx')
#         npiv_statistics_report_df = npiv_statistics_df.copy()
#         # identify columns to drop and drop columns
#         drop_columns = ['switchWwn', 'NodeName']
#         if not chassis_column_usage:
#             drop_columns.append('chassis_name')
#         drop_columns = [column for column in drop_columns if column in npiv_statistics_df.columns]
#         npiv_statistics_report_df.drop(columns=drop_columns, inplace=True)

#         # translate values in columns
#         translate_columns = [column for column in npiv_statistics_df.columns if 'note' in column and npiv_statistics_df[column].notna().any()]
#         translate_columns.extend(['Fabric_name', 'Trunking_lic_both_switches'])
#         npiv_statistics_report_df = translate_values(npiv_statistics_report_df, translate_dct, translate_columns)
#         # translate column names
#         npiv_statistics_report_df.rename(columns=translate_dct, inplace=True)
#         # drop empty columns
#         npiv_statistics_report_df.dropna(axis=1, how='all', inplace=True)
#     return npiv_statistics_report_df
