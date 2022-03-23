"""
Module to create porterr, SFP and portcfg report tables
and add this information to aggregated_portcmd_df DataFrame
"""

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def port_err_sfp_cfg_analysis(portshow_aggregated_df, sfpshow_df, portcfgshow_df,
                                project_constants_lst):
    """Main function to add porterr, transceiver and portcfg information to portshow DataFrame"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst
    
    portshow_sfp_force_flag = False
    portshow_sfp_export_flag = project_steps_df.loc['portshow_sfp_aggregated', 'export_to_excel']

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'port_err_sfp_cfg_analysis_out', 'port_err_sfp_cfg_analysis_in')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data    
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title, analyzed_data_names)

    if force_run:
        # import transeivers information from file
        sfp_model_df = sfop.dataframe_import('sfp_models', max_title)        
        # current operation information string
        info = f'Updating connected devices table and searching NPIV links'
        print(info, end =" ") 
        # add sfpshow, transceiver information and portcfg to aggregated portcmd DataFrame
        portshow_sfp_aggregated_df = port_complete(portshow_aggregated_df, sfpshow_df, sfp_model_df, portcfgshow_df)
        # link_reset_df, crc_good_eof_df, fec_df, pcs_blk_df, link_failure_df, discard_df, enc_crc_df, bad_eof_df, bad_os_df = port_error_filter(portshow_sfp_aggregated_df)
        filtered_error_lst = port_error_filter(portshow_sfp_aggregated_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))

        # warning if UKNOWN SFP present
        if (portshow_sfp_aggregated_df['Transceiver_Supported'] == 'Unknown SFP').any():
            info_columns = ['Fabric_name', 'Fabric_label', 'configname', 'chassis_name', 'chassis_wwn', 'slot',	'port', 'Transceiver_Supported']
            portshow_sfp_info_df = portshow_sfp_aggregated_df.drop_duplicates(subset=info_columns).copy()
            unknown_count = len(portshow_sfp_info_df[portshow_sfp_info_df['Transceiver_Supported'] == 'Unknown SFP'])
            info = f'{unknown_count} {"port" if unknown_count == 1 else "ports"} with UNKNOWN supported SFP tag found'
            print(info, end =" ")
            meop.status_info('warning', max_title, len(info))
            # ask if save portshow_aggregated_df
            if not portshow_sfp_export_flag:
                reply = meop.reply_request("Do you want to save 'portshow_sfp_aggregated'? (y)es/(n)o: ")
                if reply == 'y':
                    portshow_sfp_force_flag = True

        # create report tables from port_complete_df DataFrtame
        report_lst = portshow_report_main(portshow_sfp_aggregated_df, data_names, report_headers_df, report_columns_usage_sr)
        data_lst = [portshow_sfp_aggregated_df, *report_lst, *filtered_error_lst]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and reset DataFrame if yes
    else:    
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        portshow_sfp_aggregated_df, *_ = data_lst
    
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'portshow_sfp_aggregated':
            force_flag = portshow_sfp_force_flag
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst, force_flag=force_flag)
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
    """Function to rename switchname column and change switch_index 
    data type to int64 to merge changed DataFrame with aggregated DataFrame"""
    
    join_df_lst = []
    for arg in args:
        join_df = arg.copy()
        # # convert switch_index data type to int
        # join_df.switch_index = join_df.switch_index.astype('int64')
        join_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)
        join_df_lst.append(join_df)
    return join_df_lst


def portshow_report_main(port_complete_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    data_names = ['portshow_sfp_aggregated', 'Ошибки', 'Параметры_SFP', 'Параметры_портов']
    # add speed value column for fillword verification in errors_report_df
    port_complete_df['speed_fillword'] = port_complete_df['speed']
    errors_report_df, sfp_report_df, portcfg_report_df = \
        dfop.generate_report_dataframe(port_complete_df, report_headers_df, report_columns_usage_sr, *data_names[1:])
    # drop empty columns
    errors_report_df.dropna(axis=1, how = 'all', inplace=True)
    sfp_report_df.dropna(axis=1, how = 'all', inplace=True)
    portcfg_report_df.dropna(axis=1, how = 'all', inplace=True)

    report_lst = [errors_report_df, sfp_report_df, portcfg_report_df]
    return report_lst


def port_error_filter(portshow_sfp_aggregated_df, error_threshhold_num: int=100, error_threshold_percenatge: int=3):
    """Function to create Dataframes with port errors group if any error in the group exceeds the threshold
    (for critical error group threshold is 100 errors for medium error group percentage from the number of
    received frames added"""

    filtered_error_lst = []

    stat_frx = 'stat_frx'

    medium_errors = [
        ['Link_failure', 'Loss_of_sync', 'Loss_of_sig'],
        ['er_rx_c3_timeout', 'er_tx_c3_timeout', 'er_unroutable', 'er_unreachable', 'er_other_discard'],
        ['er_enc_in', 'er_enc_out', 'er_crc', 'er_bad_os'], 
        ['er_bad_eof']
        ]

    critical_errors = [
        ['Lr_in', 'Lr_out', 'Ols_in',	'Ols_out'], 
        ['er_crc_good_eof'], 
        ['fec_uncor_detected'], 
        ['er_pcs_blk']
        ]

    # convert error and received frames columns to numeric type
    errors_flat = [error for error_grp in [*critical_errors, *medium_errors] for error in error_grp]
    portshow_sfp_aggregated_df[[stat_frx, *errors_flat]] = portshow_sfp_aggregated_df[[stat_frx, *errors_flat]].apply(pd.to_numeric, errors='ignore')

    # create column with medium error percentage from number of received frames
    medium_errors_flat = [error for error_grp in medium_errors for error in error_grp]
    for err_column in medium_errors_flat:
        err_percentage_column = err_column + '_percentage'
        portshow_sfp_aggregated_df[err_percentage_column] = (portshow_sfp_aggregated_df[err_column] / portshow_sfp_aggregated_df[stat_frx]) * 100
        portshow_sfp_aggregated_df[err_percentage_column] = portshow_sfp_aggregated_df[err_percentage_column].round(2)

    switch_columns = ['Fabric_name', 'Fabric_label', 
                        'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn',
                        'portIndex', 'slot', 'port', 'switchName_Index_slot_port', 'portState', 'portType',
                        'Device_Host_Name_Port_group', 'alias_Port_group', 'stat_frx']

    # verify critical errors which exceeds threshold
    for error_grp in critical_errors:
        mask_errors_num = (portshow_sfp_aggregated_df[error_grp] > error_threshhold_num).any(axis=1)
        filtered_error_df = portshow_sfp_aggregated_df.loc[mask_errors_num, [*switch_columns, *error_grp]]
        filtered_error_df.drop_duplicates(inplace=True)
        filtered_error_lst.append(filtered_error_df)

    # verify medium errors which exceeds thresholds
    for error_grp in medium_errors:
        mask_errors_num = (portshow_sfp_aggregated_df[error_grp] > error_threshhold_num).any(axis=1)
        error_grp_percantage = [error + '_percentage' for error in error_grp]
        mask_errors_percentage = (portshow_sfp_aggregated_df[error_grp_percantage] > error_threshold_percenatge).any(axis=1)
        filtered_error_df = portshow_sfp_aggregated_df.loc[mask_errors_num & mask_errors_percentage, [*switch_columns, *error_grp, *error_grp_percantage]]
        filtered_error_df.drop_duplicates(inplace=True)
        filtered_error_lst.append(filtered_error_df)

    return filtered_error_lst
