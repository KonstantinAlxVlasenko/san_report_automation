"""Module to identify Fabric devices in portshow DataFrame"""

import pandas as pd
import numpy as np

from .portcmd_aggregation import portshow_aggregated
from .portcmd_device_connection_statistics import \
    device_connection_statistics
from .portcmd_devicename_correction import devicename_correction_main
from .portcmd_storage import storage_connection_statistics
from common_operations_dataframe import count_group_members, merge_columns
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (reply_request, status_info,
                                             verify_data, verify_force_run)
from common_operations_servicefile import (data_extract_objects,
                                           dataframe_import)
from .report_portcmd import portcmd_report_main
from common_operations_table_report import dataframe_to_report
from common_operations_database import read_db, write_db
from common_operations_dataframe_presentation import remove_duplicates_from_string, remove_value_from_string


def portcmd_analysis(portshow_df, switchshow_ports_df, switch_params_df, 
                            switch_params_aggregated_df, isl_aggregated_df, 
                            nsshow_df, nscamshow_df, nsshow_dedicated_df,
                            ag_principal_df, porttrunkarea_df, 
                            alias_df, fdmi_df, blade_module_df, 
                            blade_servers_df, blade_vc_df, 
                            synergy_module_df, synergy_servers_df, 
                            system_3par_df, port_3par_df, 
                            report_creation_info_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_steps_dct contains current step desciption and force and export tags
    # report_headers_df contains column titles, 
    # report_columns_usage_dct show if fabric_name, chassis_name and group_name of device ports should be used
    report_constant_lst, report_steps_dct, report_headers_df, report_columns_usage_dct = report_creation_info_lst
    # report_constant_lst contains information: customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = [
        'portshow_aggregated', 'storage_connection_statistics', 'device_connection_statistics', 
        'device_rename', 'report_columns_usage_upd', 
        'Серверы', 'Массивы', 'Библиотеки', 'Микрокоды_HBA', 
        'Подключение_массивов', 'Подключение_библиотек', 'Подключение_серверов', 
        'Статистика_массивов', 'Статистика_устройств'
        ]
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # report_columns_usage_bckp = report_columns_usage_dct
    
    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    # reade data from database if they were saved on previos program execution iteration
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)
    
    # flag to forcible save portshow_aggregated_df if required
    portshow_force_flag = False
    
    device_rename_df = data_lst[3]
    nsshow_unsplit_df = pd.DataFrame()

    # on the first iteration report_columns_usage_upd is None 
    # so use report_columns_usage backup from report_creation_info_lst
    
    # if not report_columns_usage_dct.empty:
    #     report_columns_usage_dct = report_columns_usage_bckp

    # if report_columns_usage_dct is None:
    #     report_columns_usage_dct = report_columns_usage_bckp

    # if report_columns_usage_dct.empty:
    #     report_columns_usage_dct = report_columns_usage_bckp

    # list of data to analyze from report_info table
    analyzed_data_names = ['portcmd', 'switchshow_ports', 'switch_params_aggregated', 
                            'switch_parameters', 'chassis_parameters', 'fdmi', 'nscamshow', 
                            'nsshow', 'alias', 'blade_servers', 'synergy_servers', 'fabric_labels', 'isl', 
                            'trunk', 'isl_aggregated', 'Параметры_SFP', 'portshow_sfp_aggregated']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # import data with switch models, firmware and etc
        switch_models_df = dataframe_import('switch_models', max_title)
        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('nameserver', max_title)

        oui_df = dataframe_import('oui', max_title, columns=['Connected_oui', 'type', 'subtype'])

        # current operation information string
        info = f'Generating connected devices table'
        print(info, end =" ") 
        

        portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df = \
            portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_df, 
                                switch_params_aggregated_df, isl_aggregated_df, nsshow_df, nsshow_dedicated_df,
                                nscamshow_df, ag_principal_df, porttrunkarea_df, switch_models_df, alias_df, 
                                oui_df, fdmi_df, blade_module_df,  blade_servers_df, blade_vc_df, 
                                synergy_module_df, synergy_servers_df, system_3par_df, port_3par_df,
                                re_pattern_lst)

        # after finish display status
        status_info('ok', max_title, len(info))
        # show warning if any UNKNOWN device class founded, if any PortSymb or NodeSymb is not parsed,
        # if new switch founded
        portshow_force_flag, nsshow_unsplit_force_flag, expected_ag_links_force_flag = \
            warning_notification(portshow_aggregated_df, switch_params_aggregated_df, 
            nsshow_unsplit_df, expected_ag_links_df, report_steps_dct, max_title)        
        # correct device names manually
        portshow_aggregated_df, device_rename_df = \
            devicename_correction_main(portshow_aggregated_df, device_rename_df, report_creation_info_lst)
        # merge 'Device_Host_Name' and 'Device_port', create column with all Device_Host_Name for port each port
        portshow_aggregated_df = device_names_per_port(portshow_aggregated_df)
        # count Device_Host_Name instances for fabric_label, label and total in fabric
        portshow_aggregated_df = device_ports_per_group(portshow_aggregated_df)
        # sort rows
        portshow_aggregated_df = sort_portshow(portshow_aggregated_df)
        # count device connection statistics
        info = f'Counting device connection statistics'
        print(info, end =" ")
        storage_connection_statistics_df = storage_connection_statistics(portshow_aggregated_df, re_pattern_lst)
        device_connection_statistics_df = device_connection_statistics(portshow_aggregated_df)    
        status_info('ok', max_title, len(info))

        servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df, \
                storage_connection_statistics_report_df, device_connection_statistics_report_df  = \
                    portcmd_report_main(portshow_aggregated_df, storage_connection_statistics_df, 
                                            device_connection_statistics_df, data_names[5:-2], 
                                            report_headers_df, report_columns_usage_dct)
        # create list with partitioned DataFrames
        data_lst = [
            portshow_aggregated_df, storage_connection_statistics_df, device_connection_statistics_df, 
            device_rename_df, report_columns_usage_dct, 
            servers_report_df, storage_report_df, library_report_df, hba_report_df, 
            storage_connection_df, library_connection_df, server_connection_df, 
            storage_connection_statistics_report_df, device_connection_statistics_report_df
            ]

        # writing data to sql
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)

        dataframe_to_report(nsshow_unsplit_df, 'nsshow_unsplit', report_creation_info_lst, force_flag = nsshow_unsplit_force_flag)
        dataframe_to_report(expected_ag_links_df, 'expected_ag_links', report_creation_info_lst, force_flag = expected_ag_links_force_flag)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        # portshow_aggregated_df, storage_connection_statistics_df, device_connection_statistics_df, \
        #     device_rename_df, report_columns_usage_dct, \
        #         servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        #             storage_connection_df, library_connection_df, server_connection_df, \
        #                 storage_connection_statistics_report_df, device_connection_statistics_report_df \
        #                     = verify_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [
        #     portshow_aggregated_df, storage_connection_statistics_df, device_connection_statistics_df, 
        #     device_rename_df, report_columns_usage_dct, 
        #     servers_report_df, storage_report_df, library_report_df, hba_report_df, 
        #     storage_connection_df, library_connection_df, server_connection_df, 
        #     storage_connection_statistics_report_df, device_connection_statistics_report_df
        #     ]

        data_lst = verify_data(report_constant_lst, data_names, *data_lst)
        portshow_aggregated_df = data_lst[0]
        # add report_columns_usage_sr to report_creation_info_lst
        report_creation_info_lst[3] = data_lst[4]

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'portshow_aggregated':
            force_flag = portshow_force_flag
        if data_name != 'report_columns_usage_upd':
            dataframe_to_report(data_frame, data_name, report_creation_info_lst, force_flag=force_flag)
    return portshow_aggregated_df


def device_ports_per_group(portshow_aggregated_df):
    """Function to count device ports for each device on (fabric_name, fabric_label), 
    fabric_label and total_fabrics levels based on Device_Host_Name column."""

    # AG mode switches dropped to avoid duplicate  connection information
    mask_switch_native = portshow_aggregated_df['switchMode'] == 'Native'

    portshow_native_df = portshow_aggregated_df.loc[mask_switch_native]
    
    group_columns = ['Fabric_name',	'Fabric_label', 'Device_Host_Name']
    group_level_lst = ['_per_fabric_name_and_label', '_per_fabric_label', '_per_fabric_name', '_total_fabrics']
    instance_column_dct = dict()

    for level in group_level_lst:
        instance_column_dct['Device_Host_Name'] = 'Device_Host_Name' + level
        if not level == '_per_fabric_name':
            portshow_native_df = count_group_members(portshow_native_df, group_columns, instance_column_dct)
            group_columns.pop(0)
        else:
            portshow_native_df = count_group_members(portshow_native_df, ['Fabric_name', 'Device_Host_Name'], instance_column_dct)


    port_columns_lst = ['Fabric_name', 'Fabric_label', 'Connected_portWwn', 
                        'Device_Host_Name_per_fabric_name_and_label', 
                        'Device_Host_Name_per_fabric_label',
                        'Device_Host_Name_per_fabric_name',
                        'Device_Host_Name_total_fabrics']

    device_hostname_stat_native_df = portshow_native_df[port_columns_lst].copy()
    device_hostname_stat_native_df.dropna(subset=['Connected_portWwn'], inplace=True)
    device_hostname_stat_native_df.drop_duplicates(subset=['Connected_portWwn'], inplace=True)
    portshow_aggregated_df = portshow_aggregated_df.merge(device_hostname_stat_native_df, how='left', on=port_columns_lst[:3])
    return portshow_aggregated_df


def warning_notification(portshow_aggregated_df, switch_params_aggregated_df, nsshow_unsplit_df, expected_ag_links_df, report_steps_dct, max_title):
    """Function to show WARNING notification if any deviceType is UNKNOWN,
    if any PortSymb or NodeSymb was not parsed or if new switch founded which was
    not previously discovered"""

    # *_, max_title, report_steps_dct = report_constant_lst
    portshow_force_flag = False
    nsshow_unsplit_force_flag = False
    expected_ag_links_force_flag = False

    portshow_export_flag, *_ = report_steps_dct['portshow_aggregated']
    nsshow_unsplit_export_flag, *_ = report_steps_dct['nsshow_unsplit']
    expected_ag_links_export_flag, *_ = report_steps_dct['expected_ag_links']

    # warning if UKNOWN device class present
    if (portshow_aggregated_df['deviceType'] == 'UNKNOWN').any():
        unknown_count = len(portshow_aggregated_df[portshow_aggregated_df['deviceType'] == 'UNKNOWN'])
        info = f'{unknown_count} {"port" if unknown_count == 1 else "ports"} with UNKNOWN device Class found'
        print(info, end =" ")
        status_info('warning', max_title, len(info))
        # ask if save portshow_aggregated_df
        if not portshow_export_flag:
            reply = reply_request("Do you want to save 'portshow_aggregated'? (y)es/(n)o: ")
            if reply == 'y':
                portshow_force_flag = True
    # warning if any values in PortSymb or NodeSymb were not parsed
    if not nsshow_unsplit_df.empty:
        portsymb_unsplit_count = nsshow_unsplit_df['PortSymb'].notna().sum()
        nodesymb_unsplit_count = nsshow_unsplit_df['NodeSymb'].notna().sum()
        unsplit_lst = [[portsymb_unsplit_count, 'PortSymb'], [nodesymb_unsplit_count, 'NodeSymb']]
        unsplit_str = ' and '.join([str(num) + " " + name for num, name in unsplit_lst if num])
        info = f'{unsplit_str} {"is" if portsymb_unsplit_count + nodesymb_unsplit_count == 1 else "are"} UNPARSED'
        print(info, end =" ")
        status_info('warning', max_title, len(info))
        # ask if save nsshow_unsplit
        if not nsshow_unsplit_export_flag:
            reply = reply_request("Do you want to save 'nsshow_unsplit'? (y)es/(n)o: ")
            if reply == 'y':
                nsshow_unsplit_force_flag = True        
    # warning if unknown switches was found
    switch_name_set = set(switch_params_aggregated_df['switchName'])
    # all founded switches in portshow_aggregated_df
    mask_switch = portshow_aggregated_df['deviceType'] == 'SWITCH'
    portshow_switch_name_set = set(portshow_aggregated_df.loc[mask_switch, 'Device_Host_Name'])
    # if unknown switches found
    if not portshow_switch_name_set.issubset(switch_name_set):
        unknown_count = len(portshow_switch_name_set.difference(switch_name_set))
        info = f'{unknown_count} NEW switch {"name" if unknown_count == 1 else "names"} detected'
        print(info, end =" ")
        status_info('warning', max_title, len(info))
        # ask if save portshow_aggregated_df
        if not portshow_export_flag and not portshow_force_flag:
            reply = reply_request("Do you want to save 'portshow_aggregated'? (y)es/(n)o: ")
            if reply == 'y':
                portshow_force_flag = True
    # if any unconfirmed AG links found
    if not expected_ag_links_df.empty:
        unknown_count = expected_ag_links_df['chassis_name'].notna().sum()
        info = f'{unknown_count} AG {"link" if unknown_count == 1 else "links"} detected'
        print(info, end =" ")
        status_info('warning', max_title, len(info))
        # ask if save expected_ag_links_df
        if not expected_ag_links_export_flag:
            reply = reply_request("Do you want to save 'expected_ag_link'? (y)es/(n)o: ")
            if reply == 'y':
                expected_ag_links_force_flag = True
    return portshow_force_flag, nsshow_unsplit_force_flag, expected_ag_links_force_flag


def device_names_per_port(portshow_aggregated_df):
    """Function to create column with merged device name and port,
    column with list of all devices connected to the port (more then 1 if connected through the NPIV)"""

    # merge device name and port number
    portshow_aggregated_df = merge_columns(portshow_aggregated_df, summary_column='Device_Host_Name_Port',
                                        merge_columns=['Device_Host_Name', 'Device_Port'],
                                        sep=' port ', drop_merge_columns=False)

    # create column with list containing all devices connected to the port
    switch_port_columns = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn','portIndex', 'slot', 'port']
    portshow_aggregated_df['Device_Host_Name_Port'].fillna('nan_device', inplace=True)
    
    portshow_aggregated_df['Device_Host_Name_Port_group'] = portshow_aggregated_df.groupby(by=switch_port_columns)['Device_Host_Name_Port'].transform(', '.join)
    remove_duplicates_from_string(portshow_aggregated_df, 'Device_Host_Name_Port_group')

    portshow_aggregated_df['alias'].fillna('nan_device', inplace=True)
    portshow_aggregated_df['alias_Port_group'] = portshow_aggregated_df.groupby(by=switch_port_columns)['alias'].transform(', '.join)
    # remove temporary 'nan_device value
    portshow_aggregated_df.replace({'nan_device': np.nan}, inplace=True)
    remove_value_from_string(portshow_aggregated_df, 'nan_device', 'alias_Port_group')

    # portshow_aggregated_df['Device_Host_Name_Port'].replace({'nan_device': np.nan}, inplace=True)
    # portshow_aggregated_df['Device_Host_Name_Port_group'].replace({'nan_device': np.nan}, inplace=True)
    # portshow_aggregated_df['alias'].replace({'nan_device': np.nan}, inplace=True)
    # portshow_aggregated_df['alias_Port_group'].replace({'nan_device': np.nan}, inplace=True)
    return portshow_aggregated_df


def sort_portshow(portshow_aggregated_df):
    """Function to sort portshow_aggregated_df"""

    portshow_aggregated_df['portIndex_int'] = pd.to_numeric(portshow_aggregated_df['portIndex'], errors='ignore')
    # sorting DataFrame
    sort_columns = ['Fabric_name', 'Fabric_label', 'chassis_wwn', 'chassis_name', 
                    'switchWwn', 'switchName', 'portIndex_int', 'Connected_portId']
    sort_order = [True, True, False, True, False, True, True, True]
    portshow_aggregated_df.sort_values(by=sort_columns, ascending=sort_order, inplace=True)
    portshow_aggregated_df.drop(columns=['portIndex_int'], inplace=True)
    return portshow_aggregated_df

