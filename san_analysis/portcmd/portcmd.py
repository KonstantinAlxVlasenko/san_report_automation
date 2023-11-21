"""Module to identify Fabric devices in portshow DataFrame"""

import numpy as np
import pandas as pd

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .devicename_change import devicename_correction, hostname_domain_remove
from .portcmd_aggregation import portshow_aggregated
from .report_portcmd import portcmd_report_main
from .statistics import (device_connection_statistics,
                         storage_connection_statistics)


def portcmd_analysis(portshow_df, switchshow_ports_df, switch_params_df, 
                        switch_params_aggregated_df, isl_aggregated_df, 
                        nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df,
                        ag_principal_df, porttrunkarea_df, 
                        alias_df, fdmi_df, blade_module_df, 
                        blade_servers_df, blade_vc_df, 
                        synergy_module_df, synergy_servers_df, 
                        system_3par_df, port_3par_df, system_oceanstor_df, port_oceanstor_df,
                        project_constants_lst):
    """Main function to add connected devices information to portshow DataFrame"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, _, report_headers_df, report_columns_usage_sr, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'portcmd_analysis_out', 'portcmd_analysis_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # flag to forcible save portshow_aggregated_df if required
    portshow_force_flag = False
    exit_after_save_flag = False    
    
    device_rename_df = data_lst[3]
    if not device_rename_df is None:
        device_rename_df, = dbop.verify_read_data(max_title, ['device_rename'], device_rename_df)

    domain_name_remove_df = data_lst[4]
    if not domain_name_remove_df is None:
        domain_name_remove_df, = dbop.verify_read_data(max_title, ['domain_name_remove'], domain_name_remove_df)

    nsshow_unsplit_df = pd.DataFrame()

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # import data with switch models, firmware and etc
        switch_models_df = sfop.dataframe_import('switch_models', max_title)
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, *_ = sfop.regex_pattern_import('ns_split', max_title)
    
        oui_df = sfop.dataframe_import('oui', max_title, columns=['Connected_oui', 'type', 'subtype'])
        # current operation information string
        info = f'Generating connected devices table'
        print(info, end =" ") 
        

        portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df = \
            portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_df, 
                                switch_params_aggregated_df, isl_aggregated_df, 
                                nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df, 
                                ag_principal_df, porttrunkarea_df, switch_models_df, alias_df, 
                                oui_df, fdmi_df, blade_module_df,  blade_servers_df, blade_vc_df, 
                                synergy_module_df, synergy_servers_df, 
                                system_3par_df, port_3par_df, system_oceanstor_df, port_oceanstor_df,
                                pattern_dct)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # show warning if any UNKNOWN device class founded, if any PortSymb or NodeSymb is not parsed,
        # if new switch founded
        portshow_force_flag, nsshow_unsplit_force_flag, expected_ag_links_force_flag, exit_after_save_flag = \
            warning_notification(portshow_aggregated_df, switch_params_aggregated_df, 
            nsshow_unsplit_df, expected_ag_links_df, project_steps_df, max_title)        
        # remove domain names
        portshow_aggregated_df, domain_name_remove_df = \
            hostname_domain_remove(portshow_aggregated_df, domain_name_remove_df, project_constants_lst)
        # correct device names manually
        portshow_aggregated_df, device_rename_df = \
            devicename_correction(portshow_aggregated_df, device_rename_df, project_constants_lst)
        # merge 'Device_Host_Name' and 'Device_port', create column with all Device_Host_Name for port each port
        portshow_aggregated_df = device_names_per_port(portshow_aggregated_df)
        # count Device_Host_Name instances for fabric_label, label and total in fabric
        portshow_aggregated_df = device_ports_per_group(portshow_aggregated_df)
        # sort rows
        portshow_aggregated_df = sort_portshow(portshow_aggregated_df)
        validate_report_columns_to_drop(portshow_aggregated_df, report_columns_usage_sr)
        # count device connection statistics
        info = f'Counting device connection statistics'
        print(info, end =" ")
        storage_connection_statistics_df = storage_connection_statistics(portshow_aggregated_df, pattern_dct)
        device_connection_statistics_df = device_connection_statistics(portshow_aggregated_df)    
        meop.status_info('ok', max_title, len(info))

        servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df, \
                storage_connection_statistics_report_df, device_connection_statistics_report_df  = \
                    portcmd_report_main(portshow_aggregated_df, storage_connection_statistics_df, 
                                            device_connection_statistics_df, data_names[6:-2], 
                                            report_headers_df, report_columns_usage_sr)
        # create list with partitioned DataFrames
        data_lst = [
            portshow_aggregated_df, storage_connection_statistics_df, device_connection_statistics_df, 
            device_rename_df, domain_name_remove_df, report_columns_usage_sr, 
            servers_report_df, storage_report_df, library_report_df, hba_report_df, 
            storage_connection_df, library_connection_df, server_connection_df, 
            storage_connection_statistics_report_df, device_connection_statistics_report_df
            ]

        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)
        # save data to service file if it's required
        report.dataframe_to_excel(nsshow_unsplit_df, 'nsshow_unsplit', project_constants_lst, force_flag=nsshow_unsplit_force_flag)
        report.dataframe_to_excel(expected_ag_links_df, 'expected_ag_links', project_constants_lst, force_flag=expected_ag_links_force_flag)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        portshow_aggregated_df, *_ = data_lst
        # add report_columns_usage_sr to report_creation_info_lst
        project_constants_lst[5] = data_lst[5]

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        force_flag = False
        if data_name == 'portshow_aggregated':
            force_flag = portshow_force_flag
        if data_name != 'report_columns_usage_upd':
            report.dataframe_to_excel(data_frame, data_name, project_constants_lst, force_flag=force_flag)
    # check if stop programm execution flag is on
    meop.validate_stop_program_flag(exit_after_save_flag)
    return portshow_aggregated_df


def validate_report_columns_to_drop(portshow_aggregated_df, report_columns_usage_sr):
    
    if portshow_aggregated_df['portIndex'].equals(portshow_aggregated_df['port']):
        report_columns_usage_sr['port_index_usage'] = 0
    else:
        report_columns_usage_sr['port_index_usage'] = 1

    if (portshow_aggregated_df['slot'] == '0').all():
        report_columns_usage_sr['slot_usage'] = 0
    else:
        report_columns_usage_sr['slot_usage'] = 1



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
            portshow_native_df = dfop.count_group_members(portshow_native_df, group_columns, instance_column_dct)
            group_columns.pop(0)
        else:
            portshow_native_df = dfop.count_group_members(portshow_native_df, ['Fabric_name', 'Device_Host_Name'], instance_column_dct)


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


def warning_notification(portshow_aggregated_df, switch_params_aggregated_df, nsshow_unsplit_df, 
                            expected_ag_links_df, project_steps_df, max_title):
    """Function to show WARNING notification if any deviceType is UNKNOWN,
    if any PortSymb or NodeSymb was not parsed or if new switch founded which was
    not previously discovered"""


    portshow_force_flag = False
    nsshow_unsplit_force_flag = False
    expected_ag_links_force_flag = False
    exit_after_save_flag = False

    portshow_export_flag = project_steps_df.loc['portshow_aggregated', 'export_to_excel']
    nsshow_unsplit_export_flag = project_steps_df.loc['nsshow_unsplit', 'export_to_excel']
    expected_ag_links_export_flag = project_steps_df.loc['expected_ag_links', 'export_to_excel']

    # warning if UKNOWN device class present
    if (portshow_aggregated_df['deviceType'] == 'UNKNOWN').any():
        unknown_count = len(portshow_aggregated_df[portshow_aggregated_df['deviceType'] == 'UNKNOWN'])
        info = f'{unknown_count} {"port" if unknown_count == 1 else "ports"} with UNKNOWN device Class found'
        print(info, end =" ")
        meop.status_info('warning', max_title, len(info))
        # ask if save portshow_aggregated_df
        if not portshow_export_flag:
            reply = meop.reply_request("\nDo you want to save 'portshow_aggregated'? (y)es/(n)o: ")
            print('\n')
            if reply == 'y':
                portshow_force_flag = True
        exit_after_save_flag = meop.display_stop_request(exit_after_save_flag)
    # warning if any values in PortSymb or NodeSymb were not parsed
    if not nsshow_unsplit_df.empty:
        portsymb_unsplit_count = nsshow_unsplit_df['PortSymb'].notna().sum()
        nodesymb_unsplit_count = nsshow_unsplit_df['NodeSymb'].notna().sum()
        unsplit_lst = [[portsymb_unsplit_count, 'PortSymb'], [nodesymb_unsplit_count, 'NodeSymb']]
        unsplit_str = ' and '.join([str(num) + " " + name for num, name in unsplit_lst if num])
        info = f'{unsplit_str} {"is" if portsymb_unsplit_count + nodesymb_unsplit_count == 1 else "are"} UNPARSED'
        print(info, end =" ")
        meop.status_info('warning', max_title, len(info))
        # ask if save nsshow_unsplit
        if not nsshow_unsplit_export_flag:
            reply = meop.reply_request("\nDo you want to save 'nsshow_unsplit'? (y)es/(n)o: ")
            print('\n')
            if reply == 'y':
                nsshow_unsplit_force_flag = True
        exit_after_save_flag = meop.display_stop_request(exit_after_save_flag)        
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
        meop.status_info('warning', max_title, len(info))
        # ask if save portshow_aggregated_df
        if not portshow_export_flag and not portshow_force_flag:
            reply = meop.reply_request("\nDo you want to save 'portshow_aggregated'? (y)es/(n)o: ")
            print('\n')
            if reply == 'y':
                portshow_force_flag = True
    # if any unconfirmed AG links found
    if not expected_ag_links_df.empty:
        unknown_count = expected_ag_links_df['chassis_name'].notna().sum()
        info = f'{unknown_count} AG {"link" if unknown_count == 1 else "links"} detected'
        print(info, end =" ")
        meop.status_info('warning', max_title, len(info))
        # ask if save expected_ag_links_df
        if not expected_ag_links_export_flag:
            reply = meop.reply_request("\nDo you want to save 'expected_ag_link'? (y)es/(n)o: ")
            print('\n')
            if reply == 'y':
                expected_ag_links_force_flag = True
    return portshow_force_flag, nsshow_unsplit_force_flag, expected_ag_links_force_flag, exit_after_save_flag


def device_names_per_port(portshow_aggregated_df):
    """Function to create column with merged device name and port,
    column with list of all devices connected to the port (more then 1 if connected through the NPIV)"""

    # merge device name and port number
    portshow_aggregated_df = dfop.merge_columns(portshow_aggregated_df, summary_column='Device_Host_Name_Port',
                                        merge_columns=['Device_Host_Name', 'Device_Port'],
                                        sep=' port ', drop_merge_columns=False)
    # if no Device_Host_Name Device_Host_Name_Port column is not informative
    mask_device_host_name_empty = portshow_aggregated_df['Device_Host_Name'].isna()
    portshow_aggregated_df.loc[mask_device_host_name_empty, 'Device_Host_Name_Port'] = np.nan
    # create column with list containing all devices connected to the port
    switch_port_columns = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn','portIndex', 'slot', 'port']
    portshow_aggregated_df['Device_Host_Name_Port'].fillna('nan_device', inplace=True)
    
    portshow_aggregated_df['Device_Host_Name_Port_group'] = portshow_aggregated_df.groupby(by=switch_port_columns)['Device_Host_Name_Port'].transform(', '.join)
    dfop.remove_duplicates_from_string(portshow_aggregated_df, 'Device_Host_Name_Port_group')

    portshow_aggregated_df['alias'] = portshow_aggregated_df['alias'].fillna('nan_device')
    # portshow_aggregated_df['alias'].fillna('nan_device', inplace=True) #depricated method
    
    portshow_aggregated_df['alias_Port_group'] = portshow_aggregated_df.groupby(by=switch_port_columns)['alias'].transform(', '.join)
    # remove temporary 'nan_device value
    portshow_aggregated_df.replace({'nan_device': np.nan}, inplace=True)
    dfop.remove_value_from_string(portshow_aggregated_df, 'nan_device', 'alias_Port_group', 'Device_Host_Name_Port_group')
    dfop.remove_duplicates_from_string(portshow_aggregated_df, 'alias_Port_group', 'Device_Host_Name_Port_group')
    dfop.sort_cell_values(portshow_aggregated_df, 'alias_Port_group', 'Device_Host_Name_Port_group')
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

