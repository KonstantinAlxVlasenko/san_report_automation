"""Module to identify Fabric devices"""

import numpy as np
import pandas as pd

from analysis_portcmd_devicename_correction import devicename_correction_main
from analysis_portcmd_aliasgroup import alias_preparation, group_name_fillna
from analysis_portcmd_bladesystem import blade_server_fillna, blade_vc_fillna
from analysis_portcmd_devicetype import oui_join, type_check
from analysis_portcmd_gateway import verify_gateway_link
from analysis_portcmd_nameserver import nsshow_analysis_main
from analysis_portcmd_switch import fill_isl_link, fill_switch_info, switchparams_join, switchshow_join
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import (status_info, verify_data,
                                             verify_force_run)
from common_operations_servicefile import (data_extract_objects,
                                           dataframe_import)
from common_operations_dataframe import dataframe_fabric_labeling
from report_portcmd import create_report_tables


def portcmd_analysis_main(portshow_df, switchshow_ports_df, switch_params_df, 
                            switch_params_aggregated_df, isl_aggregated_df, nsshow_df, 
                            nscamshow_df, ag_principal_df, alias_df, fdmi_df, blade_module_df, 
                            blade_servers_df, blade_vc_df, report_columns_usage_dct, report_data_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = [
        'portshow_aggregated', 'device_rename', 'report_columns_usage_upd', 'Сервера', 'Массивы', 'Библиотеки', 'Микрокоды_HBA', 
        'Подключение_массивов', 'Подключение_библиотек', 'Подключение_серверов', 'NPIV'
        ]
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    report_columns_usage_bckp = report_columns_usage_dct
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    portshow_aggregated_df, device_rename_df, report_columns_usage_dct, servers_report_df, storage_report_df, library_report_df, \
        hba_report_df, storage_connection_df,  library_connection_df, server_connection_df, npiv_report_df = data_lst
    nsshow_unsplit_df = pd.DataFrame()

    if not report_columns_usage_dct:
        report_columns_usage_dct = report_columns_usage_bckp

    # list of data to analyze from report_info table
    analyzed_data_names = ['portcmd', 'switchshow_ports', 'switch_params_aggregated', 
                            'switch_parameters', 'chassis_parameters', 'fdmi', 'nscamshow', 
                            'nsshow', 'alias', 'blade_servers', 'fabric_labels', 'isl', 
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

        oui_df = dataframe_import('oui', max_title) 
        # current operation information string
        info = f'Generating connected devices table'
        print(info, end =" ") 

        portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df = \
            portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_df, 
                                switch_params_aggregated_df, isl_aggregated_df, nsshow_df, 
                                nscamshow_df, ag_principal_df, switch_models_df, alias_df, 
                                oui_df, fdmi_df, blade_module_df, 
                                blade_servers_df, blade_vc_df, re_pattern_lst, report_data_lst)

        # after finish display status
        status_info('ok', max_title, len(info))

        portshow_aggregated_df, device_rename_df = \
            devicename_correction_main(portshow_aggregated_df, device_rename_df, report_columns_usage_dct, report_data_lst)


        servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df, npiv_report_df = \
                create_report_tables(portshow_aggregated_df, data_names[3:], \
                    report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [
            portshow_aggregated_df, device_rename_df, report_columns_usage_dct, 
            servers_report_df, storage_report_df, 
            library_report_df, hba_report_df, storage_connection_df,  
            library_connection_df, server_connection_df, npiv_report_df
            ]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
        save_xlsx_file(nsshow_unsplit_df, 'nsshow_unsplit', report_data_lst)
        save_xlsx_file(expected_ag_links_df, 'expected_ag_links_df', report_data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        portshow_aggregated_df, device_rename_df, report_columns_usage_dct, servers_report_df, storage_report_df, \
            library_report_df, hba_report_df, storage_connection_df, \
                library_connection_df, server_connection_df, npiv_report_df \
                    = verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [
            portshow_aggregated_df, device_rename_df, report_columns_usage_dct, servers_report_df, storage_report_df, 
            library_report_df, hba_report_df, storage_connection_df,  
            library_connection_df, server_connection_df, npiv_report_df
            ]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return portshow_aggregated_df


def portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_df, switch_params_aggregated_df, 
                        isl_aggregated_df, nsshow_df, nscamshow_df, ag_principal_df, switch_models_df, alias_df, oui_df, fdmi_df, 
                        blade_module_df, blade_servers_df, blade_vc_df, re_pattern_lst, report_data_lst):
    """
    Function to fill portshow DataFrame with information from DataFrames passed as params
    and define fabric device types
    """
    
    # add switch information (switchName, portType, portSpeed) to portshow DataFrame
    portshow_aggregated_df = switchshow_join(portshow_df, switchshow_ports_df)

    portshow_aggregated_df = dataframe_fabric_labeling(portshow_aggregated_df, switch_params_aggregated_df)
    
    # add fabric information (FabricName, FabricLabel) and switchMode to portshow_aggregated DataFrame
    portshow_aggregated_df = switchparams_join(portshow_aggregated_df, switch_params_df, 
                                                switch_params_aggregated_df, report_data_lst)
    
    # prepare alias_df (label fabrics, replace WWNn with WWNp if present)
    alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df = \
        alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df)
    # retrieve storage, host, HBA information from Name Server service and FDMI data
    nsshow_join_df, nsshow_unsplit_df = \
        nsshow_analysis_main(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df, re_pattern_lst)
    # add nsshow and alias informormation to portshow_aggregated_df DataFrame
    portshow_aggregated_df = \
        alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df)
    # fillna portshow_aggregated DataFrame null values with values from blade_servers_join_df
    portshow_aggregated_df = \
        blade_server_fillna(portshow_aggregated_df, blade_servers_df, re_pattern_lst)
    # fillna portshow_aggregated DataFrame null values with values from blade_vc_join_df
    portshow_aggregated_df = \
        blade_vc_fillna(portshow_aggregated_df, blade_module_df, blade_vc_df)
    # calculate virtual channel id for medium priority traffic
    portshow_aggregated_df = vc_id(portshow_aggregated_df)
    # add 'deviceType', 'deviceSubtype' columns
    portshow_aggregated_df = \
        portshow_aggregated_df.reindex(
            columns=[*portshow_aggregated_df.columns.tolist(), 'deviceType', 'deviceSubtype'])
    # add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)
    portshow_aggregated_df = oui_join(portshow_aggregated_df, oui_df)
    
    # preliminarily assisgn to all initiators type SRV
    mask_initiator = portshow_aggregated_df.Device_type.isin(['Physical Initiator', 'NPIV Initiator'])
    portshow_aggregated_df.loc[mask_initiator, ['deviceType', 'deviceSubtype']] = ['SRV', 'SRV']
    # define oui for each connected device to identify device type
    switches_oui = switch_params_aggregated_df['switchWwn'].str.slice(start = 6)
    # final device type define
    portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(
        lambda series: type_check(series, switches_oui, blade_servers_df), axis = 1)
    # identify MSA port numbers (A1-A4, B1-B4) based on PortWwn
    portshow_aggregated_df.Device_Port = \
        portshow_aggregated_df.apply(lambda series: find_msa_port(series) \
        if (pd.notna(series['PortName']) and  series['deviceSubtype'] == 'MSA') else series['Device_Port'], axis=1)   
    
    # verify access gateway links
    portshow_aggregated_df, expected_ag_links_df = \
        verify_gateway_link(portshow_aggregated_df, switch_params_aggregated_df, ag_principal_df, switch_models_df)
    # fill isl links information
    portshow_aggregated_df = \
        fill_isl_link(portshow_aggregated_df, isl_aggregated_df)
    # fill connected switch information
    portshow_aggregated_df = \
        fill_switch_info(portshow_aggregated_df, switch_params_df, 
                            switch_params_aggregated_df, report_data_lst)
    # libraries Device_Host_Name correction to avoid hba information from FDMI DataFrame usage for library name
    portshow_aggregated_df.Device_Host_Name = \
        portshow_aggregated_df.apply(lambda series: lib_name_correction(series) \
        if pd.notna(series[['deviceType', 'Device_Name']]).all() else series['Device_Host_Name'], axis=1)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias group name values
    portshow_aggregated_df = group_name_fillna(portshow_aggregated_df)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias values 
    portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.alias, inplace = True)

    # if Device_Host_Name is still empty fill empty values in portshow_aggregated_df Device_Host_Name column 
    # with combination of device class and it's wwnp
    portshow_aggregated_df.Device_Host_Name = \
        portshow_aggregated_df.apply(lambda series: device_name_fillna(series), axis=1)
    # sorting DataFrame
    sort_columns = ['Fabric_name', 'Fabric_label', 'chassis_wwn', 'chassis_name', 
                    'switchWwn', 'switchName']
    sort_order = [True, True, False, True, False, True]
    portshow_aggregated_df.sort_values(by=sort_columns, ascending=sort_order, inplace=True)

    return portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df


def alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df):
    """Function to add porttype (Target, Initiator) and alias to portshow_aggregated DataFrame"""
    
    nsshow_join_df.drop(columns = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn'], inplace = True)

    # adding porttype (Target, Initiator) to portshow_aggregated DataFrame
    portshow_aggregated_df = portshow_aggregated_df.merge(nsshow_join_df, how = 'left', 
                                                          left_on = ['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                          right_on = ['Fabric_name', 'Fabric_label', 'PortName'])
    
    # if switch in AG mode then device type must be replaced to Physical instead of NPIV
    mask_ag = portshow_aggregated_df.switchMode == 'Access Gateway Mode'
    portshow_aggregated_df.loc[mask_ag, 'Device_type'] = \
        portshow_aggregated_df.loc[mask_ag, 'Device_type'].str.replace('NPIV', 'Physical')

    # add aliases to portshow_aggregated_df
    portshow_aggregated_df = portshow_aggregated_df.merge(alias_wwnp_df, how = 'left', 
                                                          on = ['Fabric_name', 'Fabric_label', 'PortName'])
    return portshow_aggregated_df


def device_name_fillna(series):
    """
    Function to fill empty values in portshow_aggregated_df Device_Host_Name column
    with combination of device class and it's wwnn
    """

    mask_switch = series['deviceType'] == 'SWITCH'
    # mask_vc = series['deviceType'] == 'VC'
    mask_name_notna = pd.notna(series['Device_Host_Name'])
    mask_devicetype_wwn_isna =  pd.isna(series[['deviceType', 'NodeName']]).any()
    # if device is switch or already has name no action required
    if mask_switch or mask_name_notna:
        return series['Device_Host_Name']
    # if no device class defined or no wwn then it's not possible to concatenate to values
    if  mask_devicetype_wwn_isna:
        return np.nan
    
    return series['deviceType'] + ' ' + series['NodeName']


def vc_id(portshow_aggregated_df):
    """
    Function to calculate virtual channel id for medium priority traffic.
    VC2, VC3, VC4, VC5
    """

    # extract AreaID from PortID address
    portshow_aggregated_df['Virtual_Channel'] = portshow_aggregated_df.Connected_portId.str.slice(start=2, stop=4)
    # convert string AreaID to integer with base 16 (hexadicimal)
    # vc id defined as remainder of AreaID division by four plus 2
    portshow_aggregated_df.Virtual_Channel = portshow_aggregated_df.Virtual_Channel.apply(lambda x: int(x, 16)%4 + 2)
    # add VC to vc_id
    portshow_aggregated_df.Virtual_Channel = 'VC' + portshow_aggregated_df.Virtual_Channel.astype('str')

    return portshow_aggregated_df


def lib_name_correction(series):
    """
    Function avoid usage of HBA Host_Name value from FDMI DataFrame 
    for libraries Device_Host_Name in portshowaggregated DataFrame
    """

    # libraries mask
    mask_lib = series['deviceType'] == 'LIB'
    # correct device name for libraries
    if mask_lib:
        return series['Device_Name']
    else:
        return series['Device_Host_Name']


def find_msa_port(series):
    """
    Function to identify port name (A1-A4, B1-B4) for the MSA Storage
    based on second digit of PortWwn number (0-3 A1-A4, 4-7 B1-B4)
    """ 
    # if pd.isna(series['PortName']):
    #     return series['Device_Port']
    # print(series['PortName'])

    # port name based on second digit of portWwn
    port_num = int(series['PortName'][1])
    # Controller A ports
    if port_num in range(4):
        return 'A' + str(port_num+1)
    # Controller B ports
    elif port_num in range(4,8):
        return 'B' + str(port_num-3)
        
    return series['Device_Port']