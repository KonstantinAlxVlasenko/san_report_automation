"""Module to identify Fabric devices"""

import numpy as np
import pandas as pd

from analysis_portcmd_aliasgroup import alias_preparation, group_name_fillna
from analysis_portcmd_devicetype import oui_join, type_check
from analysis_portcmd_bladesystem import blade_server_fillna, blade_vc_fillna
from analysis_portcmd_nameserver import nsshow_analysis_main
from analysis_portcmd_gateway import verify_gateway_link
from common_operations_dataframe import dataframe_fillna, dataframe_join
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import force_extract_check, status_info
from common_operations_servicefile import (data_extract_objects,
                                           dataframe_import)
from report_portcmd import create_report_tables


def portcmd_analysis_main(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, \
    nscamshow_df, alias_df, fdmi_df, blade_module_df, blade_servers_df, blade_vc_df, report_columns_usage_dct, report_data_lst):
    """Main function to add connected devices information to portshow DataFrame"""
    
    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = [
        'portshow_aggregated', 'Сервера', 'Массивы', 'Библиотеки', 'Микрокоды_HBA', 
        'Подключение_массивов', 'Подключение_библиотек', 'Подключение_серверов'
        ]
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    portshow_aggregated_df, servers_report_df, storage_report_df, library_report_df, \
        hba_report_df, storage_connection_df,  library_connection_df, server_connection_df = data_lst
    nsshow_unsplit_df = pd.DataFrame()

    # list of data to analyze from report_info table
    analyzed_data_names = ['portcmd', 'switchshow_ports', 'switch_params_aggregated', 
    'fdmi', 'nscamshow', 'nsshow', 'alias', 'blade_servers', 'fabric_labels']

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations 
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # list with True (if data loaded) and/or False (if data was not found and None returned)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)

    # check force extract keys for data passed to main function as parameters and fabric labels
    # if analyzed data was re-extracted or re-analyzed on previous steps then data from data_lst
    # need to be re-checked regardless if it was analyzed on prev iterations
    analyzed_data_flags = [report_steps_dct[data_name][1] for data_name in analyzed_data_names]

    # when no data saved or force extract flag is on or data passed as parameters have been changed then 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or any(analyzed_data_flags):
        # information string if data used have been forcibly changed
        if any(analyzed_data_flags) and not any(force_extract_keys_lst) and all(data_check):
            info = f'Force data processing due to change in collected or analyzed data'
            print(info, end =" ")
            status_info('ok', max_title, len(info))

        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('nameserver', max_title)

        oui_df = dataframe_import('oui', max_title) 
        # current operation information string
        info = f'Generating connected devices table'
        print(info, end =" ") 

        portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df = \
            portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_aggregated_df, \
                nsshow_df, nscamshow_df, alias_df, oui_df, fdmi_df, blade_module_df, blade_servers_df, blade_vc_df, re_pattern_lst)

        # after finish display status
        status_info('ok', max_title, len(info))

        servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df = \
                create_report_tables(portshow_aggregated_df, data_names[1:], \
                    report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [
            portshow_aggregated_df, servers_report_df, storage_report_df, 
            library_report_df, hba_report_df, storage_connection_df,  
            library_connection_df, server_connection_df
            ]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)
        save_xlsx_file(alias_wwnn_wwnp_df, 'alias_wwnn', report_data_lst)
        save_xlsx_file(nsshow_unsplit_df, 'nsshow_unsplit', report_data_lst)
        save_xlsx_file(expected_ag_links_df, 'expected_ag_links_df', report_data_lst)
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return portshow_aggregated_df


def portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, \
                        alias_df, oui_df, fdmi_df, blade_module_df, blade_servers_df, blade_vc_df, re_pattern_lst):
    """
    Function to fill portshow DataFrame with information from DataFrames passed as params
    and define fabric device types
    """
    
    # add switch information (switchName, portType, portSpeed) to portshow DataFrame
    portshow_aggregated_df = switchshow_join(portshow_df, switchshow_ports_df)
    # add fabric information (FabricName, FabricLabel) and switchMode to portshow_aggregated DataFrame
    portshow_aggregated_df = switchparams_join(portshow_aggregated_df, switch_params_aggregated_df)
    # prepare alias_df (label fabrics, replace WWNn with WWNp if present)
    alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df = alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df)
    # retrieve storage, host, HBA information from Name Server service and FDMI data
    nsshow_join_df, nsshow_unsplit_df = nsshow_analysis_main(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df, re_pattern_lst)
    # add nsshow and alias informormation to portshow_aggregated_df DataFrame
    portshow_aggregated_df = alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df)
    # fillna portshow_aggregated DataFrame null values with values from blade_servers_join_df
    portshow_aggregated_df = blade_server_fillna(portshow_aggregated_df, blade_servers_df, re_pattern_lst)
    # fillna portshow_aggregated DataFrame null values with values from blade_vc_join_df
    portshow_aggregated_df = blade_vc_fillna(portshow_aggregated_df, blade_module_df, blade_vc_df)
    # calculate virtual channel id for medium priority traffic
    portshow_aggregated_df = vc_id(portshow_aggregated_df)
    # add 'deviceType', 'deviceSubtype' columns
    portshow_aggregated_df = portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), 'deviceType', 'deviceSubtype'])
    # add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)
    portshow_aggregated_df = oui_join(portshow_aggregated_df, oui_df)

    # TOREMOVE
    # # allocate oui and vendor information from  from Access Gateway switches WWNp
    # ag_oui = ag_switches_oui(ag_principal_df)

    # preliminarily assisgn to all initiators type SRV
    mask_initiator = portshow_aggregated_df.Device_type.isin(['Physical Initiator', 'NPIV Initiator'])
    portshow_aggregated_df.loc[mask_initiator, ['deviceType', 'deviceSubtype']] = ['SRV', 'SRV']
    # define oui for each connected device to identify device type
    switches_oui = switch_params_aggregated_df['switchWwn'].str.slice(start = 6)
    
    
    # TO REMOVE
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['type', 'subtype']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['Connected_oui', 'Connected_portWwn']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    
    
    # final device type define
    portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(
        lambda series: type_check(series, switches_oui, blade_servers_df), axis = 1)

    # verify access gateway links
    portshow_aggregated_df, expected_ag_links_df = verify_gateway_link(portshow_aggregated_df)
    
    # libraries Device_Host_Name correction to avoid hba information from FDMI DataFrame usage for library name
    portshow_aggregated_df.Device_Host_Name = portshow_aggregated_df.apply(lambda series: lib_name_correction(series) \
        if pd.notna(series[['deviceType', 'Device_Name']]).all() else series['Device_Host_Name'], axis=1)
    
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias group name values
    portshow_aggregated_df = group_name_fillna(portshow_aggregated_df)
    
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias values 
    portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.alias, inplace = True)
    # fill empty values in portshow_aggregated_df Device_Host_Name column with combination of device class and it's wwnp
    portshow_aggregated_df.Device_Host_Name = portshow_aggregated_df.apply(lambda series: device_name_fillna(series), axis=1)
    
    return portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df


def switchshow_join(portshow_df, switchshow_df):
    """Function to add switch information to portshow DataFrame
    Adding switchName, switchWwn, speed and portType
    Initially DataFrame contains only chassisName and chassisWwn
    Merge DataFrames on configName, chassisName, chassisWwn, slot and port"""
    
    # columns labels reqiured for join operation
    switchshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'slot', 'port', 'switchName', 
                      'switchWwn', 'speed', 'portType']
    # create left DataFrame for join operation
    switchshow_join_df = switchshow_df.loc[:, switchshow_lst].copy()
    # portshow_df and switchshow_join_df DataFrames join operation
    portshow_aggregated_df = portshow_df.merge(switchshow_join_df, how = 'left', on = switchshow_lst[:5])
    # # drop offline ports
    # mask_online = portshow_aggregated_df['portState'] == 'Online'
    # portshow_aggregated_df = portshow_aggregated_df.loc[mask_online]
    # # drop columns with empty WWN device column
    # portshow_aggregated_df.dropna(subset = ['Connected_portWwn'], inplace = True)
    
    return portshow_aggregated_df


def switchparams_join(portshow_aggregated_df, switch_params_aggregated_df):
    """Function to label switches in portshow_aggregated_df with Fabric names and labels, switchMode"""
    
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label', 'switchMode']
    # create left DataFrame for join operation
    switchparams_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    # portshow_aggregated_df and switchshow_join_df DataFrames join operation
    portshow_aggregated_df = portshow_aggregated_df.merge(switchparams_join_df, how = 'left', on = switchparams_lst[:5])

    return portshow_aggregated_df


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
