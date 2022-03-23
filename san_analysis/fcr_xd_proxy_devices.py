"""Module to create sensor related DataFrames"""

import numpy as np
import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop


def fcr_xd_device_analysis(switch_params_aggregated_df, portshow_aggregated_df, 
                            fcrproxydev_df, fcrxlateconfig_df, project_constants_lst):
    """Main function to create table of devices connected to translate domains"""
        
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'fcr_xd_device_analysis_out', 'fcr_xd_device_analysis_in')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating translate domain connected devices table'
        print(info, end =" ") 

        # aggregated DataFrames
        fcr_xd_proxydev_df = fcr_xd_proxydev_aggregation(switch_params_aggregated_df, portshow_aggregated_df, 
                                                            fcrproxydev_df, fcrxlateconfig_df)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # create list with partitioned DataFrames
        data_lst = [fcr_xd_proxydev_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst) 
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        fcr_xd_proxydev_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return fcr_xd_proxydev_df


def fcr_xd_proxydev_aggregation(switch_params_aggregated_df, portshow_aggregated_df, 
                                fcrproxydev_df, fcrxlateconfig_df):
    """Function to idenitfy devices connected to translate domains"""

    fcrproxydev_cp_df = fcrproxydev_df.copy()
    fcrxlateconfig_cp_df = fcrxlateconfig_df.copy()

    # add fabric_name, fabric_label of the backbone switches
    fcrproxydev_cp_df['switchWwn'] = fcrproxydev_cp_df['principal_switchWwn']
    fcrproxydev_cp_df = dfop.dataframe_fillna(fcrproxydev_cp_df, switch_params_aggregated_df, 
                                                join_lst=['switchWwn'], filled_lst=['Fabric_name', 'Fabric_label'])
    fcrxlateconfig_cp_df = dfop.dataframe_fillna(fcrxlateconfig_cp_df, switch_params_aggregated_df,     
                                                    join_lst=['switchWwn'], filled_lst=['Fabric_name', 'Fabric_label'])
    # identify device connected
    fcr_xd_proxydev_df = identify_proxy_device_wwn_pid(fcrxlateconfig_cp_df, fcrproxydev_cp_df)
    fcr_xd_proxydev_df = add_proxy_device_details(fcr_xd_proxydev_df, portshow_aggregated_df)
    
    # add fabric name, label and switchName of the translate domain
    switch_columns = ['Fabric_name', 'Fabric_label', 'switchName']
    fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, switch_params_aggregated_df, join_lst=['switchWwn'], filled_lst=switch_columns)
    # move translate domain columns to the front of DataFrame
    fcr_xd_proxydev_df = dfop.move_column(fcr_xd_proxydev_df, cols_to_move=switch_columns, ref_col='switchWwn', place='before')
    return fcr_xd_proxydev_df


def identify_proxy_device_wwn_pid(fcrxlateconfig_cp_df, fcrproxydev_cp_df):
    """Function to identify proxy device portWwn and PIDs (physical and proxy)"""

    # ImportedFID - Fabric ID where translate domain is created
    fcrproxydev_cp_df['ImportedFid'] = fcrproxydev_cp_df['Proxy_Created_in_Fabric']
    # ExportedFid - FabricID proxy devices are imported from
    fcrproxydev_cp_df['ExportedFid'] = fcrproxydev_cp_df['Device_Exists_in_Fabric']
    # remove leading zeroes
    for column in ['ImportedFid', 'ExportedFid', 'Domain', 'OwnerDid']:
        fcrxlateconfig_cp_df[column] = fcrxlateconfig_cp_df[column].str.lstrip('0')

    fcr_xd_proxydev_columns = ['Fabric_name', 'Fabric_label', 'XlateWWN', 'ImportedFid', 'ExportedFid', 'Domain', 'OwnerDid']
    fcr_xd_proxydev_df = fcrxlateconfig_cp_df[fcr_xd_proxydev_columns].copy()

    # add device portWwn, PID in fabric where device connected, PID in fabric where device imported based FIDs
    fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, fcrproxydev_cp_df, 
                                                join_lst=['Fabric_name', 'Fabric_label', 'ImportedFid', 'ExportedFid'], 
                                                filled_lst=['Device_portWwn', 'Proxy_PID', 'Physical_PID'], remove_duplicates=False)
    return fcr_xd_proxydev_df


def add_proxy_device_details(fcr_xd_proxydev_df, portshow_aggregated_df):
    """Function to add proxy device details (device_columns) connected to translate domain"""

    # rename column to join on
    fcr_xd_proxydev_df.rename(columns={'Device_portWwn': 'Connected_portWwn'}, inplace=True)
    # drop fabric name and label of backbone switches 
    # coz we are interested in fabric name and label of the fabric where translate domain created and where real device connected
    fcr_xd_proxydev_df.drop(columns=['Fabric_name', 'Fabric_label'], inplace=True)

    device_columns = ['Fabric_name', 'Fabric_label', 
                    'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn',
                    'portIndex', 'slot', 'port', 'Connected_portId',
                    'speed', 'portType',
                    'Device_Host_Name', 'Device_Port', 'alias',
                    'LSAN', 'deviceType', 'deviceSubtype']
    # add proxy device information based in portWwn
    fcr_xd_proxydev_df = dfop.dataframe_fillna(fcr_xd_proxydev_df, portshow_aggregated_df, join_lst=['Connected_portWwn'], filled_lst=device_columns)
    # add Device_ flag for columns related to proxy device
    device_rename_dct = {column: 'Device_' + column for column in device_columns[:6]}
    device_rename_dct['XlateWWN'] = 'switchWwn'
    # rename columns of the proxy device and Wwn of the traslate domain 
    fcr_xd_proxydev_df.rename(columns=device_rename_dct, inplace=True)
    return fcr_xd_proxydev_df