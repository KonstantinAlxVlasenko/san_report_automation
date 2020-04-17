"""Module to identify Fabric device types"""

import pandas as pd
import numpy as np
from files_operations import status_info, load_data, save_data, data_extract_objects 
from files_operations import force_extract_check, save_xlsx_file
from dataframe_operations import dataframe_join, dataframe_segmentation, dataframe_import, dataframe_fillna
from san_analysis_connected_devices import nsshow_analysis_main


def device_type_main(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, 
                        fdmi_df, blade_servers_df, report_columns_usage_dct, report_data_lst):
    """Main function to add device types to portshow DataFrame"""
    
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 20. DEVICE TYPES TABLE ...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been analyzed
    data_names = [
        'portshow_aggregated', 'Сервера', 'Массивы', 'Библиотеки', 'Микрокоды_HBA', 
        'Подключение_массивов', 'Подключение_библиотек', 'Подключение_серверов'
        ]
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    portshow_aggregated_df, servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        storage_connection_df,  library_connection_df, server_connection_df = data_lst
    nsshow_unsplit_df = pd.DataFrame()

    # list of data to analyze from report_info table
    analyzed_data_names = ['portcmd', 'switchshow_ports', 'switch_params_aggregated', 
    'fdmi', 'nscamshow', 'nsshow', 'alias', 'blade_servers', 'fabric_labels']

    # data force extract check 
    # if data was extracted on previous iterations but force key is ON 
    # then data extracted again and saved
    # force key for each DataFrame
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # check if data was loaded and not empty (DataFrame might be empty)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)

    # check force extract keys for data passed to main function as parameters and fabric labels
    # if analyzed data was re-extracted or re-analyzed on previous steps then data from data_lst
    # need to be re-checked regardless if it was analyzed on prev iterations
    analyzed_data_flags = [report_steps_dct[data_name][1] for data_name in analyzed_data_names]


    # when no data saved or force extract flag is on or data passed as parameters have been changed then 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or any(analyzed_data_flags):
        # information string if fabric labels force changed was initiated
        # and statistics recounting required
        if any(analyzed_data_flags) and not any(force_extract_keys_lst) and all(data_check):
            info = f'Force data processing due to change in collected or analyzed data'
            print(info, end =" ")
            status_info('ok', max_title, len(info))

        # data imported from init file (regular expression patterns) to extract values from data columns
        # re_pattern list contains comp_keys, match_keys, comp_dct    
        _, _, *re_pattern_lst = data_extract_objects('device_type', max_title)

        oui_df = dataframe_import('oui', max_title) 
        # current operation information string
        info = f'Generating Device types table'
        print(info, end =" ") 

        portshow_aggregated_df, nsshow_unsplit_df = portshow_aggregated(portshow_df, switchshow_ports_df, 
            switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, oui_df, fdmi_df, blade_servers_df, re_pattern_lst)

        # after finish display status
        status_info('ok', max_title, len(info))

        servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df = \
                create_report_tables(portshow_aggregated_df, data_names[1:], report_columns_usage_dct, max_title)

        # create list with partitioned DataFrames
        data_lst = [
            portshow_aggregated_df, servers_report_df, storage_report_df, 
            library_report_df, hba_report_df, storage_connection_df,  
            library_connection_df, server_connection_df
            ]

        # saving data to json or csv file
        save_data(report_data_lst, data_names, *data_lst)

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)
    save_xlsx_file(nsshow_unsplit_df, 'nsshow_unsplit', report_data_lst)

    return portshow_aggregated_df


def portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_aggregated_df, nsshow_df, nscamshow_df, alias_df, oui_df, fdmi_df, blade_servers_df, re_pattern_lst):
    # add switch information (switchName, portType, portSpeed) to portshow DataFrame
    portshow_aggregated_df = switchshow_join(portshow_df, switchshow_ports_df)
    # add fabric information (FabricName, FabricLabel) and switchMode to portshow_aggregated DataFrame
    portshow_aggregated_df = switchparams_join(portshow_aggregated_df, switch_params_aggregated_df)
    # prepare alias_df (label fabrics, replace WWNn with WWNp if present)
    alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df = alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df)

    # # prepare nscamshow_df (label fabrics, drop duplicates WWNp)
    # nscamshow_join_df = nscamshow_preparation(nscamshow_df, fabric_labels_df)

    # retrieve storage, host, HBA information from Name Server service and FDMI data
    nsshow_join_df, nsshow_unsplit_df, fabric_labels_df = nsshow_analysis_main(nsshow_df, nscamshow_df, fdmi_df, switch_params_aggregated_df, re_pattern_lst)

    

    # add nsshow and aliases to portshow_df
    # portshow_aggregated_df = alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nscamshow_join_df)

    # add nsshow and alias informormation to portshow_aggregated_df DataFrame
    portshow_aggregated_df = alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df)
    # fillna portshow_aggregated DataFrame null values with values from blade_servers_join_df
    portshow_aggregated_df = blade_fillna(portshow_aggregated_df, blade_servers_df, re_pattern_lst)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias values 
    portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.alias, inplace = True)

    # calculate virtual channel id for medium priority traffic
    portshow_aggregated_df = vc_id(portshow_aggregated_df)

    # add 'deviceType', 'deviceSubtype' columns
    portshow_aggregated_df = portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), 'deviceType', 'deviceSubtype'])
    # add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)
    portshow_aggregated_df = oui_join(portshow_aggregated_df, oui_df)
    # # allocate oui and vendor information from  from Access Gateway switches WWNp
    # ag_oui = ag_switches_oui(ag_principal_df)
    # preliminarily assisgn to all initiators type SRV
    mask_initiator = portshow_aggregated_df.Device_type.isin(['Physical Initiator', 'NPIV Initiator'])
    portshow_aggregated_df.loc[mask_initiator, ['deviceType', 'deviceSubtype']] = ['SRV', 'SRV']
    # 
    switches_oui = switch_params_aggregated_df['switchWwn'].str.slice(start = 6)
    # final device type define
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['type', 'subtype']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    # portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df) if series[['Connected_oui', 'Connected_portWwn']].notnull().all() else pd.Series((np.nan, np.nan)), axis = 1)
    portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(lambda series: type_check(series, switches_oui, fdmi_df, blade_servers_df), axis = 1)

    # fill empty values in portshow_aggregated_df Device_Host_Name column with combination of device class and it's wwnp
    portshow_aggregated_df.Device_Host_Name = portshow_aggregated_df.apply(lambda series: device_name_fillna(series) if pd.notna(series[['deviceType', 'Connected_portWwn']]).all() else np.nan, axis=1)

    return portshow_aggregated_df, nsshow_unsplit_df


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
    # drop offline ports
    mask_online = portshow_aggregated_df['portState'] == 'Online'
    portshow_aggregated_df = portshow_aggregated_df.loc[mask_online]
    # drop columns with empty WWN device column
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


def alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df):
    """Function to label aliases DataFrame and replace WWNn with WWNp if any"""
    
    # create fabric labels DataFrame
    fabric_labels_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label']
    fabric_labels_df = switch_params_aggregated_df.loc[:, fabric_labels_lst].copy()

    # create local Name Server (NS) DataFrame 
    nsshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'SwitchName', 'switchWwn', 'PortName', 'NodeName']
    nsshow_join_df = nsshow_df.loc[:, nsshow_lst].copy()
    nsshow_join_df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
    # labeling Name Server DataFrame
    nsshow_join_df = nsshow_join_df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])
    # remove switch related columns from Name Server DataFrame to leave only Fabric labels, WWNp, WWNn
    # lowercase SwitchName
    nsshow_lst[3] = nsshow_lst[3][0].lower() + nsshow_lst[3][1:]
    nsshow_join_df.drop(columns = nsshow_lst[:5], inplace = True)
    
    # fabric labeling alias DataFrame
    alias_prep_df =  alias_df.rename(columns = {'principal_switch_name': 'switchName', 'principal_switchWwn': 'switchWwn'})
    alias_labeled_df = alias_prep_df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])
    # replacing WWNn with WWNp if any
    # create alias_join DataFrame
    alias_lst = ['Fabric_name', 'Fabric_label', 'alias', 'alias_member']
    alias_join_df = alias_labeled_df.loc[:, alias_lst].copy()
    # merging alias_join and nsshow_join DataFrame
    # if alias_member column contains WWNn then new column contains corresponding WWNp for that WWNn
    alias_wwnn_wwnp_df = alias_join_df.merge(nsshow_join_df, how = 'left', left_on = ['Fabric_name', 'Fabric_label', 'alias_member'], right_on = ['Fabric_name', 'Fabric_label', 'NodeName'])
    # fill empty cells in WWNn -> WWNp column with alias_member WWNp values thus filtering off all WWNn values
    alias_wwnp_df = alias_wwnn_wwnp_df.copy()   
    alias_wwnp_df.PortName.fillna(alias_join_df['alias_member'], inplace = True)

    # drop possibly mixed WWNp and WWNn column alias_memeber and pure WWNn column
    alias_wwnp_df.drop(columns = ['alias_member', 'NodeName'], inplace = True)
    # alias_wwnp_df = alias_join_df.loc[:, ['Fabric_name', 'Fabric_label', 'alias', 'PortName']]

    # if severeal aliases for one wwnp then combine all into one alias
    alias_wwnp_df = alias_wwnp_df.groupby(['Fabric_name', 'Fabric_label', 'PortName'], as_index = False).agg(', '.join)

    
    return alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df


def alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df):
    """Function to add porttype (Target, Initiator) anf alias to portshow_aggregated DataFrame"""
    
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


def oui_join(portshow_aggregated_df, oui_df):
    """Function to add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)"""  
    
    # allocate oui from WWNp
    portshow_aggregated_df['Connected_oui'] = portshow_aggregated_df.Connected_portWwn.str.slice(start = 6, stop = 14)
    # add device types from oui DataFrame
    portshow_aggregated_df = portshow_aggregated_df.merge(oui_df, how = 'left', on = ['Connected_oui'])
    
    return portshow_aggregated_df


def type_check(series, switches_oui, fdmi_df, blade_servers_df):
    
    # drop rows with empty WWNp values
    blade_hba_df = blade_servers_df.dropna(subset = ['portWwn'])


    if series[['type', 'subtype']].notnull().all():
        # servers type
        # if WWNp in blade hba DataFrame
        if (blade_hba_df['portWwn'] == series['Connected_portWwn']).any():
            return pd.Series(('BLADE_SRV', series['subtype'].split('|')[0]))
        # devices with strictly defined type and subtype
        elif not '|' in series['type'] and not '|' in  series['subtype']:
            return pd.Series((series.type, series.subtype))
        # check SWITCH TYPE
        elif 'SRV|SWITCH' in series['type']:
            if 'E-Port' in series['portType']:
                return pd.Series(('SWITCH', series.subtype))
            elif (series['Connected_portWwn'][6:] == switches_oui).any():
                return pd.Series(('SWITCH', series.subtype))
            elif series['switchMode'] == 'Access Gateway Mode' and series['portType'] == 'N-Port':
                return pd.Series(('SWITCH', 'AG'))
            else:
                return pd.Series(('SRV', series.subtype))

        # check StoreOnce and D2D devices
        elif pd.notna(series.Device_Model) and 'storeonce' in series['Device_Model'].lower():
            return pd.Series(('LIB', 'StoreOnce'))
        elif pd.notna(series.Device_Model) and 'd2d' in series['Device_Model'].lower():
            return pd.Series(('LIB', 'D2D'))

        # if not d2d than it's storage
        elif series['type'] == 'STORAGE|LIB':
            if pd.notna(series.Device_Model) and 'ultrium' in series['Device_Model'].lower():
                return pd.Series(('LIB', series['subtype'].split('|')[1]))
            else:
                return pd.Series(('STORAGE', series['subtype'].split('|')[0]))
        
        # check if device server or library
        elif series['type'] == 'SRV|LIB':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            elif series['Device_type'] in ['Physical Target', 'NPIV Target']:
                return pd.Series(('LIB', series['subtype'].split('|')[1]))
        # check if device server or storage
        elif series['type'] == 'SRV|STORAGE':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            else:
                return pd.Series(('STORAGE', series['subtype'].split('|')[1]))
            
        elif series['type'] == 'SRV|STORAGE|LIB':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            # if target port type 
            elif series['Device_type'] in ['Physical Target', 'NPIV Target']:
                # if Ultrium Nodetype than device is MSL or ESL
                if not pd.isna(series['NodeSymb']) and 'ultrium' in series['NodeSymb'].lower():
                    return pd.Series(('LIB', series['subtype'].split('|')[2]))
            # if not initiator and not target ultrium than it's storage
            else:
                device_type = series['type'].split('|')[1]
                device_subtype = series['subtype'].split('|')[1]
                return pd.Series((device_type, device_subtype))

    # if device type is not strictly detected 
    if pd.isna(series[['deviceType', 'deviceSubtype']]).any():
        if series[['type', 'subtype']].notnull().all():                                
            return pd.Series((series['type'], series['subtype']))
        else:
            return pd.Series((np.nan, np.nan))
    else:
        return pd.Series((series['deviceType'], series['deviceSubtype']))


def blade_fillna(portshow_aggregated_df, blade_servers_df, re_pattern_lst):
    """
    Function to fillna portshow_aggregated_df null values with values from blade_servers_join_df.
    Hostname, location and HBA infromation
    """

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    if not blade_servers_df.empty:
        # make copy to avoid changing original DataFrame
        blade_servers_join_df = blade_servers_df.copy()
        # Uppercase Device_Manufacturer column
        blade_servers_join_df.Device_Manufacturer = blade_servers_join_df.Device_Manufacturer.str.upper()

        # hostname_clean_comp
        blade_servers_join_df.Host_Name = blade_servers_join_df.Host_Name.replace(comp_dct[comp_keys[0]], np.nan, regex=True)

        # auxiliary lambda function to combine two columns in DataFrame
        # it combines to columns if both are not null and takes second if first is null
        # str1 and str2 are strings before columns respectively (dfault is whitespace between columns)
        wise_combine = lambda series, str1='', str2=' ': \
            str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
                if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]
        # combine 'Device_Manufacturer' and 'Device_Model' columns
        blade_servers_join_df.Device_Model = blade_servers_join_df[['Device_Manufacturer', 'Device_Model']].apply(wise_combine, axis=1)
        # combine 'Enclosure_Name' and 'Server_Slot' columns
        blade_servers_join_df['Device_Location'] = \
            blade_servers_join_df[['Enclosure_Name', 'Server_Slot']].apply(wise_combine, axis=1, args=('Enclosure ', ' slot '))

        # rename column to correspond name in portshow_aggregated_df DataFrame
        blade_servers_join_df.rename(columns = {'portWwn': 'Connected_portWwn'}, inplace = True)

        # columns with null values to fill
        blade_columns_lst = [
            'Device_Manufacturer', 'Device_Model', 'Device_SN', 'Host_Name', 
            'IP_Address', 'HBA_Description', 'HBA_Model', 'Device_Location'
            ]
        # fillna portshow_aggregated_df null values with values from blade_servers_join_df
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, blade_servers_join_df, ['Connected_portWwn'], blade_columns_lst)
        # fillna null values in Device_Host_Name from Host_Name
        portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.Host_Name, inplace = True)

    return portshow_aggregated_df


def device_name_fillna(series):
    """
    Function to fill empty values in portshow_aggregated_df Device_Host_Name column
    with combination of device class and it's wwn
    """

    mask_switch = series['deviceType'] == 'SWITCH'
    # mask_vc = series['deviceType'] == 'VC'
    mask_notna = pd.notna(series['Device_Host_Name'])
    
    # if device is switch or already has name no action required
    if mask_switch or mask_notna:
        return series['Device_Host_Name']
    
    return series['deviceType'] + ' ' + series['Connected_portWwn']


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


def create_report_tables(portshow_aggregated_df, data_names, report_columns_usage_dct, max_title):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    add_columns_lst = ['FW_Recommeneded', 'Driver_Recommeneded', 'FW_Supported', 'HW_Supported']
    portshow_aggregated_df = portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), *add_columns_lst])
    # partition aggregated DataFrame to required tables
    servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        storage_connection_df,  library_connection_df, server_connection_df = \
        dataframe_segmentation(portshow_aggregated_df, data_names, report_columns_usage_dct, max_title)
    
    # clean and sort DataFrames
    servers_report_df = _clean_dataframe(servers_report_df, 'srv')
    hba_report_df = _clean_dataframe(hba_report_df, 'srv', duplicates = ['Идентификатор порта WWPN'])
    storage_report_df = _clean_dataframe(storage_report_df, 'stor')
    library_report_df = _clean_dataframe(library_report_df, 'lib')
    storage_connection_df = _clean_dataframe(storage_connection_df, 'stor', clean = True)
    library_connection_df = _clean_dataframe(library_connection_df, 'lib', clean = True)
    server_connection_df = _clean_dataframe(server_connection_df, 'srv', clean = True)

    return servers_report_df, storage_report_df, library_report_df, \
        hba_report_df, storage_connection_df,  library_connection_df, server_connection_df
    

def _clean_dataframe(df, mask_type, duplicates = ['Имя устройства'], clean = False):
    """
    Auxiliary function to sort, remove duplicates and drop columns in cases they are not required in report DataFrame
    """
    # list of columns to check if the are empty
    columns_empty = ['Медленное устройство', 'Подключено через AG', 'Real_device_behind_AG']
    # list of columns to check if all values are equal
    columns_unique = ['Режим коммутатора', 'LSAN']
    # list of columns to sort DataFrame on
    columns_sort = ['Расположение', 'Имя устройства', 'Имя коммутатора']

    # create mask to filter required class only
    if mask_type == 'srv':
        mask = df['Класс устройства'].isin(['SRV', 'BLADE_SRV'])
    elif mask_type == 'stor':
        mask = df['Класс устройства'] == 'STORAGE'
    elif mask_type == 'lib':
        mask = df['Класс устройства'] == 'LIB'

    # filter DataFrame base on hardware type 
    df = df.loc[mask].copy()
    # check if columns required to sort on are in the DataFrame
    columns_sort = [column for column in columns_sort if column in df.columns]
    df.sort_values(by = columns_sort, inplace = True)

    # DataFrames are cleaned in two ways
    # by drop duplicate values in certain columns
    if duplicates and not clean:
        df.drop_duplicates(subset = duplicates, inplace = True)
    # or by drop entire column
    if clean:
        # if all values in the column is the same
        for column in columns_unique:
            if column in df.columns and df[column].nunique() < 2:
                df.drop(columns = [column], inplace = True)
        # if all values are None in the column
        for column in columns_empty:
            # if all values are None
            if column in df.columns and pd.isnull(df[column]).all():
                df.drop(columns = [column], inplace = True)
            # if all non None values are 'No'
            elif column in df.columns and pd.Series(df[column] == 'No', pd.notna(df[column])).all():
                df.drop(columns = [column], inplace = True)

    return df


    

