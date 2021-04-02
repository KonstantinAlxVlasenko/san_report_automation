"""
Module to add Blade Servers and Virtual connect information to portcmd table.
Auxiliary to analysis_portcmd module.
"""

import re
import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_fillna

# auxiliary lambda function to combine two columns in DataFrame
# it combines to columns if both are not null and takes second if first is null
# str1 and str2 are strings before columns respectively (default is whitespace between columns)
wise_combine = lambda series, str1='', str2=' ': \
    str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
        if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]


def blade_server_fillna(portshow_aggregated_df, blade_servers_df, synergy_servers_df, re_pattern_lst):
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
        # lower case WWNp
        blade_servers_join_df.portWwn = blade_servers_join_df.portWwn.str.lower()

        # hostname_clean_comp
        blade_servers_join_df.Host_Name = blade_servers_join_df.Host_Name.replace(comp_dct[comp_keys[0]], np.nan, regex=True)
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

    if not synergy_servers_df.empty:
        # columns with null values to fill

        synergy_servers_df['Device_Manufacturer'] = 'HPE'

        synergy_columns_lst = [
            'Device_Manufacturer', 'Device_Model', 'Device_SN', 'Host_Name', 'Host_OS',
            'HBA_Description', 'HBA_Firmware', 'Device_Location'
            ]

        # fillna portshow_aggregated_df null values with values from blade_servers_join_df
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, synergy_servers_df, ['Connected_portWwn'], synergy_columns_lst)
        # fillna null values in Device_Host_Name from Host_Name
        portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.Host_Name, inplace = True)        



    return portshow_aggregated_df


def blade_vc_fillna(portshow_aggregated_df, blade_module_df, blade_vc_df, synergy_module_df):
    """
    Function to fillna portshow_aggregated_df null values with values from blade_vc_join_df.
    Virtual connect information.
    """

    interconnect_module_columns_dct = {'Port': 'Device_Port', 'portWwn': 'Connected_portWwn', 
                                        'Interconnect_Name': 'Device_Host_Name', 
                                        'Interconnect_Manufacturer': 'Device_Manufacturer', 
                                        'Interconnect_Model': 'Device_Model', 'Interconnect_SN': 'Device_SN', 
                                        'Interconnect_IP': 'IP_Address', 'Interconnect_Firmware': 'Device_Fw'}    

    if not blade_vc_df.empty:
        # columns of Blade Interconnect modules DataFrame
        module_lst = ['Enclosure_Name',	'Enclosure_SN', 'Interconnect_Bay', 
                    'Interconnect_Name', 'Interconnect_Manufacturer', 
                    'Interconnect_Model', 'Interconnect_SN', 
                    'Interconnect_IP', 'Interconnect_Firmware']
        # columns of Blade Virtual Connect modules DataFrame 
        vc_lst = ['Enclosure_Name', 'Enclosure_SN', 
                    'Interconnect_Bay', 'Port', 'portWwn']
        
        blade_vc_join_df = blade_vc_df.loc[:, vc_lst].copy()
        blade_module_join_df = blade_module_df.loc[:, module_lst]
        # combine 'Enclosure_Name' and 'Bay' columns
        blade_vc_join_df['Device_Location'] = \
            blade_vc_join_df[['Enclosure_Name', 'Interconnect_Bay']].apply(wise_combine, axis=1, args=('Enclosure ', ' bay '))
            
        # extend Blade Virtual Connect modules DataFrame with values from
        # Blade Interconnect modules DataFrame
        blade_vc_join_df = blade_vc_join_df.merge(blade_module_join_df, how = 'left', \
            on = ['Enclosure_Name',	'Enclosure_SN', 'Interconnect_Bay'])

        # rename Blade Virtual Connect modules DataFrame columns to correspond
        # portshow_aggregated_df DataFrame
        blade_vc_join_df.rename(columns = interconnect_module_columns_dct,  inplace = True)

        # apply lower case to WWNp column
        blade_vc_join_df.Connected_portWwn = blade_vc_join_df.Connected_portWwn.str.lower()
        # fillna portshow_aggregated_df null values with values Blade Virtual Connect modules DataFram
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, blade_vc_join_df, ['Connected_portWwn'], 
                                                ['Device_Port', 'Device_Location', 'Device_Host_Name', 'Device_Manufacturer', 
                                                'Device_Model', 'Device_SN', 'IP_Address', 'Device_Fw'])
                            
    if not synergy_module_df.empty:
        synergy_module_join_df = synergy_module_df.rename(columns=interconnect_module_columns_dct)
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, synergy_module_join_df, ['NodeName'], 
                                                ['Device_Location', 'Device_Host_Name', 
                                                'Device_Model', 'Device_SN', 'Device_Fw'])        

    return portshow_aggregated_df


def vc_name_fillna(portshow_aggregated_df):

        mask_vc = portshow_aggregated_df['deviceType'] == 'VC'
        mask_sn = portshow_aggregated_df['Device_SN'].notna()
        mask_devicename_empty = portshow_aggregated_df['Device_Host_Name'].isna()
        mask_complete = mask_vc & mask_sn & mask_devicename_empty
        portshow_aggregated_df.loc[mask_complete, 'Device_Host_Name'] = 'VC' + portshow_aggregated_df['Device_SN']

        return portshow_aggregated_df


def storage_3par_fillna(portshow_aggregated_df, system_3par_df, port_3par_df):
    """Function to add 3PAR information collected from 3PAR configuration files to
    portshow_aggregated_df"""

    if not port_3par_df.empty and not system_3par_df.empty:
        # system information
        system_columns = ['configname', 'System_Model', 'System_Name', 
                            'Serial_Number', 'IP_Address', 'Location']
        system_3par_cp_df = system_3par_df[system_columns].copy()
        system_3par_cp_df.drop_duplicates(inplace=True)

        # add system information to 3PAR ports DataFrame
        system_port_3par_df = port_3par_df.merge(system_3par_cp_df, how='left', on=['configname'])
        # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
        for  wwn_column in ['NodeName', 'PortName']:
            system_port_3par_df[wwn_column] = system_port_3par_df[wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
            system_port_3par_df[wwn_column] = system_port_3par_df[wwn_column].str.lower()

        # rename columns to correspond portshow_aggregated_df
        rename_columns = {'System_Name': 'Device_Name',	'System_Model':	'Device_Model', 
                            'Serial_Number': 'Device_SN', 'Location': 'Device_Location'}
        system_port_3par_df.rename(columns=rename_columns, inplace=True)
        system_port_3par_df['Device_Host_Name'] = system_port_3par_df['Device_Name']

        # add 3PAR information to portshow_aggregated_df
        fillna_wwnn_columns = ['Device_Name', 'Device_Host_Name', 'Device_Model', 'Device_SN', 'IP_Address', 'Device_Location']
        portshow_aggregated_df = \
            dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['NodeName'] , filled_lst=fillna_wwnn_columns)

        fillna_wwnp_columns = ['Storage_Port_Mode', 'Storage_Port_Type',	'Storage_Port_Partner']
        portshow_aggregated_df = \
            dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['PortName'] , filled_lst=fillna_wwnp_columns)

    # if 3PAR configuration was not extracted apply reserved name (3PAR model and SN combination)
    if 'Device_Name_reserved' in portshow_aggregated_df.columns:
        portshow_aggregated_df['Device_Host_Name'].fillna(portshow_aggregated_df['Device_Name_reserved'], inplace = True)

    return portshow_aggregated_df