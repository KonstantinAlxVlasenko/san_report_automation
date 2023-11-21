"""Module contains functions related to storage connections"""


import utilities.dataframe_operations as dfop


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
        system_port_3par_df = dfop.convert_wwn(system_port_3par_df, ['NodeName', 'PortName'])
        # rename columns to correspond portshow_aggregated_df
        rename_columns = {'System_Name': 'Device_Name',	'System_Model':	'Device_Model', 
                            'Serial_Number': 'Device_SN', 'Location': 'Device_Location'}
        system_port_3par_df.rename(columns=rename_columns, inplace=True)
        system_port_3par_df['Device_Host_Name'] = system_port_3par_df['Device_Name']

        # add 3PAR port partner (faiolver port) Wwnp and fabric connection information
        system_port_3par_df = storage_port_partner(system_port_3par_df, portshow_aggregated_df)
        # add 3PAR information to portshow_aggregated_df
        fillna_wwnn_columns = ['Device_Name', 'Device_Host_Name', 'Device_Model', 'Device_SN', 'IP_Address', 'Device_Location']
        portshow_aggregated_df = \
            dfop.dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['NodeName'] , filled_lst=fillna_wwnn_columns)

        fillna_wwnp_columns = ['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label', 
                                'Storage_Port_Partner', 'Storage_Port_Partner_Wwnp', 
                                'Storage_Port_Mode', 'Storage_Port_Type']
        portshow_aggregated_df = \
            dfop.dataframe_fillna(portshow_aggregated_df, system_port_3par_df, join_lst=['PortName'] , filled_lst=fillna_wwnp_columns)

        portshow_aggregated_df = dfop.sequential_equality_note(portshow_aggregated_df, 
                                                            columns1=['Fabric_name', 'Fabric_label'], 
                                                            columns2=['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label'], 
                                                            note_column='Storage_Port_Partner_Fabric_equal')
    # if 3PAR configuration was not extracted apply reserved name (3PAR model and SN combination)
    if 'Device_Name_reserved' in portshow_aggregated_df.columns:
        portshow_aggregated_df['Device_Host_Name'].fillna(portshow_aggregated_df['Device_Name_reserved'], inplace = True)
    return portshow_aggregated_df


def storage_port_partner(system_port_3par_df, portshow_aggregated_df):
    """Function to add 3PAR port partner (faiolver port) Wwnp and fabric connection information to system_port_3par_df"""

    # add port partner Wwnp to system_port_3par_df
    system_port_partner_3par_df = system_port_3par_df[['configname', 'Storage_Port', 'PortName']].copy()
    system_port_partner_3par_df.rename(columns={'Storage_Port': 'Storage_Port_Partner', 'PortName': 'Storage_Port_Partner_Wwnp'}, inplace=True)
    system_port_3par_df = dfop.dataframe_fillna(system_port_3par_df, system_port_partner_3par_df, 
                                            filled_lst=['Storage_Port_Partner_Wwnp'], 
                                            join_lst=['configname', 'Storage_Port_Partner'])
    # DataDrame containing all Wwnp in san
    fabric_wwnp_columns = ['Fabric_name', 'Fabric_label', 'PortName']
    portshow_fabric_wwnp_df = portshow_aggregated_df[fabric_wwnp_columns].copy()
    portshow_fabric_wwnp_df.dropna(subset=fabric_wwnp_columns, inplace=True)
    portshow_fabric_wwnp_df.drop_duplicates(inplace=True)
    
    # rename portshow_fabric_wwnp_df columns to correspond columns in system_port_partner_3par_df DataDrame
    storage_port_partner_columns = ['Storage_Port_Partner_Fabric_name', 'Storage_Port_Partner_Fabric_label', 'Storage_Port_Partner_Wwnp']
    rename_dct = dict(zip(fabric_wwnp_columns, storage_port_partner_columns))
    portshow_fabric_wwnp_df.rename(columns=rename_dct, inplace=True)
    # fill in Fabric connection information of failover ports
    system_port_3par_df = dfop.dataframe_fillna(system_port_3par_df, portshow_fabric_wwnp_df, 
                                            join_lst=storage_port_partner_columns[2:], 
                                            filled_lst=storage_port_partner_columns[:2])
    return system_port_3par_df


def storage_oceanstore_fillna(portshow_aggregated_df, system_oceanstor_df, port_oceanstor_df):
    """Function to add Huawei OceanStore information collected from configuration files to
    portshow_aggregated_df"""
    
    if not port_oceanstor_df.empty and not system_oceanstor_df.empty:
        # system information
        system_columns = ['configname', 'Product_Model', 'System_Name', 
                            'Product_Serial_Number', 'IP_Address', 'Point Release', 'System_Location']
        system_oceanstor_cp_df = system_oceanstor_df[system_columns].copy()
        system_oceanstor_cp_df.drop_duplicates(inplace=True)
    
        # add system information to 3PAR ports DataFrame
        system_port_oceanstor_df = port_oceanstor_df.merge(system_oceanstor_cp_df, how='left', on=['configname'])
        # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
        system_port_oceanstor_df = dfop.convert_wwn(system_port_oceanstor_df, ['WWN'])
        # max speed
        system_port_oceanstor_df['Device_portSpeed_max'] = \
            system_port_oceanstor_df['Max_Speed(Mbps)'].astype('float', errors='ignore')/1000 
        
        system_port_oceanstor_df['Device_portSpeed_max'] = \
            system_port_oceanstor_df['Device_portSpeed_max'].astype('int32', errors='ignore').astype('str', errors='ignore')
        
        # rename columns to correspond portshow_aggregated_df
        rename_columns = {'System_Name': 'Device_Name',	'Product_Model': 'Device_Model', 
                            'Product_Serial_Number': 'Device_SN', 'System_Location': 'Device_Location',
                            'Point Release': 'Device_Fw', 'ID': 'Device_Port', 'WWN': 'PortName',
                            'Type': 'Storage_Port_Type', 'Role': 'Storage_Port_Mode'}
        system_port_oceanstor_df.rename(columns=rename_columns, inplace=True)
        system_port_oceanstor_df['Device_Host_Name'] = system_port_oceanstor_df['Device_Name']
        # add OceanStore information to portshow_aggregated_df
        fillna_wwpn_columns = ['Device_Name', 'Device_Host_Name', 'Device_Model', 'Device_SN', 
                               'Device_Location', 'Device_Fw', 'Device_Port', 'IP_Address', 
                               'Storage_Port_Type', 'Storage_Port_Mode', 'Device_portSpeed_max']        
        portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, system_port_oceanstor_df, 
                                                       join_lst=['PortName'], 
                                                       filled_lst=fillna_wwpn_columns)
    return portshow_aggregated_df


def construct_infinidat_node_port_from_wwn(portshow_aggregated_df, pattern_dct):
    """Function to extract node, port number from WWPN 8th octet.
    Construct Node Port in INFINIDAT format N#FC# from extracted values"""

    # filter INFINIDAT ports
    storage_ports_df = filter_empty_storage_ports(portshow_aggregated_df, storage_type='INFINIDAT')
    # extract node and port numbers
    storage_ports_df[['Node_extracted', 'Port_extracted']] = storage_ports_df['Connected_portWwn'].str.extract(pattern_dct['wwn_8th_octet']).values
    # merge node number and port number with 'FC' tag
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Node_Port_extracted', 
                                          merge_columns=['Node_extracted', 'Port_extracted'], sep='FC', drop_merge_columns=False)
    # add 'N' tag to the non empty device ports 
    mask_node_notna = storage_ports_df['Node_Port_extracted'].notna()
    dfop.column_to_object(storage_ports_df, 'Node_tag')
    storage_ports_df.loc[mask_node_notna, ['Node_tag']] = 'N'
    storage_ports_df = dfop.merge_columns(storage_ports_df, summary_column='Device_Port', 
                                          merge_columns=['Node_tag', 'Node_Port_extracted'], sep='', drop_merge_columns=False)
    # add extacted and constructed 'Device_Port' to the aggregated port DataFrame
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, storage_ports_df, 
                                                   join_lst=['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                   filled_lst=['Device_Port'])
    return portshow_aggregated_df


def filter_empty_storage_ports(portshow_aggregated_df, storage_type: str):
    """Function to filter ports of storages defined with storage_type.
    Filterd ports have no 'Device_port' extracted from switch or storage configuration files"""
    
    mask_storage = (portshow_aggregated_df[['deviceType', 'deviceSubtype']] == ('STORAGE', storage_type)).all(axis=1)
    mask_port_na = portshow_aggregated_df['Device_Port'].isna()
    storage_ports_df = portshow_aggregated_df.loc[mask_storage & mask_port_na].copy()
    return storage_ports_df




