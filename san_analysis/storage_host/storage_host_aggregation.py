import pandas as pd

import utilities.dataframe_operations as dfop

from .storage_host_functions import (drop_unequal_fabrics_ports,
                                     get_ctrl_ports_fabric,
                                     get_host_ports_fabric, verify_host_mode,
                                     verify_storage_host_zoning)


def storage_host_aggregation(host_3par_df, system_3par_df, port_3par_df, 
                            system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, 
                            host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                            hostid_ctrlportid_oceanstor_df,
                            portshow_aggregated_df, zoning_aggregated_df):
    """Function to create aggregated storage host presentation DataFrame"""

    # 3par host and controller ports
    storage_host_3par_df = generate_3par_hosts(host_3par_df, system_3par_df, port_3par_df, 
                                            portshow_aggregated_df, zoning_aggregated_df)
    # oceanstor dorado host and controller ports
    storage_host_oceanstor_df = generate_oceanstor_hosts(system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, 
                                                        host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                                                        hostid_ctrlportid_oceanstor_df,
                                                        portshow_aggregated_df, zoning_aggregated_df)
    # combine hosts from all storages
    storage_host_aggregated_df = pd.concat([storage_host_3par_df, storage_host_oceanstor_df], ignore_index=True)
    # verify if host and storage ports are in the same fabric
    storage_host_aggregated_df = dfop.sequential_equality_note(storage_host_aggregated_df, 
                                                            ['Host_Fabric_name', 'Host_Fabric_label'], 
                                                            ['Storage_Fabric_name', 'Storage_Fabric_label'],
                                                            'Host_Storage_Fabric_equal')
    # verify persona (host mode) is defined in coreespondence with host os
    storage_host_aggregated_df = verify_host_mode(storage_host_aggregated_df)
    # verify if storage port and host port are zoned
    storage_host_aggregated_df = verify_storage_host_zoning(storage_host_aggregated_df, zoning_aggregated_df)
    # sort aggregated DataFrame
    sort_columns = ['System_Name', 'Host_Id', 'Host_Name', 'Storage_Port']
    storage_host_aggregated_df.sort_values(by=sort_columns, inplace=True)
    # create storage name column free of duplicates
    storage_host_aggregated_df = dfop.remove_duplicates_from_column(storage_host_aggregated_df, 'System_Name',
                                                                duplicates_subset=['configname', 'System_Name'], ) 
    return storage_host_aggregated_df


def generate_oceanstor_hosts(system_oceanstor_df, port_oceanstor_df, host_oceanstor_df, 
                            host_id_name_oceanstor_df, host_id_fcinitiator_oceanstor_df, 
                            hostid_ctrlportid_oceanstor_df,
                            portshow_aggregated_df, zoning_aggregated_df):
    
    if system_oceanstor_df.empty:
        return pd.DataFrame()
    
    storage_host_dorado_v3_df = generate_dorado_v3_hosts(port_oceanstor_df, host_oceanstor_df)
    storage_host_dorado_v6_df = generate_dorado_v6_hosts(port_oceanstor_df, host_id_name_oceanstor_df, 
                                                        host_id_fcinitiator_oceanstor_df, hostid_ctrlportid_oceanstor_df)
    # concatenate dorado hosts v3 and v6
    storage_host_oceanstor_df = pd.concat([storage_host_dorado_v6_df, storage_host_dorado_v3_df], ignore_index=True)
    # add system_name
    storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, system_oceanstor_df, 
                                                    join_lst=['configname'], filled_lst=['System_Name'])
    # rename
    storage_host_oceanstor_df.rename(columns={'Os_Type': 'Persona'}, inplace=True)    
    # convert wwpn
    storage_host_oceanstor_df = dfop.convert_wwn(storage_host_oceanstor_df, ['Host_Wwn', 'PortName'])
    # add controller ports NodeName
    storage_host_oceanstor_df = dfop.dataframe_fillna(storage_host_oceanstor_df, portshow_aggregated_df, 
                                                        join_lst=['PortName'], filled_lst=['NodeName'])
    # add controllers ports Fabric_name and Fabric_label
    storage_host_oceanstor_df = get_ctrl_ports_fabric(portshow_aggregated_df, zoning_aggregated_df, storage_host_oceanstor_df)       
    # add host ports fabric information
    storage_host_oceanstor_df = get_host_ports_fabric(portshow_aggregated_df, storage_host_oceanstor_df)
    # filter off rows where host and controller ports are in different fabrics
    storage_host_oceanstor_df = drop_unequal_fabrics_ports(storage_host_oceanstor_df)
    return storage_host_oceanstor_df
    

def generate_dorado_v3_hosts(port_oceanstor_df, host_oceanstor_df):
    """Function to combine hosts and controllers fcports for Dorado V3"""

    if host_oceanstor_df.empty:
        return pd.DataFrame()

    host__old_columns = ['configname', 'Host_Id', 'Host_Name', 'Os_Type', 'Host_IP', 'Host_Wwn']
    storage_host_dorado_v3_df = host_oceanstor_df[host__old_columns].copy()
    # add controllers ports
    # dorado v3 doesn't have host and controllers port relation in config filr
    # so to each host all controller fcports in 'Up' state is added
    mask_online_port = port_oceanstor_df['Running_Status'].str.contains('up', case=False, na=None)
    port_oceanstor_cp_df = port_oceanstor_df.loc[mask_online_port].copy()
    port_oceanstor_cp_df['PortName'] = port_oceanstor_df['WWN']
    port_oceanstor_cp_df['Storage_Port'] = port_oceanstor_df['ID']
    storage_host_dorado_v3_df = dfop.dataframe_fillna(storage_host_dorado_v3_df, port_oceanstor_cp_df, 
                                                    join_lst=['configname'], 
                                                    filled_lst=['Storage_Port', 'PortName'], remove_duplicates=False)
    # remove '0x' symbol from host wwn
    storage_host_dorado_v3_df['Host_Wwn'] = storage_host_dorado_v3_df['Host_Wwn'].str.extract('0x(.+)')
    return storage_host_dorado_v3_df


def generate_dorado_v6_hosts(port_oceanstor_df, host_id_name_oceanstor_df, 
                            host_id_fcinitiator_oceanstor_df, hostid_ctrlportid_oceanstor_df):
    """Function to combine hosts and controllers fcports for Dorado V6"""
    
    if host_id_name_oceanstor_df.empty:
        return pd.DataFrame()

    host_columns = ['configname', 'Host_Id', 'Host_Name', 'Os_Type', 'Host_IP']
    storage_host_dorado_v6_df = host_id_name_oceanstor_df[host_columns].copy()
    # add host fc ports
    storage_host_dorado_v6_df = dfop.dataframe_fillna(storage_host_dorado_v6_df, host_id_fcinitiator_oceanstor_df, 
                                                    join_lst=['configname', 'Host_Id'], 
                                                    filled_lst=['Host_Wwn'], remove_duplicates=False)
    # create hostid - single portid relation from hostid - portids list realtion
    hostid_ctrlportid_oceanstor_expl_df = explode_hostid_portid_relation(hostid_ctrlportid_oceanstor_df)
    # add controller portids
    storage_host_dorado_v6_df = dfop.dataframe_fillna(storage_host_dorado_v6_df, hostid_ctrlportid_oceanstor_expl_df, 
                                                    join_lst=['configname', 'Host_Id'], 
                                                    filled_lst=['Storage_Port', 'LUN_quantity'], remove_duplicates=False)
    # add controller port Wwpn
    port_oceanstor_cp_df = port_oceanstor_df.copy()
    port_oceanstor_cp_df['PortName'] = port_oceanstor_df['WWN']
    port_oceanstor_cp_df['Storage_Port'] = port_oceanstor_df['ID']
    storage_host_dorado_v6_df = dfop.dataframe_fillna(storage_host_dorado_v6_df, port_oceanstor_cp_df, 
                                                    join_lst=['configname', 'Storage_Port'], 
                                                    filled_lst=['PortName'])
    return storage_host_dorado_v6_df
    

def explode_hostid_portid_relation(hostid_ctrlportid_oceanstor_df):
    """Function to create hostid - single portid relation from hostid - portids list realtion.
    Each hostid-portid relation presented as single row"""

    # hostid, related controller portid and number of presented luns
    hostid_ctrlportid_oceanstor_expl_df = hostid_ctrlportid_oceanstor_df.copy()
    # count luns quantity for each hostid
    hostid_ctrlportid_oceanstor_expl_df['LUN_quantity'] = hostid_ctrlportid_oceanstor_expl_df['LUN_ID_List'].str.count('\w+')
    # explode portid lists column so eash hostid, portid presented as a single row
    hostid_ctrlportid_oceanstor_expl_df = dfop.explode_columns(hostid_ctrlportid_oceanstor_expl_df, 'Port_ID_List', sep=',')
    # drop columns with lunid lists and exploaded column name
    hostid_ctrlportid_oceanstor_expl_df.drop(columns=['LUN_ID_List', 'Exploded_column'], inplace=True)
    # rename portid column
    hostid_ctrlportid_oceanstor_expl_df.rename(columns={'Exploded_values': 'Storage_Port'}, inplace=True)
    return hostid_ctrlportid_oceanstor_expl_df



def generate_3par_hosts(host_3par_df, system_3par_df, port_3par_df, 
                        portshow_aggregated_df, zoning_aggregated_df):
    """Function to combine hosts and controllers fcports for 3Par"""
    
    if system_3par_df.empty:
        return pd.DataFrame()
    
    storage_host_3par_df = host_3par_df.copy()
    # add system_name
    storage_host_3par_df = dfop.dataframe_fillna(storage_host_3par_df, system_3par_df, 
                                                    join_lst=['configname'], filled_lst=['System_Name'])
    # add controller's ports Wwnp and Wwnp
    storage_host_3par_df = dfop.dataframe_fillna(storage_host_3par_df, port_3par_df, 
                                                    join_lst=['configname', 'Storage_Port'], filled_lst=['NodeName', 'PortName'])
    # convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)
    storage_host_3par_df = dfop.convert_wwn(storage_host_3par_df, ['Host_Wwn', 'NodeName', 'PortName'])
    # add controllers ports Fabric_name and Fabric_label
    storage_host_3par_df = get_ctrl_ports_fabric(portshow_aggregated_df, zoning_aggregated_df, storage_host_3par_df)    
    # add host ports fabric information
    storage_host_3par_df = get_host_ports_fabric(portshow_aggregated_df, storage_host_3par_df)
    return storage_host_3par_df