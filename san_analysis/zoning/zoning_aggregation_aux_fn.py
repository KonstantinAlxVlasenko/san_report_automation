"""Module with auxiliary functions to process zoning configuration in analysis_zoning_aggregation module"""

import re
import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop

def wwn_type(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df):
    """Function to verify which type of WWN (port or node WWN) is used for each alias_member"""

    # list of all Node WWNs in Fabric
    wwnn_lst = sorted(portshow_aggregated_df.NodeName.dropna().drop_duplicates().to_list())
    # list of all Port WWns in Fabric
    wwnp_lst = sorted(portshow_aggregated_df.PortName.dropna().drop_duplicates().to_list())
    # list of WWNs for which Port and Node WWN values are the same
    wwn_intersection = set(wwnp_lst).intersection(wwnn_lst)
    # remove identical WWNs (intersection lst WWNs) from Node and Port WWNs lists
    wwnp_clear_lst = [wwnp for wwnp in wwnp_lst if wwnp not in wwn_intersection]
    wwnn_clear_lst = [wwnn for wwnn in wwnn_lst if wwnn not in wwn_intersection]
    
    def wwn_check(wwn, wwnp_lst, wwnn_lst, wwn_intersection):
        # if Node and Port WWNs are identical
        # than WWNP is considered to be used
        if wwn in wwn_intersection:
            return 'Wwnp'
        if wwn in wwnp_lst:
            return 'Wwnp'
        if wwn in wwnn_lst:
            return 'Wwnn'
        return np.nan

    zoning_aggregated_df['Wwn_type'] = zoning_aggregated_df.alias_member.apply(
                                        lambda wwn: wwn_check(wwn, wwnp_clear_lst, wwnn_clear_lst, wwn_intersection))
    alias_aggregated_df['Wwn_type'] = alias_aggregated_df.alias_member.apply(
                                        lambda wwn: wwn_check(wwn, wwnp_lst, wwnn_lst, wwn_intersection))
    return zoning_aggregated_df, alias_aggregated_df


def replace_wwnn(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df):
    """Function to replace each wwnn in zoning configuration with it's wwnp
    if wwnn is present in the same fabric."""

    # create DataFrame with WWNP and WWNN
    port_node_name_df = portshow_aggregated_df[['Fabric_name', 'Fabric_label', 'PortName', 'NodeName']].copy()
    port_node_name_df.dropna(subset = ['Fabric_name', 'Fabric_label', 'PortName', 'NodeName'], inplace=True)
    # check if Wwnn corresponds to two or more Wwnps
    port_node_name_df['Wwnn_unpack'] = np.nan
    mask_duplicated_wwnn = ~port_node_name_df.duplicated(subset=['Fabric_name', 'Fabric_label', 'NodeName'], keep=False)
    port_node_name_df['Wwnn_unpack'] = port_node_name_df['Wwnn_unpack'].where(mask_duplicated_wwnn, 'Да')

    port_node_name_df.rename(columns={'PortName': 'Strict_Wwnp', 'NodeName': 'alias_member'}, inplace=True )
    # merge zoning_aggregated_df and alias_aggregated_df with port_node_name_df based on alias members and WWNN values
    # thus finding for each alias member defined through WWNN it's WWNP number
    # if no aliases defined through the WWNN then Strict_Wwnp column is empty
    zoning_aggregated_df = zoning_aggregated_df.merge(port_node_name_df, how='left', on=['Fabric_name', 'Fabric_label', 'alias_member'])
    alias_aggregated_df = alias_aggregated_df.merge(port_node_name_df, how='left', on=['Fabric_name', 'Fabric_label', 'alias_member'])
    # all empty cells in Strict_Wwnp column after merging mean that corresponding cells in
    # alias_member contain WWNP values. 
    # fillna copies those WWNP values to the Strict_Wwnp columns thus this column
    # contains WWNP values only
    zoning_aggregated_df.Strict_Wwnp.fillna(zoning_aggregated_df.alias_member, inplace=True)
    alias_aggregated_df.Strict_Wwnp.fillna(alias_aggregated_df.alias_member, inplace=True)

    return zoning_aggregated_df, alias_aggregated_df


def replace_domain_index(aggregated_df, portshow_aggregated_df):
    """Function to replace Domain_Index with Wwpn"""

    aggregated_df['Strict_Wwnp'].replace(to_replace='\d+,\d+', value=np.nan, regex=True, inplace=True)
    portshow_cp = portshow_aggregated_df.copy()
    portshow_cp.rename(columns={'PortName': 'Strict_Wwnp', 'Domain_Index': 'alias_member'}, inplace=True)
    aggregated_df = dfop.dataframe_fillna(aggregated_df, portshow_cp, join_lst=['Fabric_name', 'Fabric_label', 'alias_member'], 
                                            filled_lst=['Strict_Wwnp'], remove_duplicates=False)
    return aggregated_df


def zonemember_connection(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df):
    """Function to find each zonememeber and aliasmember fabric connection if it's exist"""

    port_columns_lst = ['Fabric_name', 'Fabric_label', 
                    'Device_Host_Name', 'Group_Name', 'Device_Port',
                    'Device_type', 'deviceType', 'deviceSubtype', 'portType',
                    'PortName', 'NodeName', 'Connected_portId',
                    'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 
                    'Index_slot_port', 'portIndex', 'slot', 'port']

    # connection information revealed by merging based on WWNP number
    # of connected device switch port  (PortName) and 
    # WWNP number of alias_member (Strict_Wwnp)
    # AG mode switches dropped to avoid duplicate  connection information
    mask_switch_native = portshow_aggregated_df['switchMode'] == 'Native'
    portcmd_join_df = portshow_aggregated_df.loc[mask_switch_native, port_columns_lst].copy()
    portcmd_join_df['Strict_Wwnp'] = np.nan
    portcmd_join_df['Strict_Wwnp'].fillna(portcmd_join_df.PortName, inplace=True)
    # zonemember_Fabric_name and zonemember_Fabric_label show which fabric device connected to.
    # Fabric_name and Fabric_label show which Fabric zone is defined in.
    portcmd_join_df = portcmd_join_df.rename(
        columns={'Fabric_name': 'zonemember_Fabric_name', 'Fabric_label': 'zonemember_Fabric_label'})
    portcmd_join_df.dropna(subset=['Strict_Wwnp'], inplace=True)
                                              
    zoning_aggregated_df = zoning_aggregated_df.merge(portcmd_join_df, how='left', on=['Strict_Wwnp'])
    alias_aggregated_df = alias_aggregated_df.merge(portcmd_join_df, how='left', on=['Strict_Wwnp'])
    # no need in Strict_Wwnp column. WWNP values are in PortName column
    zoning_aggregated_df.drop(columns=['Strict_Wwnp'], inplace=True)
    alias_aggregated_df.drop(columns=['Strict_Wwnp'], inplace=True)

    return zoning_aggregated_df, alias_aggregated_df


def verify_enforcement_type(aggregated_df, portshow_aggregated_df):
    """Function to add zone enforcemnt type (hard wwn, hard port, session)"""

    port_columns_lst = ['chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Index_slot_port']
    aggregated_df = dfop.dataframe_fillna(aggregated_df, portshow_aggregated_df, join_lst=port_columns_lst, filled_lst=['zoning_enforcement'])
    return aggregated_df


def zonemember_in_cfg_fabric_verify(zoning_aggregated_df, lsan=True):
    """Function to check if zoned device available in Fabric where configuration is defined"""

    # check if device defined in zone (zonemember, alias) connected to the same fabric where zone configuration is defined  
    zoning_aggregated_df['Member_in_cfg_Fabric'] = \
        (zoning_aggregated_df['Fabric_name'] == zoning_aggregated_df['zonemember_Fabric_name']) & \
            (zoning_aggregated_df['Fabric_label'] == zoning_aggregated_df['zonemember_Fabric_label'])
    
    zoning_aggregated_df['Fabric_device_status'] = np.nan
    zoning_aggregated_df['Fabric_device_status'] = zoning_aggregated_df['Fabric_device_status'].fillna(zoning_aggregated_df['Member_in_cfg_Fabric'])
    # remove False values for devices which are not connected to any fabric (leave blank)
    zoning_aggregated_df['Member_in_cfg_Fabric'] = \
        zoning_aggregated_df['Member_in_cfg_Fabric'].where(pd.notna(zoning_aggregated_df.zonemember_Fabric_name), np.nan)

    zoning_aggregated_df['Fabric_device_status'] = \
        zoning_aggregated_df['Fabric_device_status'].where(pd.notna(zoning_aggregated_df.zonemember_Fabric_name), 'absent')

    # Replace 0 and 1 with Yes and No
    zoning_aggregated_df['Member_in_cfg_Fabric'].replace(to_replace={1: 'Да', 0: 'Нет'}, inplace = True)
    zoning_aggregated_df['Member_in_cfg_Fabric'].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)
    zoning_aggregated_df['Fabric_device_status'].replace(to_replace={1: 'local', 0: 'remote_na', True: 'local'}, inplace = True)
    zoning_aggregated_df['Fabric_device_status'].replace(to_replace={True: 'local', False: 'remote_na'}, inplace = True)

    # mark devices which are not in the same fabric with principal switch where configiguration defined
    # but which is part of LSAN zone for that fabric and device status is Imported as Yes 
    mask_member_imported = zoning_aggregated_df['LSAN_device_state'].str.contains('Imported', na=False)
    mask_member_configured = zoning_aggregated_df['LSAN_device_state'].str.contains('Configured', na=False)
    mask_member_initializing = zoning_aggregated_df['LSAN_device_state'].str.contains('Initializing', na=False)
    mask_fabric_name = pd.notna(zoning_aggregated_df['zonemember_Fabric_name'])

    zoning_aggregated_df['Member_in_cfg_Fabric'] = \
        np.where((mask_member_imported & mask_fabric_name), 'Да', zoning_aggregated_df['Member_in_cfg_Fabric'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_imported & mask_fabric_name), 'remote_imported', zoning_aggregated_df['Fabric_device_status'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_configured & mask_fabric_name), 'remote_configured', zoning_aggregated_df['Fabric_device_status'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_initializing & mask_fabric_name), 'remote_initializing', zoning_aggregated_df['Fabric_device_status'])
    
    # replace 'absent' status for peerzone property member for np.nan 
    if 'peerzone_member_type' in zoning_aggregated_df.columns:
        mask_peerzone_property = zoning_aggregated_df['peerzone_member_type'].str.contains('property', na=False)
        zoning_aggregated_df['Fabric_device_status'] = \
            np.where((mask_peerzone_property), np.nan, zoning_aggregated_df['Fabric_device_status'])
    return zoning_aggregated_df


def alias_cfg_type(alias_aggregated_df, zoning_aggregated_df):
    """Function to check which type of configuration alias apllied in (effective or defined)"""

    # separate effective and defined configs zoning configurations
    mask_effective = zoning_aggregated_df['cfg_type'].str.contains('effective', na=False)
    mask_defined = zoning_aggregated_df['cfg_type'].str.contains('defined', na=False)
    cfg_lst = ['Fabric_name', 'Fabric_label', 'zone_member', 'alias_member', 'cfg_type']
    cfg_effective_df = zoning_aggregated_df.loc[mask_effective, cfg_lst]
    cfg_defined_df = zoning_aggregated_df.loc[mask_defined, cfg_lst]
    # fill empty cfg_type values in alias_aggregated_df DataFrame with config type from cfg_effective_df
    # thus if alias is in effective config it's marked as effective
    alias_aggregated_df = dfop.dataframe_fillna(alias_aggregated_df, cfg_effective_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    # fill rest empty values with defined word if alias is in defined config
    alias_aggregated_df = dfop.dataframe_fillna(alias_aggregated_df, cfg_defined_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    return alias_aggregated_df


def verify_cfg_type(aggregated_df, zoning_aggregated_df, search_lst):
    """Function to check which type of configuration 'search_lst' apllied in (effective or defined)"""

    # separate effective and defined configs zoning configurations
    mask_effective = zoning_aggregated_df['cfg_type'].str.contains('effective', na=False)
    mask_defined = zoning_aggregated_df['cfg_type'].str.contains('defined', na=False)
    
    cfg_lst = [*['Fabric_name', 'Fabric_label'], *search_lst, *['cfg_type']]
    cfg_effective_df = zoning_aggregated_df.loc[mask_effective, cfg_lst]
    cfg_defined_df = zoning_aggregated_df.loc[mask_defined, cfg_lst]
    # fill empty cfg_type values in alias_aggregated_df DataFrame with config type from cfg_effective_df
    # thus if alias is in effective config it's marked as effective
    aggregated_df = dfop.dataframe_fillna(aggregated_df, cfg_effective_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    # fill rest empty values with defined word if alias is in defined config
    aggregated_df = dfop.dataframe_fillna(aggregated_df, cfg_defined_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    return aggregated_df


def sort_dataframe(zoning_aggregated_df, alias_aggregated_df):
    """Function to sort zoning configuration based on config type, fabric labels and devices zoned"""

    sort_zone_lst = ['Fabric_label', 'Fabric_name', 'cfg_type', 'cfg' , 'zone', 'deviceType', 'Device_Host_Name', 'zone_member']
    # sort_zone_lst = ['cfg_type', 'Fabric_label', 'cfg', 'zone', 'deviceType', 'zone_member', 'Fabric_name']
    sort_alias_lst = ['cfg_type', 'Fabric_label', 'Fabric_name', 'zone_member', 'Wwn_type']

    zoning_aggregated_df.sort_values(by=sort_zone_lst, \
        ascending=[True, True, False, *5*[True]], inplace=True)
    # zoning_aggregated_df.sort_values(by=sort_zone_lst, \
    #     ascending=[False, *6*[True]], inplace=True)
    alias_aggregated_df.sort_values(by=sort_alias_lst, \
        ascending=[False, *4*[True]], inplace=True)
    return zoning_aggregated_df, alias_aggregated_df


# def verify_alias_duplicate(alias_aggregated_df):
#     """
#     Function to check if alias_member (wwnp or wwnn) has duplicated aliases and
#     counts its number if they exist
#     """

#     # perform zone_member (alias names) grouping based on alias_member (set of wwnn and wwnp) for each fabric
#     alias_count_columns = ['Fabric_name', 'Fabric_label', 'alias_member']
#     # join zone_members in each group

#     # agg({'text': lambda x: ' '.join(set(x))})
#     # agg({'zone_member': ', '.join})


#     alias_duplicated_names_df = alias_aggregated_df.groupby(alias_count_columns, as_index = False).agg({'zone_member': lambda x: ', '.join(set(x))})    
#     # rename column with joined zone_members names and perform merge with aggregated DataFrame
#     alias_duplicated_names_df.rename(columns={'zone_member': 'alias_duplicated'}, inplace=True)
#     alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_names_df, how='left', on=alias_count_columns)

#     # count number of alias(es) for the alias_member (wwnn or wwnp)
#     alias_duplicated_number_df = alias_aggregated_df.groupby(alias_count_columns, as_index=False).agg({'zone_member': lambda x: len(set(x))})
#     alias_duplicated_number_df.rename(columns={'zone_member': 'alias_count'}, inplace=True)
#     alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_number_df, how='left', on=alias_count_columns)


#     # leave duplicated zone_memebers only
#     mask_duplicated = alias_aggregated_df['alias_duplicated'] != alias_aggregated_df['zone_member']
#     alias_aggregated_df['alias_duplicated'] = alias_aggregated_df['alias_duplicated'].where(mask_duplicated, np.nan)
    
#     # # count number of alias(es) for the alias_member (wwnn or wwnp)
#     # alias_duplicated_number_df = alias_aggregated_df.groupby(alias_count_columns, as_index = False).agg({'zone_member': 'count'})
#     # alias_duplicated_number_df.rename(columns={'zone_member': 'alias_count'}, inplace=True)
#     # alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_number_df, how='left', on=alias_count_columns)

#     return alias_aggregated_df


def verify_alias_duplicate(alias_aggregated_df):
    """
    Function to check if alias_member (wwnp or wwnn) has duplicated aliases and
    counts its number if they exist
    """

    # perform zone_member (alias names) grouping based on alias_member (set of wwnn and wwnp) for each fabric
    alias_count_columns = ['Fabric_name', 'Fabric_label', 'PortName']
    # join zone_members in each group
    alias_duplicated_names_df = alias_aggregated_df.groupby(alias_count_columns, as_index = False).agg({'zone_member': lambda x: ', '.join(set(x))})    
    # rename column with joined zone_members names and perform merge with aggregated DataFrame
    alias_duplicated_names_df.rename(columns={'zone_member': 'alias_duplicated'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_names_df, how='left', on=alias_count_columns)

    # count number of alias(es) for the alias_member (wwnn or wwnp)
    alias_duplicated_number_df = alias_aggregated_df.groupby(alias_count_columns, as_index=False).agg({'zone_member': lambda x: len(set(x))})
    alias_duplicated_number_df.rename(columns={'zone_member': 'alias_count'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_number_df, how='left', on=alias_count_columns)
    # alias_aggregated_df['alias_count'].fillna(1, inplace=True)

    # leave duplicated zone_memebers only
    mask_duplicated = alias_aggregated_df['alias_duplicated'] != alias_aggregated_df['zone_member']
    alias_aggregated_df['alias_duplicated'] = alias_aggregated_df['alias_duplicated'].where(mask_duplicated, np.nan)
    return alias_aggregated_df


def zone_using_alias(zoning_aggregated_df, alias_aggregated_df):
    """Function to identify which zones use zone_member (alias) and count number of zones"""

    # perform grouping based on zone_member (alias name) and alias_member (wwnn or wwnp) in each fabric
    # it is required to take into account alias_member to avoid zone number multiplication in case if zone_member
    # contains several alias_memeber(s)
    zone_count_columns = ['Fabric_name', 'Fabric_label', 'zone_member', 'alias_member']
    # drop rows with duplicated alias_members in zones (in case if wwnn splitted into several wwnps earlier)
    zoning_effective_defined_duplicates_free_df = zoning_aggregated_df.drop_duplicates(subset=zone_count_columns + ['zone'])
    # join zone_names for each group
    alias_zone_names_df = zoning_effective_defined_duplicates_free_df.groupby(zone_count_columns, as_index = False).agg({'zone': ', '.join})
    # rename column with joined zone_names and perform merge with aggregated DataFrame
    alias_zone_names_df.rename(columns={'zone': 'zone_name_alias_used_in'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_zone_names_df, how='left', on=zone_count_columns)

    # perform zones count
    alias_zone_number_df = zoning_effective_defined_duplicates_free_df.groupby(zone_count_columns, as_index = False).agg({'zone': 'count'})
    alias_zone_number_df.rename(columns={'zone': 'zone_number_alias_used_in'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_zone_number_df, how='left', on=zone_count_columns)
    alias_aggregated_df['zone_number_alias_used_in'].fillna(0, inplace=True)
    return alias_aggregated_df


def wwnp_instance_number_per_group(aggregated_df, df_type):
    """
    Auxiliary function to identify is there duplicated wwnp values inside each group
    (zone or alias). Function calculates how many times wwnp used inside zone or alias.
    Normally each wwnp used only once.
    """
    
    # columns to perform grouping on
    group_columns = ['Fabric_name',	'Fabric_label']
    
    if df_type == 'zone':
        group_columns = [*group_columns, *['cfg', 'cfg_type', 'zone', 'PortName']]
        wwnp_number_column = 'wwnp_instance_number_per_zone'
    elif df_type == 'alias':
        group_columns = [*group_columns, *['zone_member', 'PortName']]
        wwnp_number_column = 'wwnp_instance_number_per_alias'
    
    # count wwnp instances for each group
    wwnp_number_df = aggregated_df.groupby(group_columns).PortName.count()
    wwnp_number_df = pd.DataFrame(wwnp_number_df)
    wwnp_number_df.rename(columns={'PortName': wwnp_number_column}, inplace=True)
    wwnp_number_df.reset_index(inplace=True)
    # add wwnp instance number to zoning or aliases DataFrames
    aggregated_df = aggregated_df.merge(wwnp_number_df, how='left', on=group_columns)
    return aggregated_df


def verify_zonemember_type(aggregated_df, column = 'zone_member'):
    """Function to verify what type of member in zone configuration (alias, wwn, DI) """

    # create column name with verified values
    verified_column = column + '_type'
    member_type = column.replace('_', '')
    # copy values from column to be verified to column with verified values
    aggregated_df[verified_column] = aggregated_df[column]

    # create dictionary with compiled regular expressins for alias, wwn and DI 
    alias_regex = re.compile(r'^[\w_$^-]+$')
    wwn_regex = re.compile(r'^([0-9a-fA-F]{2}:){7}[0-9a-fA-F]{2}$')
    domain_portindex_regex = re.compile(r'^\d+,\d+$')
    replace_dct = {alias_regex: member_type + '_alias',
                    wwn_regex: member_type + '_wwn',
                    domain_portindex_regex: member_type + '_domain_portindex'}

    # replace values in verified column with values from dict 
    aggregated_df[verified_column] = aggregated_df[verified_column].replace(to_replace=replace_dct, regex=True)
    # remove wwn type from property member
    if 'peerzone_member_type' in aggregated_df.columns:
        mask_peerzone_property = aggregated_df['peerzone_member_type'].str.contains('property', na=False)
        aggregated_df.loc[mask_peerzone_property, verified_column] = np.nan
    return aggregated_df

# TO REMOVE
# def count_group_members(df, count_columns: dict, group_columns):
#     """Function to count members in groups and merge information to DataFrame"""

#     for count_column, rename_column in count_columns.items():
#         if count_column in df.columns:
#             current_sr = df.groupby(by=group_columns)[count_column].count()
#             current_df = pd.DataFrame(current_sr)
#             # current_df.reset_index(inplace=True)
#             current_df.rename(columns={count_column: rename_column}, inplace=True)
#             current_df.reset_index(inplace=True)
#             df = df.merge(current_df, how='left', on=group_columns)

#     return df



def verify_device_hostname_instances(aggregated_df, portshow_aggregated_df):
    """Function to add Device_Host_Name instances number on (fabric_name, fabric_label), 
    fabric_label and total_fabrics levels based on PortName (WWNp)"""

    fabric_wwnp_columns = ['Fabric_name', 'Fabric_label', 'PortName']
    storage_port_columns = ['Storage_Port_Type']
    device_port_columns = ['Device_Host_Name_per_fabric_name_and_label', 
                            'Device_Host_Name_per_fabric_label',
                            'Device_Host_Name_per_fabric_name', 
                            'Device_Host_Name_total_fabrics']

    storage_port_columns = [column for column in storage_port_columns if column in portshow_aggregated_df.columns]
    device_port_columns = [column for column in device_port_columns if column in portshow_aggregated_df.columns]

    # AG mode switches dropped to avoid duplicate  connection information
    mask_switch_native = portshow_aggregated_df['switchMode'] == 'Native'
    portcmd_join_df = portshow_aggregated_df.loc[mask_switch_native, 
                                                    [*fabric_wwnp_columns, *storage_port_columns, *device_port_columns]].copy()
    portcmd_join_df.dropna(subset=['PortName'], inplace=True)

    aggregated_df = aggregated_df.merge(portcmd_join_df, how='left', on=fabric_wwnp_columns)

    # add 'Device_Host_Name_per_fabric_label' and 'Device_Host_Name_total_fabrics' for devices connected to
    # other fabrics through backbone 
    # 'Device_Host_Name_per_fabric_name_and_label' and 'Device_Host_Name_per_fabric_name' should stay empty
    # since device is not connected to the current fabric name
    portcmd_join_df.rename(columns={'Fabric_name': 'zonemember_Fabric_name', 'Fabric_label': 'zonemember_Fabric_label'}, inplace=True)
    aggregated_df = dfop.dataframe_fillna(aggregated_df, portcmd_join_df, filled_lst=['Device_Host_Name_per_fabric_label', 'Device_Host_Name_total_fabrics'], 
                                    join_lst=['zonemember_Fabric_name', 'zonemember_Fabric_label', 'PortName'])
    # clean 'Device_Host_Name_per_fabric_label' if Fabric_label and 'zonemember_Fabric_label' don't match
    # due to information in device_port_columns is shown relative to fabric name and fabric label
    mask_fabric_label_mismatch = aggregated_df['Fabric_label'] != aggregated_df['zonemember_Fabric_label']
    mask_notna = aggregated_df['Device_Host_Name_per_fabric_label'].notna()
    aggregated_df.loc[mask_notna & mask_fabric_label_mismatch, 'Device_Host_Name_per_fabric_label'] = np.nan
    

    # verify if device is present in other faric_labels of the same fabric_name
    aggregated_df[device_port_columns] = aggregated_df[device_port_columns].apply(pd.to_numeric)

    mask_multiple_fabric_label_connection = \
        aggregated_df['Device_Host_Name_per_fabric_name'] > aggregated_df['Device_Host_Name_per_fabric_name_and_label']
    mask_notna = aggregated_df[['Device_Host_Name_per_fabric_name', 'Device_Host_Name_per_fabric_name_and_label']].notna().all(axis=1)
    aggregated_df['Multiple_fabric_label_connection'] = np.where(mask_multiple_fabric_label_connection & mask_notna, 'Yes', 'No')
    # remove information from lines without port number information
    aggregated_df['Multiple_fabric_label_connection'] = aggregated_df['Multiple_fabric_label_connection'].where(mask_notna, np.nan)
    return aggregated_df