"""Module to combine set of zoning DataFrames into aggregated zoning configuration DataFrame"""

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_fillna, dataframe_join


def zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                        cfg_df, zone_df, alias_df, cfg_effective_df, 
                        fcrfabric_df, lsan_df, peerzone_df, report_data_lst):
    """
    Main aggregation function. 
    Combines set of zoning DataFrames into aggregated zoning configuration DataFrame.
    """

    # create fabric labaled zoning configuration DataFrame
    zoning_aggregated_df, alias_aggregated_df = \
        zoning_from_configuration(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df, peerzone_df)
    # verify which type of WWN (port or node WWN) is used for each member
    zoning_aggregated_df, alias_aggregated_df = wwn_type(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df)
    # replace each wwnn in zoning configuration with it's wwnp  
    zoning_aggregated_df, alias_aggregated_df = replace_wwnn(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df)
    # finds fabric connection for each zonemember (alias)
    zoning_aggregated_df, alias_aggregated_df = \
        zonemember_connection(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df)
    # checks device status (imported, configured and etc) for LSAN zones
    zoning_aggregated_df, alias_aggregated_df = \
        lsan_state_verify(zoning_aggregated_df,alias_aggregated_df, switch_params_aggregated_df, fcrfabric_df, lsan_df)
    # checks if zoned device available in Fabric where configuration defined
    zoning_aggregated_df = zonemember_in_cfg_fabric_verify(zoning_aggregated_df)
    alias_aggregated_df = zonemember_in_cfg_fabric_verify(alias_aggregated_df)
    # checks in which type of configuration alias apllied in (effective or defined)
    # alias_aggregated_df = alias_cfg_type(alias_aggregated_df, zoning_aggregated_df)
    alias_aggregated_df = verify_cfg_type(alias_aggregated_df, zoning_aggregated_df, ['zone_member', 'alias_member'])
    # sort zoning configuration based on config type, fabric labels and devices zoned
    zoning_aggregated_df, alias_aggregated_df = sort_dataframe(zoning_aggregated_df, alias_aggregated_df)
    # create zone_duplicates_free column with no duplicated zonenames
    zoning_aggregated_df['zone_duplicates_free'] = np.nan
    mask_zone_duplicate = zoning_aggregated_df.duplicated(subset=['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone'], keep='first')
    zoning_aggregated_df['zone_duplicates_free'] = zoning_aggregated_df['zone_duplicates_free'].where(mask_zone_duplicate, zoning_aggregated_df.zone)
    # create alias_duplicates_free column with no duplicated aliasnames
    alias_aggregated_df['zonemember_duplicates_free'] = np.nan
    mask_zonemember_duplicate = alias_aggregated_df.duplicated(subset=['Fabric_name', 'Fabric_label', 'zone_member'], keep='first')
    alias_aggregated_df['zonemember_duplicates_free'] = alias_aggregated_df['zonemember_duplicates_free'].where(mask_zonemember_duplicate, alias_aggregated_df.zone_member)

    alias_aggregated_df = verify_alias_duplicate(alias_aggregated_df)
    alias_aggregated_df = zone_using_alias(zoning_aggregated_df, alias_aggregated_df)

    zoning_aggregated_df = wwnp_instance_number_per_group(zoning_aggregated_df, 'zone')
    alias_aggregated_df = wwnp_instance_number_per_group(alias_aggregated_df, 'alias')

    return zoning_aggregated_df, alias_aggregated_df


def zoning_from_configuration(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df, peerzone_df):
    """Function to create fabric labaled zoning configuration DataFrame"""

    # fabric label, columns rename and drop for  separate zoning DataFrames
    # pylint: disable=unbalanced-tuple-unpacking
    cfg_join_df, cfg_effective_join_df, zone_join_df, alias_join_df, peerzone_join_df = \
        align_dataframe(switch_params_aggregated_df, cfg_df, cfg_effective_df, zone_df, alias_df, peerzone_df)
    # define type of zoning config (effective or defined)
    cfg_join_df = cfg_join_df.merge(cfg_effective_join_df, how='left', on=['Fabric_name', 'Fabric_label'])
    cfg_join_df['cfg_type'] = np.where(cfg_join_df.cfg == cfg_join_df.effective_config, 'effective', 'defined')
    # change columns order
    cfg_join_df = cfg_join_df.reindex(columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone', 'zone_duplicates_free'])
    # add zone members (aliases or WWN) for each zone in configs-zones DataFrame
    zoning_aggregated_df = cfg_join_df.merge(zone_join_df, how='left', on= ['Fabric_name', 'Fabric_label', 'zone'])
    # add WWN for each alias in zone configuration configs-zones-aliases
    alias_join_df.rename(columns = {'alias': 'zone_member'}, inplace = True)
    zoning_aggregated_df = zoning_aggregated_df.merge(alias_join_df, how='left', on = ['Fabric_name', 'Fabric_label', 'zone_member'])
    # if zonemember defined directly through wwn omitting alias then copy wwn to alias_member column
    zoning_aggregated_df.alias_member.fillna(zoning_aggregated_df.zone_member, inplace=True)
    # add peerzones zonemember type (Property, Principal, Peer)
    peerzone_join_df['peerzone_member_type'] = peerzone_join_df['peerzone_member_type'].str.lower()
    zoning_aggregated_df = zoning_aggregated_df.merge(peerzone_join_df, how='left', on = ['Fabric_name', 'Fabric_label', 'zone', 'zone_member'])
    # alias aggregated DataFrame
    alias_aggregated_df = alias_join_df.copy()
    alias_aggregated_df = alias_aggregated_df.reindex(columns= ['Fabric_name', 'Fabric_label', 'zone_member', 'alias_member'])

    return zoning_aggregated_df, alias_aggregated_df


def align_dataframe(switch_params_aggregated_df, *args, drop_columns=True):
    """
    Function to label DataFrame with Fabric names and labels.
    Rename columns to correspond DataFrame to join with.
    Drop if required unneccessary columns.
    """

    # add Fabric labels from switch_params_aggregated_df Fataframe
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switchName', 'switchWwn', 
                        'Fabric_name', 'Fabric_label'
                        ]
    # create left DataFrame for join operation
    switchparams_aggregated_join_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    
    df_lst = []
    column_dct = {'principal_switch_index': 'switch_index',	
                'principal_switchName': 'switchName',	
                'principal_switchWwn': 'switchWwn'}

    for arg in args:
        df = arg.copy()
        # rename columns
        df.rename(columns =  column_dct, inplace = True)
        # label df with Fabric names and labels
        df = df.merge(switchparams_aggregated_join_df, how = 'left', on = switchparams_lst[:5])
        # drop data for Fabrics which are out of assessment scope
        df.dropna(subset = ['Fabric_name', 'Fabric_label'], inplace = True)
        # drop columns containing switch information if df dedicated for the whole Fabric
        if drop_columns:
            df.drop(columns = [*switchparams_lst[:5], 'switch_index', 'Fabric_ID'], inplace=True)
        df_lst.append(df)
        
    return df_lst


def wwn_type(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df):
    """
    Function to verify which type of WWN (port or node WWN) is used for each alias_member
    """

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
    """Function to replace each wwnn in zoning configuration with it's wwnp if any wwnn is present"""

    # create DataFrame with WWNP and WWNN
    port_node_name_df = portshow_aggregated_df[['Fabric_name', 'Fabric_label', 'PortName', 'NodeName']].copy()
    port_node_name_df.dropna(subset = ['Fabric_name', 'Fabric_label', 'PortName', 'NodeName'], inplace=True)
    # check if

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


def zonemember_connection(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df):
    """Function to find each zonememeber and aliasmember fabric connection if it's exist"""

    port_columns_lst = ['Fabric_name', 'Fabric_label', 
                    'Device_Host_Name', 'Group_Name', 'Device_Port',
                    'Device_type', 'deviceType', 'deviceSubtype', 'portType',
                    'PortName', 'NodeName', 'Connected_portId',
                    'chassis_name', 'switchName', 'Index_slot_port']

    # connection information revealed by merging based on WWNP number
    # of connected device switch port  (PortName) and 
    # WWNP number of alias_member (Strict_Wwnp)
    # AG mode switches dropped to avoid duplicate  connection information3
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


def lsan_state_verify(zoning_aggregated_df, alias_aggregated_df, switch_params_aggregated_df, fcrfabric_df, lsan_df):
    """Function to check device status (imported, exist, configured and etc) for LSAN zones"""

    
    # rename columns in fcrfabric for join operation
    fcr_columns_dct = {'principal_switch_index': 'switch_index',	
                        'principal_switchName': 'switchName',	
                        'principal_switchWwn': 'switchWwn',
                        'EX_port_switchWwn': 'Connected_switchWwn',
                        'EX_port_FID': 'Connected_Fabric_ID'}
    fcrfabric_join_df = fcrfabric_df.copy()
    fcrfabric_join_df.rename(columns=fcr_columns_dct, inplace=True)

    # add fabric labels to routers of BackBone fabric based on principal router switchWwn
    # and fabric labels to all switches of fabrics connected to BB roters through EX-Port  
    fcr_columns_lst = ['switchWwn', 'Fabric_name', 'Fabric_label']
    fcrfabric_join_df = dataframe_join(fcrfabric_join_df, switch_params_aggregated_df, fcr_columns_lst, 1)

    # extract required columns and drop duplicates since EX-port column is dropped
    # thus rows with duplicated switches appeared
    fcrfabric_lst=['configname', 'chassis_name', 'chassis_wwn',
                    'switchName', 'switchWwn',
                    'BB_Fabric_ID', 'Fabric_name', 'Fabric_label',
                    'Connected_Fabric_ID', 'Connected_Fabric_name', 'Connected_Fabric_label']
    fcrfabric_join_df = fcrfabric_join_df.reindex(columns=fcrfabric_lst)
    fcrfabric_join_df.drop_duplicates(inplace=True)

    # add fabric labels to lsan zones DataFrame
    # pylint: disable=unbalanced-tuple-unpacking
    lsan_join_df, = align_dataframe(switch_params_aggregated_df, lsan_df, drop_columns=False)
    """
    LSAN zones DataFrame contain fabric labels of BackBone Fabric where they are stored.
    Information of real LSAN zone Fabric creation can be obtained from fcrfabric_join_df
    DataFrame by merging based on Rooter information and Connected to it FabricID 
    """
    lsan_join_df = lsan_join_df.merge(fcrfabric_join_df, how='left', left_on=[*fcrfabric_lst[:8], 'Zone_Created_Fabric_ID'],
                                    right_on = [*fcrfabric_lst[:8], 'Connected_Fabric_ID'])

    # drop BackBone principal Router columns information and rename columns
    # to correspond zoning_aggregated_df columns to join
    lsan_columns_lst = ['zone', 'zone_member', 'LSAN_device_state',
                        'Connected_Fabric_name', 'Connected_Fabric_label']
    lsan_columns_dct = {'Connected_Fabric_name': 'Fabric_name', 
                        'Connected_Fabric_label': 'Fabric_label',
                        'zone_member': 'alias_member'}
    lsan_join_df = lsan_join_df.reindex(columns=lsan_columns_lst)
    lsan_join_df.rename(columns=lsan_columns_dct, inplace=True)

    # add lsan zones member status (Imported, Exist, Configured) to zoning_aggregated_df
    # based on fabric where zone is defined, zonename and device wwn
    lsan_join_columns_lst = ['Fabric_name', 'Fabric_label', 'zone', 'alias_member']
    zoning_aggregated_df = zoning_aggregated_df.merge(lsan_join_df, how='left', on=lsan_join_columns_lst)

    # create DataFrame containing original combination of LSAN zonemembers(aliases), 
    # it's WWN with fabric labels and device status
    mask_lsan = pd.notna(zoning_aggregated_df['LSAN_device_state'])
    lsan_alias_lst = ['Fabric_name', 'Fabric_label', 'zone_member', 'alias_member', 'LSAN_device_state']
    lsan_alias_df = zoning_aggregated_df.loc[mask_lsan, lsan_alias_lst].copy()
    lsan_alias_df.drop_duplicates(inplace=True)
    # add LSAN zones device status to alias_aggregated_df DataFrame
    alias_aggregated_df = alias_aggregated_df.merge(lsan_alias_df, how='left', on=lsan_alias_lst[:-1]) 

    return zoning_aggregated_df, alias_aggregated_df


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
    zoning_aggregated_df['Fabric_device_status'].replace(to_replace={1: 'local', 0: 'remote_na'}, inplace = True)
    # mark devices which are not in the same fabric with principal switch where configiguration defined
    # but which is part of LSAN zone for that fabric and device status is Imported as Yes 
    mask_member_imported = zoning_aggregated_df['LSAN_device_state'].str.contains('Imported', na=False)
    mask_member_configured = zoning_aggregated_df['LSAN_device_state'].str.contains('Configured', na=False)
    mask_member_initializing = zoning_aggregated_df['LSAN_device_state'].str.contains('Initializing', na=False)
    mask_fabric_name = pd.notna(zoning_aggregated_df['zonemember_Fabric_name'])


    zoning_aggregated_df['Member_in_cfg_Fabric'] = \
        np.where((mask_member_imported&mask_fabric_name), 'Да', zoning_aggregated_df['Member_in_cfg_Fabric'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_imported & mask_fabric_name), 'remote_imported', zoning_aggregated_df['Fabric_device_status'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_configured & mask_fabric_name), 'remote_configured', zoning_aggregated_df['Fabric_device_status'])
    zoning_aggregated_df['Fabric_device_status'] = \
        np.where((mask_member_initializing & mask_fabric_name), 'remote_initializing', zoning_aggregated_df['Fabric_device_status'])
    
    # replace 'absent' status for peerzone property member fo np.nan 
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
    alias_aggregated_df = dataframe_fillna(alias_aggregated_df, cfg_effective_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    # fill rest empty values with defined word if alias is in defined config
    alias_aggregated_df = dataframe_fillna(alias_aggregated_df, cfg_defined_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])

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
    aggregated_df = dataframe_fillna(aggregated_df, cfg_effective_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])
    # fill rest empty values with defined word if alias is in defined config
    aggregated_df = dataframe_fillna(aggregated_df, cfg_defined_df, join_lst = cfg_lst[:-1], filled_lst= cfg_lst[-1:])

    return aggregated_df


def sort_dataframe(zoning_aggregated_df, alias_aggregated_df):
    """Function to sort zoning configuration based on config type, fabric labels and devices zoned"""

    sort_zone_lst = ['Fabric_label', 'Fabric_name', 'cfg_type', 'cfg' , 'zone', 'deviceType', 'zone_member']
    # sort_zone_lst = ['cfg_type', 'Fabric_label', 'cfg', 'zone', 'deviceType', 'zone_member', 'Fabric_name']
    sort_alias_lst = ['cfg_type', 'Fabric_label', 'Fabric_name', 'zone_member', 'Wwn_type']

    zoning_aggregated_df.sort_values(by=sort_zone_lst, \
        ascending=[True, True, False, *4*[True]], inplace=True)
    # zoning_aggregated_df.sort_values(by=sort_zone_lst, \
    #     ascending=[False, *6*[True]], inplace=True)
    alias_aggregated_df.sort_values(by=sort_alias_lst, \
        ascending=[False, *4*[True]], inplace=True)

    return zoning_aggregated_df, alias_aggregated_df


def verify_alias_duplicate(alias_aggregated_df):
    """
    Function to check if alias_member (wwnp or wwnn) has duplicated aliases and
    counts its number if they exist
    """

    # perform zone_member (alias names) grouping based on alias_member (set of wwnn and wwnp) for each fabric
    alias_count_columns = ['Fabric_name', 'Fabric_label', 'alias_member']
    # join zone_members in each group
    alias_duplicated_names_df = alias_aggregated_df.groupby(alias_count_columns, as_index = False).agg({'zone_member': ', '.join})
    # rename column with joined zone_members names and perform merge with aggregated DataFrame
    alias_duplicated_names_df.rename(columns={'zone_member': 'alias_duplicated'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_names_df, how='left', on=alias_count_columns)
    # leave duplicated zone_memebers only
    mask_duplicated = alias_aggregated_df['alias_duplicated'] != alias_aggregated_df['zone_member']
    alias_aggregated_df['alias_duplicated'] = alias_aggregated_df['alias_duplicated'].where(mask_duplicated, np.nan)
    # count number of alias(es) for the alias_member (wwnn or wwnp)
    alias_duplicated_number_df = alias_aggregated_df.groupby(alias_count_columns, as_index = False).agg({'zone_member': 'count'})
    alias_duplicated_number_df.rename(columns={'zone_member': 'alias_count'}, inplace=True)
    alias_aggregated_df = alias_aggregated_df.merge(alias_duplicated_number_df, how='left', on=alias_count_columns)

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