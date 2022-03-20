"""Module to combine set of zoning DataFrames into aggregated zoning configuration DataFrame"""


import numpy as np
import pandas as pd

from .zoning_aggregation_aux_fn import (
    alias_cfg_type, replace_wwnn, replace_domain_index, sort_dataframe,
    verify_alias_duplicate, verify_cfg_type, verify_zonemember_type, wwn_type,
    wwnp_instance_number_per_group, zone_using_alias, zonemember_connection,
    zonemember_in_cfg_fabric_verify, verify_device_hostname_instances, verify_enforcement_type)

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop

# from common_operations_dataframe import dataframe_fillna, dataframe_join, count_group_members


def zoning_aggregated(switch_params_aggregated_df, portshow_aggregated_df, 
                        cfg_df, zone_df, alias_df, cfg_effective_df, 
                        fcrfabric_df, lsan_df, peerzone_df):
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
    # replace Domain_Index with wwnp
    zoning_aggregated_df = replace_domain_index(zoning_aggregated_df, portshow_aggregated_df)
    alias_aggregated_df = replace_domain_index(alias_aggregated_df, portshow_aggregated_df)
    # finds fabric connection for each zonemember (alias)
    zoning_aggregated_df, alias_aggregated_df = \
        zonemember_connection(zoning_aggregated_df, alias_aggregated_df, portshow_aggregated_df)
    # zone enforcement type
    zoning_aggregated_df = verify_enforcement_type(zoning_aggregated_df, portshow_aggregated_df)
    alias_aggregated_df = verify_enforcement_type(alias_aggregated_df, portshow_aggregated_df)
    # checks device status (imported, configured and etc) for LSAN zones
    zoning_aggregated_df, alias_aggregated_df = \
        lsan_state_verify(zoning_aggregated_df,alias_aggregated_df, switch_params_aggregated_df, fcrfabric_df, lsan_df)
    # checks if zoned device available in Fabric where configuration defined
    zoning_aggregated_df = zonemember_in_cfg_fabric_verify(zoning_aggregated_df)
    alias_aggregated_df = zonemember_in_cfg_fabric_verify(alias_aggregated_df)
    # checks in which type of configuration alias apllied in (effective or defined)
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
    # check if alias has duplicates
    alias_aggregated_df = verify_alias_duplicate(alias_aggregated_df)
    # find zones alias used in
    alias_aggregated_df = zone_using_alias(zoning_aggregated_df, alias_aggregated_df)
    # count how many times wwnp meets in zone, alias
    zoning_aggregated_df = wwnp_instance_number_per_group(zoning_aggregated_df, 'zone')
    alias_aggregated_df = wwnp_instance_number_per_group(alias_aggregated_df, 'alias')
    # count Device_Host_Name instances for fabric_label, label and total in fabric
    zoning_aggregated_df = verify_device_hostname_instances(zoning_aggregated_df, portshow_aggregated_df)
    alias_aggregated_df = verify_device_hostname_instances(alias_aggregated_df, portshow_aggregated_df)
    # verify if zonemember is alias, wwn or DI format
    zoning_aggregated_df = verify_zonemember_type(zoning_aggregated_df, column = 'zone_member')
    zoning_aggregated_df = verify_zonemember_type(zoning_aggregated_df, column = 'alias_member')
    # count active alias_members vs defined alias members
    alias_count_columns = {'alias_member': 'ports_per_alias', 'PortName': 'active_ports_per_alias'}
    alias_group_columns = ['Fabric_name', 'Fabric_label', 'zone_member']
    alias_aggregated_df = dfop.count_group_members(alias_aggregated_df, alias_group_columns, alias_count_columns)
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
    # zoning_aggregated_df = cfg_join_df.merge(zone_join_df, how='left', on= ['Fabric_name', 'Fabric_label', 'zone']) # merge order changed
    zoning_aggregated_df = zone_join_df.merge(cfg_join_df, how='left', on= ['Fabric_name', 'Fabric_label', 'zone'])
    # add WWN for each alias in zone configuration configs-zones-aliases
    alias_join_df.rename(columns = {'alias': 'zone_member'}, inplace = True)
    zoning_aggregated_df = zoning_aggregated_df.merge(alias_join_df, how='left', on = ['Fabric_name', 'Fabric_label', 'zone_member'])
    # fill empty cfg and cfg_type
    zoning_aggregated_df['cfg'].fillna('-', inplace=True)
    zoning_aggregated_df['cfg_type'].fillna('defined', inplace=True)
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
        mask_valid_fabric = ~df['Fabric_name'].isin(['x', '-'])
        df = df.loc[mask_valid_fabric].copy()
        # drop columns containing switch information if df dedicated for the whole Fabric
        if drop_columns:
            df.drop(columns = [*switchparams_lst[:5], 'switch_index', 'Fabric_ID'], inplace=True)
        df_lst.append(df)
        
    return df_lst


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
    fcrfabric_join_df = dfop.dataframe_join(fcrfabric_join_df, switch_params_aggregated_df, fcr_columns_lst, 1)

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
    
    # add 'zone_lsan' tag for statistics
    mask_lsan = zoning_aggregated_df['LSAN_device_state'].isna()
    zoning_aggregated_df['lsan_tag'] = zoning_aggregated_df['LSAN_device_state'].where(mask_lsan, 'lsan_tag')

    # create DataFrame containing original combination of LSAN zonemembers(aliases), 
    # it's WWN with fabric labels and device status
    mask_lsan = pd.notna(zoning_aggregated_df['LSAN_device_state'])
    lsan_alias_lst = ['Fabric_name', 'Fabric_label', 'zone_member', 'alias_member', 'LSAN_device_state']
    lsan_alias_df = zoning_aggregated_df.loc[mask_lsan, lsan_alias_lst].copy()
    lsan_alias_df.drop_duplicates(inplace=True)
    # add LSAN zones device status to alias_aggregated_df DataFrame
    alias_aggregated_df = alias_aggregated_df.merge(lsan_alias_df, how='left', on=lsan_alias_lst[:-1]) 

    return zoning_aggregated_df, alias_aggregated_df
