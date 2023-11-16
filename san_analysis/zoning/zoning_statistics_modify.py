"""Module to modify zoning_aggregated_df DataFrame to count statistics 
and find pair zones, duplicated and target driven zones"""

import itertools
import re
from difflib import SequenceMatcher

import numpy as np
import pandas as pd
import utilities.dataframe_operations as dfop


def modify_zoning(zoning_aggregated_df):
    """Function to modify zoning_aggregated_df DataFrame to count statistics"""

    statistics_columns_lst = ['deviceType', 'deviceSubtype', 'Device_type', 'Wwn_type', 'peerzone_member_type'] 

    # to count zonemeber stitistics it is required to make
    # changes in zoning_aggregated_df DataFrame
    zoning_modified_df = zoning_aggregated_df.copy()
    # All classes of servers are considered to be SRV class
    zoning_modified_df.deviceType.replace(to_replace={'BLADE_SRV': 'SRV', 'SYNERGY_SRV': 'SRV', 'SRV_BLADE': 'SRV', 'SRV_SYNERGY': 'SRV'}, inplace=True)
    # deviceType transformed to be combination if device class and device type
    zoning_modified_df.deviceSubtype = zoning_modified_df['deviceType'] + ' ' + zoning_modified_df['deviceSubtype']
    # servers device type is not important for zonemember analysis
    mask_srv = zoning_modified_df.deviceType.str.contains('SRV', na=False)
    zoning_modified_df.deviceSubtype = np.where(mask_srv, np.nan, zoning_modified_df.deviceSubtype)
    # tag unique device in each zones by combination of deviceType and Unique tag to count unique devices in zone
    mask_device_name_unique = ~zoning_modified_df.duplicated(subset=['Fabric_name',	'Fabric_label', 
                                                                    'cfg', 'zone', 
                                                                    'deviceType', 'Device_Host_Name'])
    zoning_modified_df.loc[mask_device_name_unique, 'Unique_device_type_name'] = \
        zoning_modified_df.loc[mask_device_name_unique, 'deviceType'] + ' Unique name'
    # tag duplicated PortWwnp in zone 
    mask_wwnp_duplicated = zoning_modified_df['wwnp_instance_number_per_zone'] > 1
    zoning_modified_df['Wwnp_duplicated'] = np.where(mask_wwnp_duplicated, 'Wwnp_duplicated', np.nan)

    """
    We are interested to count connected devices statistics only.
    Connected devices are in the same fabric with the switch which 
    zoning configurutaion defined in (local) or imported to that fabric
    in case of LSAN zones (imported).
    Ports with status remote_na, initializing and configured considered to be
    not connected (np.nan) and thus it's 'deviceType', 'deviceSubtype', 'Device_type', 
    'Wwn_type', 'peerzone_member_type' are not taking into acccount.
    'peerzone_member_type' for Peerzone property member is not changed and counted in statistics. 
    But device status for not connected ports is reflected in zonemember statistics.
    """  
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    mask_peerzone_property = zoning_aggregated_df['peerzone_member_type'].str.contains('property', na=False)
    # axis 1 replace values with nan along the row (leave local, remote_imported or property members, others replace with nan)
    zoning_modified_df[statistics_columns_lst] = \
    zoning_modified_df[statistics_columns_lst].where(mask_connected | mask_peerzone_property, pd.Series((np.nan*len(statistics_columns_lst))), axis=1)
    
    mask_zone_name = zoning_modified_df['zone_duplicates_free'].isna()
    zoning_modified_df['zone_tag'] = zoning_modified_df['zone_duplicates_free'].where(mask_zone_name, 'zone_tag')
    # lsan_tag was added in analysis_zoning_aggregation module
    zoning_modified_df['lsan_tag'] = zoning_modified_df['lsan_tag'].where(~mask_zone_name, np.nan)
    # add tdz_tag
    zoning_modified_df = verify_tdz(zoning_modified_df)
    # add qos zone tag
    if zoning_modified_df['zone_duplicates_free'].notna().any():
        mask_qos = zoning_modified_df['zone_duplicates_free'].str.contains(r'^QOS[LMH]\d?', na=False)
        zoning_modified_df.loc[~mask_zone_name & mask_qos, 'qos_tag'] = 'qos_tag'

    # verify duplicated zones (zones with the same set of PortWwns)
    zoning_duplicated_df = verify_duplicated_zones(zoning_aggregated_df)
    zoning_duplicated_columns = ['Fabric_name', 'Fabric_label',  'cfg',  'cfg_type',  'zone_duplicates_free', 'zone_duplicated_tag']
    # add zone_duplicated_tag for each duplicated zone from zone_duplicates_free column (to count each zone only once further)
    
    print(zoning_duplicated_df)
    # if not zoning_duplicated_df.empty:
    zoning_modified_df = \
        dfop.dataframe_fillna(zoning_modified_df, zoning_duplicated_df, join_lst=zoning_duplicated_columns[:-1], filled_lst=[zoning_duplicated_columns[-1]])

    # verify absorbed zones (zones which are part of other zones in effective configuration excluding duplicated zones)
    zoning_absorbed_df = verify_absorbed_zones(zoning_aggregated_df)
    zoning_absorbed_columns = ['Fabric_name', 'Fabric_label', 'zone_duplicates_free', 'zone_absorbed_tag']
    # add zone_duplicated_tag for each duplicated zone from zone_duplicates_free column (to count each zone only once further)
    if not zoning_absorbed_df.empty:
        zoning_modified_df = \
            dfop.dataframe_fillna(zoning_modified_df, zoning_absorbed_df, join_lst=zoning_absorbed_columns[:-1], filled_lst=[zoning_absorbed_columns[-1]])

    # find zone pairs (zones with the same set device names) in another fabric_labels of the same fabric_name
    zoning_pairs_df = verify_pair_zones(zoning_aggregated_df)
    zoning_pairs_columns = ['Fabric_name', 'Fabric_label',  'cfg_type',  'zone_duplicates_free', 'zone_paired_tag']
    # add zone_paired_tag for each paired zone from zone_duplicates_free column (to count each zone only once further)
    zoning_modified_df = \
        dfop.dataframe_fillna(zoning_modified_df, zoning_pairs_df, join_lst=zoning_pairs_columns[:-1], filled_lst=[zoning_pairs_columns[-1]]) 

    zoning_modified_df.replace(to_replace='nan', value=np.nan, inplace=True)

    return zoning_modified_df, zoning_duplicated_df, zoning_pairs_df, zoning_absorbed_df


def verify_duplicated_zones(zoning_aggregated_df):
    """Function to find duplicated zones (zones with identical set of PortWwns)"""

    columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type']
    zoning_cp_df = zoning_aggregated_df.copy()
    # drop rows with empty PortWwwns (absent zonemembers)
    zoning_cp_df.dropna(subset=['PortName'], inplace=True)
    
    # group PortWwns of each zone to sorted set thus removing duplicates and present it as comma separated list
    grp_columns = columns + ['zone']
    zoning_grp_df = zoning_cp_df.groupby(by=grp_columns)['PortName'].agg(lambda x: ', '.join(sorted(set(x))))
    zoning_grp_df = pd.DataFrame(zoning_grp_df)
    zoning_grp_df.reset_index(inplace=True)

    grp_columns = columns + ['PortName']
    # filter duplicated zones only (zones with equal set of PortWwns for each Fabric)
    zoning_duplicated_df = zoning_grp_df.loc[zoning_grp_df.duplicated(subset=grp_columns, keep=False)].copy()
    zoning_duplicated_df['zone_duplicated_tag'] = 'zone_duplicated_tag'
    # add column with identical zones list for each duplicated zone 
    zoning_duplicated_df['zone_duplicated'] = zoning_duplicated_df.groupby(by=grp_columns)['zone'].transform(', '.join)
    # add column with zone names (for merge purposes further)
    zoning_duplicated_df['zone_duplicates_free'] = zoning_duplicated_df['zone']

    return zoning_duplicated_df


def verify_absorbed_zones(zoning_aggregated_df):
    """Function to find obserbed zones (zones which are part of other zones in effective configuration excluding duplicated zones)"""

    # prepare zoning (slice effective zoning and local or imported ports only)
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    mask_effective = zoning_aggregated_df['cfg_type'] == 'effective'
    
    group_columns = ['Fabric_name', 'Fabric_label', 'zone']

    # zones to be verified (effective and defined) 
    zoning_verified_df = zoning_aggregated_df.loc[mask_connected].copy()
    # count active ports in each zone to take into account zones which are bigger than verified zone
    zoning_verified_df['Portname_quantity'] = zoning_verified_df.groupby(by=group_columns)['PortName'].transform('count')
    # zone configuration in which absorber zones are searched for (effective only)
    zoning_valid_df = zoning_verified_df.loc[mask_effective].copy()
    # find obsorbed and absorber zones
    zoning_absorbed_df  = \
        zoning_verified_df.groupby(by=group_columns).apply(lambda verified_zone_grp: find_zone_absorber(verified_zone_grp, zoning_valid_df))
    
    zoning_absorbed_df = pd.DataFrame(zoning_absorbed_df)
    if not zoning_absorbed_df.empty:
        zoning_absorbed_df.reset_index(inplace=True)
        # rename column with absorber zone names
        zoning_absorbed_df.rename(columns={0: 'zone_absorber'}, inplace=True)
        # drop rows if there is no zone absorber found
        zoning_absorbed_df.dropna(subset=['zone_absorber'], inplace=True)
        zoning_absorbed_df['zone_duplicates_free'] = zoning_absorbed_df['zone']
        zoning_absorbed_df['zone_absorbed_tag'] = 'zone_absorbed_tag'
    return zoning_absorbed_df



def find_zone_absorber(verified_zone_grp, zoning_valid_df):
    """Auxiliary function for verify_absorbed_zones fn 
    to find zones in effective configuration which contain same active portWwns as verified zones.
    Zones with identical set of portWwns (duplicated zones) as verified zone are not taken into account"""

    group_columns = ['Fabric_name', 'Fabric_label', 'zone']

    # identify fabric name, label of the verified zone to filter off zones from other fabrics
    verified_zone_fb,  = verified_zone_grp['Fabric_name'].unique()
    verified_zone_fl,  = verified_zone_grp['Fabric_label'].unique()
    # identify active port quantity to filter off zones of the same size or smaller
    verified_zone_port_quntity,  = verified_zone_grp['Portname_quantity'].unique()
    # slice zoning configuration to reduce process time
    mask_same_fabic = (zoning_valid_df['Fabric_name'] == verified_zone_fb) & \
                        (zoning_valid_df['Fabric_label'] == verified_zone_fl)
    mask_bigger_zone = zoning_valid_df['Portname_quantity'] > verified_zone_port_quntity
    zoning_valid_fabric_df = zoning_valid_df.loc[mask_same_fabic & mask_bigger_zone].copy()
    
    # find zones which include (absorbe) current verified zone
    absorbed_zone_df = \
        zoning_valid_fabric_df.groupby(by=group_columns).filter(lambda valid_zone_grp: verified_zone_grp['PortName'].isin(valid_zone_grp['PortName']).all())
    # represent zones as comma separated string
    if not absorbed_zone_df.empty:
        zone_sr = absorbed_zone_df['zone'].drop_duplicates()
        zones_str = ', '.join(zone_sr.to_list())
        return zones_str 


def verify_tdz(zoning_modified_df):
    """Function to find target driven zones zones"""
    
    if 'peerzone_member_type' in zoning_modified_df.columns and zoning_modified_df['peerzone_member_type'].notna().any():
        # zone need to be efficient and peer type
        # mask_valid_zone = ~zoning_modified_df['Target_Initiator_note'].isin(invalid_zone_tags)
        mask_property = zoning_modified_df['peerzone_member_type'] == 'principal'
        zoning_tdz_df = zoning_modified_df.loc[mask_property].copy()
        zoning_tdz_df.dropna(subset=['PortName'], inplace=True)
        
        # zone name need contain tdz tag and principal member Wwnp (without colons)
        zoning_tdz_df['PortName_colon_free'] = zoning_tdz_df['PortName'].str.replace(r':', '')
        zoning_tdz_df = zoning_tdz_df.loc[zoning_tdz_df.apply(lambda x: 'tdz' in x.zone and x.PortName_colon_free in x.zone, axis=1)].copy()
        
        # zone_duplicates_free and tdz_tag columns used for dataframe_fillna
        zoning_tdz_df['zone_duplicates_free'] = zoning_tdz_df['zone']
        zoning_tdz_df['tdz_tag'] = 'tdz_tag'

        tdz_columns = ['Fabric_name', 'Fabric_label', 'cfg', 'cfg_type', 'zone_duplicates_free', 'tdz_tag']
        zoning_modified_df = dfop.dataframe_fillna(zoning_modified_df, zoning_tdz_df, filled_lst=tdz_columns[-1:], join_lst=tdz_columns[:-1])

    return zoning_modified_df


def verify_pair_zones(zoning_aggregated_df):
    """Function to find pair zone or zones in other fabric_labels of the same fabric_name"""

    columns = ['Fabric_name', 'Fabric_label', 'cfg_type']
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    zoning_cp_df = zoning_aggregated_df.loc[mask_connected].copy()

    # drop duplicated Wwnp in each zone
    zoning_cp_df.drop_duplicates(subset=[*columns, 'zone', 'PortName'], inplace=True)

    # verify if all devices in each zone are connected to other fabric_labels of the same fabric_name
    # (if device have links both to fabric A and B)
    grp_columns = columns + ['zone']
    mask_all_devices_multiple_fabric_label_connection = \
        zoning_cp_df.groupby(by=grp_columns)['Multiple_fabric_label_connection'].transform(lambda series: series.isin(['Yes']).all())
    zoning_cp_df['All_devices_multiple_fabric_label_connection'] = np.where(mask_all_devices_multiple_fabric_label_connection, 'Yes', 'No')

    # group device names of each zone to sorted set thus removing duplicates and present it as comma separated list
    grp_columns.append('All_devices_multiple_fabric_label_connection')
    
    zoning_grp_df = zoning_cp_df.groupby(by=grp_columns)['Device_Host_Name'].agg(lambda x: ', '.join(sorted(set(x))))
    zoning_grp_df = pd.DataFrame(zoning_grp_df)
    zoning_grp_df.reset_index(inplace=True)

    # drop peer zone members to verify if zone name correpond to local or remote_imported Device_Host_Names included in the zone
    # since peer zone usually contains many zonemembers it's name based on principal member Device_Host_Name
    if 'peerzone_member_type' in zoning_cp_df.columns:
        mask_not_peer = zoning_cp_df['peerzone_member_type'] != 'peer'
        zoning_peer_member_free_df = zoning_cp_df.loc[mask_not_peer].copy()
        # group device names of each zone to sorted set thus removing duplicates and present it as comma separated list
        zoning_peer_free_grp_df = zoning_peer_member_free_df.groupby(by=grp_columns)['Device_Host_Name'].agg(lambda x: ', '.join(sorted(set(x))))
        zoning_peer_free_grp_df = pd.DataFrame(zoning_peer_free_grp_df)
        zoning_peer_free_grp_df.reset_index(inplace=True)
    else:
        zoning_peer_free_grp_df = zoning_grp_df.copy()

    fabric_name_lst = zoning_grp_df['Fabric_name'].unique().tolist()
    fabric_label_lst = zoning_grp_df['Fabric_label'].unique().tolist()

    # columns take into account zone members (zone devices) in cfg_type for each zone 
    # cfg_name is not taken into account since it might differ in fabrics
    cfgtype_zone_device_columns = [*columns, 'All_devices_multiple_fabric_label_connection', 'Device_Host_Name']
    # DataFrame contains list of zones with its pairs for each fabric and cfg_type
    zoning_pairs_df = pd.DataFrame()                           
    # list of columns with zone pairs for concatenation below
    zone_paired_columns = set()

    for fabric_name in fabric_name_lst:
        for fabric_label in fabric_label_lst:
            # fabric labels to be verified to find pair zones
            verified_label_lst = fabric_label_lst.copy()
            # fabric_label for which pair zones are verified should be removed from the list
            verified_label_lst.remove(fabric_label)
            # DataFrame with zones for which pair zones are need to be find
            mask_current = (zoning_grp_df[['Fabric_name', 'Fabric_label']] == (fabric_name, fabric_label)).all(axis=1)
            current_df = zoning_grp_df.loc[mask_current].copy()
            if not current_df.empty:
                # check zoning configurations in each verified fabrics to find pair zones
                for verified_label in verified_label_lst:
                    # DataFrame with zones in which pair zones are searched for
                    mask_verified = (zoning_grp_df[['Fabric_name', 'Fabric_label']] == (fabric_name, verified_label)).all(axis=1)
                    verified_df = zoning_grp_df.loc[mask_verified].copy()
                    if not verified_df.empty:
                        # if there are more then one zone with identical list of devices
                        verified_grp_df = verified_df.groupby(by=cfgtype_zone_device_columns)['zone'].agg(lambda x: ', '.join(sorted(set(x))))
                        verified_grp_df = pd.DataFrame(verified_grp_df)
                        verified_grp_df.reset_index(inplace=True)
                        # if there are more then two fabric_labels in fabric_name then use fabric_label tag with the name of pair zone
                        if len(fabric_label_lst) > 2:
                            mask_zone_notna = verified_grp_df['zone'].notna()
                            verified_grp_df.loc[mask_zone_notna, 'zone'] = '(' + verified_label + ': ' + verified_grp_df.loc[mask_zone_notna, 'zone'] + ')'
                        # to merge pair zones change fabric_label in verified fabric to fabric_label of fabric for which pair zones are searched for
                        verified_grp_df['Fabric_label'] = fabric_label
                        # column name with pair zones in verified fabric_label
                        zone_paired_column = 'zone_paired_' + str(verified_label)
                        zone_paired_columns.add(zone_paired_column)
                        verified_grp_df.rename(columns={'zone': zone_paired_column}, inplace=True)
                        # add column with pair zones in verified fabric_label to zoning configuration
                        current_df = dfop.dataframe_fillna(current_df, verified_grp_df, 
                                                    join_lst=cfgtype_zone_device_columns, 
                                                    filled_lst=[zone_paired_column])           
                # add zoning configuration with pair zones in all fabric_labels to general zoning configuration DataFrame
                zoning_pairs_df = pd.concat([zoning_pairs_df, current_df])

    zone_paired_columns = list(zone_paired_columns)
    zoning_pairs_df = dfop.concatenate_columns(zoning_pairs_df, summary_column='zone_paired', 
                                            merge_columns=zone_paired_columns, sep=', ', drop_merge_columns=True)
    # add zone_paired_tag
    mask_zone_notna = zoning_pairs_df['zone_paired'].notna()
    zoning_pairs_df.loc[mask_zone_notna, 'zone_paired_tag'] = 'zone_paired_tag'
    zoning_pairs_df['zone_duplicates_free'] = zoning_pairs_df['zone']
    # verify if zonename related with pair zone name and device names included in each zone
    zoning_pairs_df = verify_zonename_ratio(zoning_pairs_df, zoning_peer_free_grp_df)
    return zoning_pairs_df


def verify_zonename_ratio(zoning_pairs_df, zoning_peer_free_grp_df):
    """Function to verify if zonename related with pair zone name and device names included in each zone"""

    # verify if zone name and it's pair zone name related
    zoning_pairs_df['Zone_and_Pairzone_names_ratio'] = zoning_pairs_df.apply(lambda series: calculate_zone_names_ratio(series), axis=1)    
    zoning_pairs_df = dfop.threshold_exceed(zoning_pairs_df, 'Zone_and_Pairzone_names_ratio', 0.8, 'Zone_and_Pairzone_names_related')
    # verify if zone name related with device names included in this zone
    peer_free_columns = zoning_peer_free_grp_df.columns.tolist()
    peer_free_columns.remove('Device_Host_Name')

    zoning_peer_free_grp_df['Zone_name_device_names_ratio'] = zoning_peer_free_grp_df.apply(lambda series: calculate_zonename_devicenames_ratio(series), axis=1)
    zoning_peer_free_grp_df.drop(columns='Device_Host_Name', inplace=True)
    # add zone name and active device names included into zone ratio to zoning_pairs_df
    zoning_pairs_df = zoning_pairs_df.merge(zoning_peer_free_grp_df, how='left', on=peer_free_columns)
    zoning_pairs_df = dfop.threshold_exceed(zoning_pairs_df, 'Zone_name_device_names_ratio', 0.7, 'Zone_name_device_names_related')
    return zoning_pairs_df


def calculate_zone_names_ratio(series):
    """Function to verify if zone name and it's pair zone name related.
    If zone have more than one pair zones then maximum ratio returned"""

    if pd.notna(series[['zone', 'zone_paired']]).all():
    
        zone_name = series['zone'].lower()
        pair_zone_names = series['zone_paired'].lower().split(', ')

        ratio_lst = [round(SequenceMatcher(None, zone_name, pair_zone_name).ratio(), 2) for pair_zone_name in pair_zone_names]
        return max(ratio_lst)


def calculate_zonename_devicenames_ratio(series):
    """Function to count ratio in which zone name related with device names included in this zone.
    By applying device names permutations maximum ratio value is calaculated"""
    
    # use lower case for consistency
    zone_name = series['zone'].lower().replace('-', '_').replace('__', '_')
    # remove lsan, qos tags from zonename to  increase ratio
    lsan_qos_tag_remover_pattern = '(?:lsan|qos[hml]\d?)_(.+)'
    if re.match(lsan_qos_tag_remover_pattern, zone_name):
        zone_name = re.search(lsan_qos_tag_remover_pattern, zone_name).group(1)

    device_names = series['Device_Host_Name'].lower().replace('-', '_').split(', ')
    # drop domain name (symbols after dot) for each device name
    device_names = [name.split('.')[0] for name in device_names]
    # remove duplicated device names
    device_names = list(dict.fromkeys(device_names))

    # list containing all ration values
    ratio_lst = []
    
    if len(device_names) < 6:
        for permuation in itertools.permutations(device_names):
            # join device names for current names permutation to string
            device_names_str = '_'.join(permuation)
            ratio = round(SequenceMatcher(None, zone_name, device_names_str).ratio(), 2)
            ratio_lst.append(ratio)
    else:
        device_names_str = '_'.join(device_names)
        ratio = round(SequenceMatcher(None, zone_name, device_names_str).ratio(), 2)
        ratio_lst.append(ratio)             
    return max(ratio_lst)



