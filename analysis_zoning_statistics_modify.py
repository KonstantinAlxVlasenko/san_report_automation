"""Module to modify zoning_aggregated_df DataFrame to count statistics 
and find pair zones, duplicated and target driven zones"""

import itertools
from difflib import SequenceMatcher

import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_fillna, сoncatenate_columns


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
    # servers device type is not important for zonemember analysys
    mask_srv = zoning_modified_df.deviceType.str.contains('SRV', na=False)
    zoning_modified_df.deviceSubtype = np.where(mask_srv, np.nan, zoning_modified_df.deviceSubtype)
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
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'imported'])
    mask_peerzone_property = zoning_aggregated_df['peerzone_member_type'].str.contains('property', na=False)
    # axis 1 replace values with nan along the row
    zoning_modified_df[statistics_columns_lst] = \
    zoning_modified_df[statistics_columns_lst].where(mask_connected | mask_peerzone_property, pd.Series((np.nan*len(statistics_columns_lst))), axis=1)
    
    # TO_REMOVE due different series len and columns number
    # zoning_modified_df[statistics_columns_lst] = \
    #     zoning_modified_df[statistics_columns_lst].where(mask_connected | mask_peerzone_property, pd.Series((np.nan, np.nan)), axis=1)

    mask_zone_name = zoning_modified_df['zone_duplicates_free'].isna()
    zoning_modified_df['zone_tag'] = zoning_modified_df['zone_duplicates_free'].where(mask_zone_name, 'zone_tag')
    # lsan_tag was added in analysis_zoning_aggregation module
    zoning_modified_df['lsan_tag'] = zoning_modified_df['lsan_tag'].where(~mask_zone_name, np.nan)
    # add tdz_tag
    zoning_modified_df = verify_tdz(zoning_modified_df)
    # add qos zone tag
    mask_qos = zoning_modified_df['zone_duplicates_free'].str.contains(r'^QOS[LMH]\d')
    zoning_modified_df.loc[~mask_zone_name & mask_qos, 'qos_tag'] = 'qos_tag'

    # verify duplicated zones (zones with the same set of PortWwns)
    zoning_duplicated_df = verify_duplicated_zones(zoning_aggregated_df)
    zoning_duplicated_columns = ['Fabric_name', 'Fabric_label',  'cfg',  'cfg_type',  'zone_duplicates_free', 'zone_duplicated_tag']
    # add zone_duplicated_tag for each duplicated zone from zone_duplicates_free column (to count each zone only once further)
    zoning_modified_df = \
        dataframe_fillna(zoning_modified_df, zoning_duplicated_df, join_lst=zoning_duplicated_columns[:-1], filled_lst=[zoning_duplicated_columns[-1]])

    # find zone pairs (zones with the same set device names) in another fabric_labels of the same fabric_name
    zoning_pairs_df = verify_pair_zones(zoning_aggregated_df)
    zoning_pairs_columns = ['Fabric_name', 'Fabric_label',  'cfg_type',  'zone_duplicates_free', 'zone_paired_tag']
    # add zone_paired_tag for each paired zone from zone_duplicates_free column (to count each zone only once further)
    zoning_modified_df = \
        dataframe_fillna(zoning_modified_df, zoning_pairs_df, join_lst=zoning_pairs_columns[:-1], filled_lst=[zoning_pairs_columns[-1]]) 

    zoning_modified_df.replace(to_replace='nan', value=np.nan, inplace=True)

    return zoning_modified_df, zoning_duplicated_df, zoning_pairs_df


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
        zoning_modified_df = dataframe_fillna(zoning_modified_df, zoning_tdz_df, filled_lst=tdz_columns[-1:], join_lst=tdz_columns[:-1])

    return zoning_modified_df


def verify_pair_zones(zoning_aggregated_df):
    """Function to find pair zone or zones in other fabric_labels of the same fabric_name"""

    columns = ['Fabric_name', 'Fabric_label', 'cfg_type']
    mask_connected = zoning_aggregated_df['Fabric_device_status'].isin(['local', 'remote_imported'])
    zoning_cp_df = zoning_aggregated_df.loc[mask_connected].copy()
    # TO_REMOVE
    # # drop rows with empty PortWwwns (absent zonemembers)
    # zoning_cp_df.dropna(subset=['PortName'], inplace=True)

    # drop duplicated Wwnp in each zone
    zoning_cp_df.drop_duplicates(subset=[*columns, 'zone', 'PortName'], inplace=True)

    # verify if all devices in each zone are connected to other faric_labels of the same fabric_name
    grp_columns = columns + ['zone']
    mask_all_devices_multiple_fabric_label_connection = \
        zoning_cp_df.groupby(by=grp_columns)['Multiple_fabric_label_connection'].transform(lambda series: series.isin(['Yes']).all())
    zoning_cp_df['All_devices_multiple_fabric_label_connection'] = np.where(mask_all_devices_multiple_fabric_label_connection, 'Yes', 'No')

    # group device names of each zone to sorted set thus removing duplicates and present it as comma separated list
    grp_columns.append('All_devices_multiple_fabric_label_connection')
    zoning_grp_df = zoning_cp_df.groupby(by=grp_columns)['Device_Host_Name'].agg(lambda x: ', '.join(sorted(set(x))))
    zoning_grp_df = pd.DataFrame(zoning_grp_df)
    zoning_grp_df.reset_index(inplace=True)

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
                            mask_zone_notna = zoning_pairs_df['zone'].notna()
                            verified_grp_df.loc[mask_zone_notna, 'zone'] = '(' + verified_label + ': ' + zoning_pairs_df.loc[mask_zone_notna, 'zone'] + ')'
                        # to merge pair zones change fabric_label in verified fabric to fabric_label of fabric for which pair zones are searched for
                        verified_grp_df['Fabric_label'] = fabric_label
                        # column name with pair zones in verified fabric_label
                        zone_paired_column = 'zone_paired_' + str(verified_label)
                        zone_paired_columns.add(zone_paired_column)
                        verified_grp_df.rename(columns={'zone': zone_paired_column}, inplace=True)
                        # add column with pair zones in verified fabric_label to zoning configuration
                        current_df = dataframe_fillna(current_df, verified_grp_df, 
                                                    join_lst=cfgtype_zone_device_columns, 
                                                    filled_lst=[zone_paired_column])           
                # add zoning configuration with pair zones in all fabric_labels to general zoning configuration DataFrame
                zoning_pairs_df = pd.concat([zoning_pairs_df, current_df])

    zone_paired_columns = list(zone_paired_columns)
    zoning_pairs_df = сoncatenate_columns(zoning_pairs_df, summary_column='zone_paired', 
                                            merge_columns=zone_paired_columns, sep=', ', drop_merge_columns=True)
    # add zone_paired_tag
    mask_zone_notna = zoning_pairs_df['zone_paired'].notna()
    zoning_pairs_df.loc[mask_zone_notna, 'zone_paired_tag'] = 'zone_paired_tag'
    zoning_pairs_df['zone_duplicates_free'] = zoning_pairs_df['zone']
    # verify if zonename related with pair zone name and device names included in each zone
    zoning_pairs_df = verify_zonename_ratio(zoning_pairs_df)
    return zoning_pairs_df


def verify_zonename_ratio(zoning_pairs_df):
    """Function to verify if zonename related with pair zone name and device names included in each zone"""

    # verify if zone name and it's pair zone name related
    mask_notna = zoning_pairs_df[['zone', 'zone_paired']].notna().all(axis=1)
    zoning_pairs_df.loc[mask_notna, 'Zone_and_Pairzone_names_ratio'] = \
        zoning_pairs_df.loc[mask_notna, ['zone', 'zone_paired']].apply(lambda x: round(SequenceMatcher(None, x[0].upper(), x[1].upper()).ratio(), 2), axis=1)
    zoning_pairs_df = threshold_exceed(zoning_pairs_df, 'Zone_and_Pairzone_names_ratio', 0.9, 'Zone_and_Pairzone_names_related')
    # verify if zone name related with device names included in this zone
    zoning_pairs_df['Zone_name_device_names_ratio'] = zoning_pairs_df.apply(lambda series: calculate_zonename_devicenames_ratio(series), axis=1)
    zoning_pairs_df = threshold_exceed(zoning_pairs_df, 'Zone_name_device_names_ratio', 0.65, 'Zone_name_device_names_related')
    return zoning_pairs_df


# def calculate_zone_names_ratio(zoning_pairs_df):
#     """Function to verify if zone name and it's pair zone name related"""

#     mask_notna = zoning_pairs_df[['zone', 'zone_paired']].notna().all(axis=1)
#     zoning_pairs_df.loc[mask_notna, 'Zone_and_Pairzone_names_ratio'] = \
#         zoning_pairs_df.loc[mask_notna, ['zone', 'zone_paired']].apply(lambda x: round(SequenceMatcher(None, x[0].upper(), x[1].upper()).ratio(), 2), axis=1)
    
#     # mask_ratio = zoning_pairs_df['Zone_and_Pairzone_names_ratio'] >= 0.9
#     # zoning_pairs_df['Zone_and_Pairzone_names_related'] = \
#     #     np.select([mask_notna & mask_ratio, mask_notna & ~mask_ratio], ['Yes', 'No'], default=pd.NA)
#     # zoning_pairs_df.fillna(np.nan, inplace=True)

#     return zoning_pairs_df


def calculate_zonename_devicenames_ratio(series):
    """Function to count ratio in which zone name related with device names included in this zone.
    By applying device names permutations maximum ratio value is calaculated"""
    
    # use upper case for consistency
    zone_name = series['zone'].upper()
    device_names = series['Device_Host_Name'].upper().split(', ')
    # device names permutations
    device_names_permutations = itertools.permutations(device_names)
    # list containing all ration values
    ratio_lst = []
    
    for permuation in device_names_permutations:
        # join device names for current names permutation to string
        device_names_str = ''.join(permuation)
        ratio = round(SequenceMatcher(None, zone_name, device_names_str).ratio(), 2)
        ratio_lst.append(ratio)

    return max(ratio_lst)


def threshold_exceed(df, value_column: str, threshold: float, result_column: str):
    """Function to check if value in value_column exceeds threshold.
    'Yes' or 'No' in result_column"""
    
    mask_value_notna = df[value_column].notna()
    mask_threshold_exceeded = df[value_column] >= threshold
    df[result_column] = np.select([mask_value_notna & mask_threshold_exceeded, 
                                   mask_value_notna & ~mask_threshold_exceeded], 
                                  ['Yes', 'No'], default=pd.NA)
    df.fillna(np.nan, inplace=True)
    return df
