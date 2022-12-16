"""
Module to define device name based on aliases of device ports,
label aliasses and check if aliases defined using wwnn instead of wwnp.
Auxiliary to analysis_portcmd module. 
"""


import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def group_name_fillna(portshow_aggregated_df):

    storage_grp_df, library_grp_df = alias_wwnn_group(portshow_aggregated_df)
    library_sn_grp_df = alias_serial_group(portshow_aggregated_df)

    portshow_aggregated_df['Group_Name'] = None

    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, storage_grp_df, ['NodeName'], ['Group_Name'])
    portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.Group_Name, inplace= True)
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, library_sn_grp_df, ['Device_SN'], ['Group_Name'])
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, library_grp_df, ['NodeName'], ['Group_Name'])

    return portshow_aggregated_df


def alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df, portshow_aggregated_df):
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
    alias_prep_df =  alias_df.rename(columns = {'principal_switchName': 'switchName', 'principal_switchWwn': 'switchWwn'})
    alias_labeled_df = alias_prep_df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])

    # replacing WWNn with WWNp if any
    # create alias_join DataFrame
    alias_lst = ['Fabric_name', 'Fabric_label', 'alias', 'alias_member']
    alias_join_df = alias_labeled_df.loc[:, alias_lst].copy()
    # drop aliases from fabrics that is not part of the assessment
    alias_join_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace=True)
    # merging alias_join and nsshow_join DataFrame
    # if alias_member column contains WWNn then new column contains corresponding WWNp for that WWNn
    alias_wwnn_wwnp_df = alias_join_df.merge(nsshow_join_df, how = 'left', left_on = ['Fabric_name', 'Fabric_label', 'alias_member'], \
                                                                            right_on = ['Fabric_name', 'Fabric_label', 'NodeName'])
    # fill empty cells in WWNn -> WWNp column with alias_member WWNp values thus filtering off all WWNn values
    alias_wwnp_df = alias_wwnn_wwnp_df.copy()
    # alias_wwnp_df.PortName.fillna(alias_join_df['alias_member'], inplace = True)
    alias_wwnp_df.PortName.fillna(alias_wwnp_df['alias_member'], inplace = True)
    
    # replace domain, index with WWpn
    alias_wwnp_df['PortName'].replace(to_replace='\d+,\d+', value=np.nan, regex=True, inplace=True)
    portshow_cp = portshow_aggregated_df.copy()
    portshow_cp.rename(columns={'Connected_portWwn': 'PortName', 'Domain_Index': 'alias_member'}, inplace=True)
    alias_wwnp_df = dfop.dataframe_fillna(alias_wwnp_df, portshow_cp, join_lst=['Fabric_name', 'Fabric_label', 'alias_member'], 
                                            filled_lst=['PortName'], remove_duplicates=False)

    # drop possibly mixed WWNp and WWNn column alias_memeber and pure WWNn column
    alias_wwnp_df.drop(columns = ['alias_member', 'NodeName'], inplace = True)
    # if severeal aliases for one wwnp then combine all into one alias or
    # if in the same alias wwnn and wwnp from single device present thenn unique aliases arr joined (set usage) 
    alias_wwnp_df = alias_wwnp_df.groupby(['Fabric_name', 'Fabric_label', 'PortName'], as_index = False).agg(lambda x: ', '.join(sorted(set(x))))

    return alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df


def alias_wwnn_group(portshow_aggregated_df):
    """
    Function to group storage and libraries based on wwnn.
    And find alias group name.
    """
    
    # it is required to find group names for storages and libraries only 
    portshow_alias_grp_df = portshow_aggregated_df.loc[portshow_aggregated_df.deviceType.isin(['STORAGE', 'LIB'])].copy()
    # aliases are required to define device group name
    portshow_alias_grp_df.dropna(subset = ['alias'], inplace = True)
    # change alias case
    if portshow_alias_grp_df['alias'].notna().any():
        portshow_alias_grp_df.alias = portshow_alias_grp_df.alias.str.upper()

    # columns required to find name for group of wwns based on alias 
    alias_grp_lst = ['NodeName', 'alias', 'deviceType']

    # alias grouping is performed based on wwnn in two ways
    # first option is based on whole wwnn number (for 3par, msa, eva devices)
    wwnn_complete_df = portshow_alias_grp_df.loc[:, alias_grp_lst].copy()
    # for the whole wwnn case we are interested in duplicated wwnns only to have parameter to group on
    # filter rows from portshow DataFrame with repeated wwnn only
    wwnn_complete_df = wwnn_complete_df[wwnn_complete_df.duplicated(subset=['NodeName'], keep=False)]
    # perform aliases grouping based on complete wwnn number
    wwnn_complete_grp_df = wwnn_complete_df.groupby(['NodeName'], as_index = False).agg({'alias': ', '.join, 'deviceType': 'first'})
    # define group name for each wwnn (find longest coomon string in aliases for that group)
    wwnn_complete_grp_df['Group_Name'] = wwnn_complete_grp_df.alias.apply(lambda aliases: find_grp_name(aliases))

    # second option is based on sliced wwnn number without last byte (for xp, hitachi)
    wwnn_sliced_df = portshow_alias_grp_df.loc[:, alias_grp_lst].copy()
    # for the sliced wwnn case we are interested in unique wwnns only coz grouping perfromed on wwnn number without last byte
    # filter rows from portshow DataFrame with unique wwnn only
    wwnn_sliced_df = wwnn_sliced_df[~wwnn_sliced_df.duplicated(subset=['NodeName'], keep=False)]
    # create new column with value wwnn minus last byte
    wwnn_sliced_df['NodeName_sliced'] = wwnn_sliced_df.NodeName.str.slice(stop = 20)

    # # filter rows with repeated sliced wwnn to have parameter to group on
    # wwnn_sliced_df = wwnn_sliced_df[wwnn_sliced_df.duplicated(subset=['NodeName_sliced'], keep=False)]

    # keep original wwnn_sliced DataFrame for later wwnn and group name correlation and perform grouping on DataFrame copy
    wwnn_sliced_grp_df = wwnn_sliced_df.copy()
    # perform aliases grouping based on sliced wwnn number (wwnn with last byte sliced off)
    wwnn_sliced_grp_df  = wwnn_sliced_grp_df.groupby(['NodeName_sliced'], as_index = False).agg({'alias': ', '.join, 'deviceType': 'first'})
    # define group name for each sliced wwnn (find longest coomon string in aliases for that group)
    wwnn_sliced_grp_df['Group_Name'] = wwnn_sliced_grp_df.alias.apply(lambda aliases: find_grp_name(aliases))
    # in order to correlate each complete wwnn number with group name in wwnn_sliced DataFrame it is required to drop unnecessary columns
    # (leave wwnn sliced and group name columns)
    wwnn_sliced_grp_df.drop(columns = ['alias'], inplace=True)
    # correlate each wwnn with it's group name based on sliced wwnn
    wwnn_sliced_df = wwnn_sliced_df.merge(wwnn_sliced_grp_df, how = 'left', on = ['NodeName_sliced', 'deviceType'])
    # drop sliced wwnn column in order to join two DataFrames
    wwnn_sliced_df.drop(columns = ['NodeName_sliced'], inplace = True)
    
    # concatenate two DataFrames with groups names found based both on complete and sliced wwnn 
    group_name_df = pd.concat([wwnn_complete_grp_df, wwnn_sliced_df])

    # due to different group names appliance depending on device type it is required to filter DataFrames based on device type
    storage_grp_df = group_name_df.loc[group_name_df.deviceType == 'STORAGE'].copy()
    storage_grp_df.drop(columns=['alias', 'deviceType'], inplace = True)
    library_grp_df = group_name_df.loc[group_name_df.deviceType == 'LIB'].copy()
    library_grp_df.drop(columns=['alias', 'deviceType'], inplace = True)

    return storage_grp_df, library_grp_df


def alias_serial_group(portshow_aggregated_df):
    """
    Function to group libraries based on serial number.
    And find alias group name.
    """

    # it is required to find group names for storages and libraries only 
    portshow_alias_grp_df = portshow_aggregated_df.loc[portshow_aggregated_df.deviceType.isin(['STORAGE', 'LIB'])].copy()
    # aliases are required to define device group name
    portshow_alias_grp_df.dropna(subset = ['alias'], inplace = True)
    # change alias case  
    if portshow_alias_grp_df['alias'].notna().any():
        portshow_alias_grp_df.alias = portshow_alias_grp_df.alias.str.upper()
    # columns required to find name for group of serials based on alias
    alias_serial_lst = ['Device_SN', 'alias', 'deviceType']
    # mask to filter libraries
    mask_lib = portshow_alias_grp_df.deviceType == 'LIB'
    # serial number value shouldn't be empty
    mask_serial = pd.notna(portshow_alias_grp_df.Device_SN)
    # filter libraries with non-empty serail number
    lib_sn_df = portshow_alias_grp_df.loc[mask_lib & mask_serial, alias_serial_lst].copy()
    lib_sn_df.drop(columns = ['deviceType'], inplace = True)

    # filter rows from lib DataFrame with repeated serial numbers only
    lib_sn_df = lib_sn_df[lib_sn_df.duplicated(subset=['Device_SN'], keep=False)]
    # perform aliases grouping based on serial number
    lib_sn_group_df = lib_sn_df.groupby(['Device_SN'], as_index = False).agg({'alias': ', '.join})
    # define group name for each serail number (find longest common string in aliases for that group)
    lib_sn_group_df['Group_Name'] = lib_sn_group_df.alias.apply(lambda aliases: find_grp_name(aliases))

    return lib_sn_group_df


def find_grp_name(aliases):
    """Function to find find longest common string in the set of strings"""
    
    aliases = aliases.split(', ')
    if len(aliases) == 1:
        return aliases[0]
    grp_name = get_longest_common_subseq(aliases)
    if not grp_name or len(grp_name) < 3:
        return None
    
    return grp_name.strip('_-')


def get_longest_common_subseq(data):
    """Auxiliary function for find_grp_name function"""
    
    substr = []
    if len(data) > 1 and len(data[0]) > 0:
        for i in range(len(data[0])):
            for j in range(len(data[0])-i+1):
                if j > len(substr) and is_subseq_of_any(data[0][i:i+j], data):
                    substr = data[0][i:i+j]
    return substr

def is_subseq_of_any(find, data):
    """Auxiliary function for get_longest_common_subseq function"""

    if len(data) < 1 and len(find) < 1:
        return False
    for i in range(len(data)):
        if not is_subseq(find, data[i]):
            return False
    return True

# Will also return True if possible_subseq == seq.
def is_subseq(possible_subseq, seq):
    """Auxiliary function for is_subseq_of_any function"""
    
    if len(possible_subseq) > len(seq):
        return False
    def get_length_n_slices(n):
        for i in range(len(seq) + 1 - n):
            yield seq[i:i+n]
    for slyce in get_length_n_slices(len(possible_subseq)):
        if slyce == possible_subseq:
            return True
    return False
