"""
Module to define Access Gateway and NPV switches connection in portcmd DataFrame.
"""


from collections import defaultdict

import numpy as np
import pandas as pd

from common_operations_dataframe import (dataframe_fabric_labeling,
                                         dataframe_fillna, merge_columns)

portcmd_columns_lst = ['configname', 'Fabric_name', 'Fabric_label',
                        'chassis_name', 'chassis_wwn',
                        'switchName', 'switchWwn',
                        'Index_slot_port', 'portIndex', 'slot', 'port',
                        'speed', 'Connected_portId', 'Connected_portWwn',
                        'portType', 'Device_Host_Name', 'Device_Port', 
                        'deviceType', 'deviceSubtype', 'Connected_NPIV']

def verify_gateway_link(portshow_aggregated_df, switch_params_aggregated_df, ag_principal_df, switch_models_df):
    """Main function to find AG links in portshow_aggregated_df DataFrame"""


    # find AG links using information in portshow_aggregated_df DataFrame itself
    portshow_aggregated_df = portshow_aggregated_df.astype({'portIndex': 'str', 'slot': 'str', 'port': 'str'}, errors = 'ignore').copy()
    # create column with index slot port and column with switchname index slot port
    portshow_aggregated_df['Index_slot_port'] = portshow_aggregated_df.portIndex + '-' + \
        portshow_aggregated_df.slot + '-' + portshow_aggregated_df.port
    portshow_aggregated_df['switchName_Index_slot_port'] = \
        portshow_aggregated_df['switchName'] + ' port ' + portshow_aggregated_df['Index_slot_port']

    portshow_aggregated_df = portshow_aggregated_df.copy()

    portshow_aggregated_df['Connected_NPIV'] = np.where(portshow_aggregated_df.deviceType == 'VC', 'yes', pd.NA)
    master_native_df, master_native_cisco_df, master_ag_df, slave_native_df, slave_ag_df = portcmd_split(portshow_aggregated_df)
    master_native_df, master_ag_df, slave_native_df, slave_ag_df =  \
        find_aglink_connected_port(master_native_df, master_ag_df, slave_native_df, slave_ag_df)
    portshow_aggregated_df, expected_ag_links_df = add_aglink_connected_port(portshow_aggregated_df,
                                master_native_df, master_native_cisco_df, master_ag_df, 
                                slave_native_df, slave_ag_df)
    # complement AG links information in portshow_aggregated_df with information from ag_principal_df DataFrame
    ag_principal_label_df = ag_fabric_labeling(ag_principal_df, switch_params_aggregated_df)
    portshow_aggregated_df = ag_principal_fillna(portshow_aggregated_df, ag_principal_label_df, switch_models_df)

    return portshow_aggregated_df, expected_ag_links_df


def ag_fabric_labeling(ag_principal_df, switch_params_aggregated_df):
    """Function to label ag_principal_df with Fabric names and labels"""

    # columns to select from ag_principal_df DataFrame
    ag_columns_lst = ['configname',	'chassis_name', 'chassis_wwn', 
                    'Principal_switch_name',	'Principal_switch_wwn',
                    'AG_Switch_Name',	'AG_Switch_WWN', 'AG_Switch_Type', 
                    'AG_Switch_Number_of_Ports',	'AG_Switch_IP_Address',	
                    'AG_Switch_Firmware_Version']
    # translate dictionary to correspond columns in switch_params_aggregated_df
    ag_columns_dct = {'Principal_switch_name': 'switchName', 'Principal_switch_wwn': 'switchWwn',
                    'AG_Switch_Name': 'Device_Host_Name', 'AG_Switch_WWN': 'NodeName', 
                    'AG_Switch_Type': 'switchType', 'AG_Switch_IP_Address': 'IP_Address', 
                    'AG_Switch_Firmware_Version': 'Device_Fw'}
    # select and rename ag_principal_df colummns 
    ag_principal_label_df =  ag_principal_df.loc[:, ag_columns_lst].copy()
    ag_principal_label_df.drop_duplicates(inplace=True)
    ag_principal_label_df.rename(columns=ag_columns_dct, inplace=True)
    # label AG Principal DataFrame and drop columns containing Principal switch information
    ag_principal_label_df = dataframe_fabric_labeling(ag_principal_label_df, switch_params_aggregated_df)
    ag_principal_label_df.drop(columns=['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn'], inplace=True)

    return ag_principal_label_df


def ag_principal_fillna(portshow_aggregated_df, ag_principal_label_df, switch_models_df):
    """
    Function to add information from labled Princial AG swithes DataFrame
    to portshow_aggregated_df DataFrame
    """

    # define all AG switches ann VCFC modules as Connected through NPIV
    ag_principal_label_df['Connected_NPIV'] = 'yes'

    # complete ag_principal_label_df DataFrame with information from switch_models DataFrame
    ag_principal_label_df.switchType = ag_principal_label_df.switchType.astype('float64', errors='ignore')
    # floor switchType fractional value to correcpond values in  switch_models_df DataFrame
    ag_principal_label_df.switchType = np.floor(ag_principal_label_df.switchType)  
    switch_models_df.switchType = switch_models_df.switchType.astype('float64', errors='ignore')
    ag_principal_label_df = ag_principal_label_df.merge(switch_models_df, how='left', on='switchType')

    # add AG information to portshow_aggregated_df
    ag_principal_label_df.rename(columns={'HPE_modelName': 'Device_Model'}, inplace=True)
    fillna_columns_lst = ['Fabric_name', 'Fabric_label', 'NodeName',
                        'Device_Host_Name', 'IP_Address', 'Device_Fw', 
                        'Connected_NPIV', 'Device_Model']
    portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, ag_principal_label_df,
                                            join_lst=fillna_columns_lst[:3], filled_lst=fillna_columns_lst[3:])

    return portshow_aggregated_df


def portcmd_split(portshow_aggregated_df):
    """
    Function to allocate presumed AG links from portcmd DataFrame.
    Split them into five logical groups for later merge with each other.
    AG links with founded connected port considered to be confirmed.
    """

    # Native mode and AG(NPIV) mode swithes connected through F-port and N-port respectively
    mask_porttype = portshow_aggregated_df.portType.isin(['F-Port', 'N-Port'])
    # Trunk master and regular NPIV links have Connected WWNp and deviceType identified as swith
    mask_devicetype_switch = portshow_aggregated_df.deviceType == 'SWITCH'
    # Trunk slave AG links have no WWNp and deviceType is empty
    mask_devicetype_empty = portshow_aggregated_df.deviceType.isna()
    
    # DataFrame with suspected AG links to be checked
    portshow_ag_df = portshow_aggregated_df.loc[(mask_devicetype_empty|mask_devicetype_switch) & 
                                                    mask_porttype, portcmd_columns_lst].copy()

    """
    Next step is to devide AG links into five groups.
    1. Group of Trunk master and single AG links with WWNp of Native mode switch.
        Merged with Group#3 to retrieve connected port information
    2. Group of confirmed single NPV links (Cisco switches).
        Information for connected switchname and  portnumber retieved from nsshow.
    3. Group of Trunk master and single AG links with WWNp of AG mode switch.
        Merged with Group#1 to retrieve connected port information.
    4. Group of Trunk slave AG links with WWNp of Native mode switch.
        Merged with Group#5 to retrieve connected port information.
    5. Group of Trunk slave AG links without WWNp of AG mode switch.
        Merged with Group#4 to retrieve connected port information
    """

    # trunk master and single link mask (with WWNp)
    mask_master = portshow_ag_df.Connected_portWwn.notna()
    # trunk slave mask (without WWNp)
    mask_slave = portshow_ag_df.Connected_portWwn.isna()

    # Group#1, Group#2 and Group#3 AG links contain connected WWNp
    master_df = portshow_ag_df.loc[mask_master].copy()
    # Group#1 and Group#2 AG links. 
    # Native mode switches connected to AG/NPV mode switches through the F-port
    master_native_df = master_df.loc[master_df.portType == 'F-Port'].copy()
    # deviceType identified as SWITCH from WWNp
    mask_devicetype_switch = master_native_df.deviceType == 'SWITCH'
    # connected switch name and port number are already known
    mask_device_name_port = master_native_df[['Device_Host_Name', 'Device_Port']].notna().all(axis = 1)
    # Group#2 AG links already contain deviceType and switch name and port number retrieved from NameServer
    master_native_cisco_df = master_native_df.loc[mask_devicetype_switch & mask_device_name_port].copy()
    # Group#1 AG links retrieved from #1 and #2 by dropping #2
    master_native_df.drop(master_native_df[mask_devicetype_switch & mask_device_name_port].index, inplace = True)
    # Group#3 AG links. AG switches connected to Native mode switches through the N-port
    master_ag_df = master_df.loc[master_df.portType == 'N-Port'].copy()
    
    # Group#4 and Group#5 don't contain connected WWNp
    slave_df = portshow_ag_df.loc[mask_slave].copy()
    # Group#4 AG links. Native mode switches connected to AG mode switches through the F-port 
    slave_native_df = slave_df.loc[slave_df.portType == 'F-Port'].copy()
    #  Group#5 AG links. AG switches connected to Native mode switches through the N-port
    slave_ag_df = slave_df.loc[slave_df.portType == 'N-Port'].copy()


    return master_native_df, master_native_cisco_df, master_ag_df, slave_native_df, slave_ag_df


def find_aglink_connected_port(master_native_df, master_ag_df, 
                                slave_native_df, slave_ag_df):
    """
    Function to find connected paired port for each NPIV port
    in AG links Groups#1,3,4,5. Group#5 already have connected port 
    information retrieved from switch NameServer. 
    Function takes four AG link groups as parameters. 
    Each link group with master and slave links identifies paired port by
    merging with corresponding AG link port. Native mode switch merged 
    with AG mode switch and vice versa for master and slave links respectively
    thus finding connected ports. 
    For each of four merging operations four DataFrames to merge with (right DataFrame)
    derived from Groups#1,3,4,5 by filtering columns and renaming columns to correspond
    empty column names in each of AG links groups DataFrames.  
    """

    """
    Find ports connected to trunk master and regular ag links of Native mode switch 
    Group#1 (master_native) merged with Group#3 (master_ag)
    """
    master_native_df = _merge_ag_groups(master_native_df, master_ag_df)


    """
    Find ports connected to trunk master and regular ag links of Access Gateway mode switch 
    Group#3 (master_ag) merged with Group#1 (master_native)
    """

    master_ag_df = _merge_ag_groups(master_ag_df, master_native_df)

    """
    Find ports connected to trunk slave ag links of Access Gateway mode switch 
    Group#5 (slave_ag) merged with Group#4 (slave_native)
    """

    slave_ag_df = _merge_ag_groups(slave_ag_df, slave_native_df, slave_group=True)

    """
    Find ports connected to trunk slave ag links of Native mode switch 
    Group#4 (slave_native) merged with Group#5 (slave_ag)
    """

    slave_native_df = _merge_ag_groups(slave_native_df, slave_ag_df, slave_group=True)

    # fill device type for slave trunk AG links (was not defined before coz of WWNp absence)
    slave_native_df['deviceType'] = np.where(slave_native_df.Device_Host_Name.notna(), 'SWITCH', pd.NA)
    slave_native_df['deviceSubtype'] = np.where(slave_native_df.Device_Host_Name.notna(), 'SWITCH', pd.NA)

    return  master_native_df, master_ag_df, slave_native_df, slave_ag_df


def add_aglink_connected_port(portshow_aggregated_df,
                                master_native_df, master_native_cisco_df, master_ag_df, 
                                slave_native_df, slave_ag_df):    
    """
    Function to combine five AG link groups into one, drop undefined links
    and add information about connected switch name
    and port number from joint AG link DataFrame to the main portcmd DataFrame.
    All AG links from Native mode switch to the AG/NPV switch marked as NPIV.
    """

    # concatenate AG link groups 1, 2, 4 (links from Native to AG)
    # mark links as npiv
    ag_df = pd.concat([master_native_df, master_native_cisco_df, slave_native_df])
    expected_ag_links_df = ag_df.copy()

    ag_df.dropna(subset = ['Device_Host_Name'], inplace = True)
    ag_df['Connected_NPIV'] = 'yes'
    # add access gateway switches 
    ag_df = pd.concat([ag_df, master_ag_df, slave_ag_df])
    expected_ag_links_df = pd.concat([expected_ag_links_df, master_ag_df, slave_ag_df])
    # drop rows with undefined links
    ag_df.dropna(subset = ['Device_Host_Name'], inplace = True)
    # add information about connected switch name and port number 
    # from joint AG link DataFrame to the main portcmd DataFrame.
    join_lst = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn', 'Connected_portId', 'portIndex', 'slot', 'port']
    filled_lst = ['Device_Host_Name', 'Device_Port', 'deviceType', 'deviceSubtype', 'Connected_NPIV']
    portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, ag_df, join_lst, filled_lst)

    return portshow_aggregated_df, expected_ag_links_df


def _merge_ag_groups(left_group_df, right_group_df, slave_group = False):
    """
    Auxiliary function to merge two npiv link groups. Takes right and left groups to merge.
    Returns left group with connected ports from right group.
    """

    # columns of DataFrame to merge with
    join_columns_lst = [
        'Fabric_name', 
        'Fabric_label', 
        'configname',
        'chassis_name',
        'chassis_wwn',
        'switchName',
        'switchWwn',
        'Connected_portId',
        'Index_slot_port'
        ]

    # each group with presumed AG links have empty columns with information about
    # connected device name (Device_Host_Name) and it's port number (Device_Port)
    rename_columns_dct = {'switchName': 'Device_Host_Name', 'Index_slot_port': 'Device_Port'}

    # dataframe_fillna function accepept as parameters two lists
    # list with DataFrames column names to merge on  
    join_lst = ['Fabric_name', 'Fabric_label', 'Connected_portId']
    # list with DataFrames column names from which information need to be copied
    # from right to left DataFrame  
    filled_lst = ['Device_Host_Name', 'Device_Port']

    """
    It's not possible to strictly identify port number connected to the slave AG link 
    if number of slave links in trunk exceeds one (FCID is the same for all trunk ports 
    and there is no WWNp value for slave link). Thus after merge operation we can get
    multiple ports with the same port number for both sides of the link.
    To avoid this we apply grouping of links and joining all ports with the same FCID in one line
    """
    # columns names grouping performed on
    grp_lst = portcmd_columns_lst.copy()
    grp_lst.remove('Device_Port')

    # align Group#5 DataFrame paired for Group#4 DataFrame and perform merge operation
    right_join_df = right_group_df.loc[:, join_columns_lst].copy()
    right_join_df.rename(columns=rename_columns_dct, inplace= True)
    # fill values for connected switch name and port number for each AG link
    left_group_df = dataframe_fillna(left_group_df, right_join_df, join_lst, filled_lst, remove_duplicates=False)

    if slave_group:
        # pandas 1.04 is not able to perform grouping if nan values present thus replcae it with unknown value
        left_group_df.fillna('unknown', inplace=True)
        # grouping ports numbers in case of multiple links in tunk
        left_group_df = left_group_df.groupby(grp_lst, as_index = False).agg({'Device_Port': ', '.join})
        # return nan values
        left_group_df.replace('unknown', pd.NA, inplace=True)

    return left_group_df


def verify_trunkarea_link(portshow_aggregated_df, porttrunkarea_df):
    """Function to fill missing device information for  slave links of trunk area links.
    Device information obtained from master link. And then identify NPIV link number.
    All links inside trunk area link have same number (analogy with ISL number within the trunk)."""

    # counter for switch wwn appeared in DataFrames
    switch_npiv_link_dct = defaultdict(int)

    switch_columns = ['configname', 'chassis_name', 'chassis_wwn',	'switchName', 'switchWwn']
    master_port_columns = ['Master_slot', 'Master_port']
    port_columns = ['slot', 'port']
    device_columns = ['NodeName', 'Device_type', 'Device_Model', 'Device_Fw', 'Device_Name', 
                        'HBA_Manufacturer', 'IP_Address', 'Device_Host_Name', 'Device_Location',  
                        'deviceType',	'deviceSubtype', 'Connected_NPIV', 'NPIV_link_number']
    
    def npiv_link_counter(series):
        """Aux function to identify npiv link number. Link numbers are unique within
        connection of two devices (pairs of Wwnns)"""

        switch_npiv_link_dct[series['Link_Wwnns']] += 1
        return switch_npiv_link_dct[series['Link_Wwnns']]

    if not porttrunkarea_df.empty:
        portshow_cp_df = portshow_aggregated_df.copy()
        porttrunkarea_cp_df = porttrunkarea_df.copy()
        porttrunkarea_cp_df.rename(columns={'SwitchName': 'switchName'}, inplace=True)

        # filter off devices connected behind npiv (except AG links and links between switch and VC module)
        # all devices with FCID xxxx00 are directly connected
        re_zero_fcid = r'\w{4}00'
        mask_zero_fcid = portshow_cp_df['Connected_portId'].str.contains(pat=re_zero_fcid)
        portshow_cp_df = portshow_cp_df.loc[mask_zero_fcid]
        portshow_cp_df['Master_slot'] = portshow_cp_df['slot']
        portshow_cp_df['Master_port'] = portshow_cp_df['port']
        portshow_cp_df = portshow_cp_df.astype({'slot': 'str', 'port': 'str', 
                                                    'Master_slot': 'str', 'Master_port': 'str'}, errors = 'ignore')
        porttrunkarea_cp_df = porttrunkarea_cp_df.astype({'slot': 'str', 'port': 'str', 
                                                    'Master_slot': 'str', 'Master_port': 'str'}, errors = 'ignore')
        # fill device information for each link in trunk area link based on trunk master link
        porttrunkarea_cp_df = dataframe_fillna(porttrunkarea_cp_df, portshow_cp_df, join_lst=[*switch_columns, *master_port_columns],
                                            filled_lst=[*device_columns[:-2], 'Connected_portId'])
        porttrunkarea_cp_df['Connected_NPIV'] = 'yes'


        # npiv trunk area link number defined by link number of trunk master link 
        master_port = porttrunkarea_cp_df['State'] == 'Master'
        porttrunkarea_master_df = porttrunkarea_cp_df.loc[master_port].copy()
        # column with pair Wwnns of the npiv link
        porttrunkarea_master_df['Link_Wwnns'] = porttrunkarea_master_df['switchWwn'] + '_' + porttrunkarea_master_df['NodeName']
        porttrunkarea_master_df['NPIV_link_number'] = porttrunkarea_master_df.apply(lambda series: npiv_link_counter(series), axis=1)
        porttrunkarea_cp_df = dataframe_fillna(porttrunkarea_cp_df, porttrunkarea_master_df, join_lst=[*switch_columns, *master_port_columns],
                                            filled_lst=['NPIV_link_number'])
        # add device information for slave trunk area links
        portshow_aggregated_df = portshow_aggregated_df.astype({'portIndex': 'str', 'slot': 'str', 'port': 'str'}, errors = 'ignore')
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, porttrunkarea_cp_df, 
                                                            join_lst=[*switch_columns, 'Connected_portId', *port_columns], 
                                                            filled_lst=device_columns)
    else:
        portshow_aggregated_df['NPIV_link_number'] = np.nan

    # number npiv links which are not part of the trunk area link
    # each link in that case is independent and have it's own number
    mask_npiv = portshow_aggregated_df['Connected_NPIV'] == 'yes'
    mask_empty_npiv_number = portshow_aggregated_df['NPIV_link_number'].isna()
    mask_link_wwnn_not_na = portshow_aggregated_df[['switchWwn', 'NodeName']].notna().all(axis=1)
    portshow_trunkless_npiv_df = portshow_aggregated_df.loc[mask_npiv & mask_empty_npiv_number & mask_link_wwnn_not_na].copy()
    # column with pair Wwnns of the npiv link
    portshow_trunkless_npiv_df['Link_Wwnns'] = portshow_trunkless_npiv_df['switchWwn'] + '_' + portshow_trunkless_npiv_df['NodeName']

    if not portshow_trunkless_npiv_df.empty:
        portshow_trunkless_npiv_df['NPIV_link_number'] = portshow_trunkless_npiv_df.apply(lambda series: npiv_link_counter(series), axis=1)
        # add npiv links numbers for links out of trunk area links to portshow_aggregated_df DataFrame
        portshow_aggregated_df = dataframe_fillna(portshow_aggregated_df, portshow_trunkless_npiv_df, 
                                                    join_lst=[*switch_columns, 'Connected_portId', *port_columns], 
                                                    filled_lst=['NPIV_link_number'])

    if portshow_aggregated_df['NPIV_link_number'].notna().any():
        portshow_aggregated_df['NPIV_link_number'] = \
            portshow_aggregated_df['NPIV_link_number'].astype('float64', errors='ignore')
        portshow_aggregated_df['NPIV_link_number'] = \
            portshow_aggregated_df['NPIV_link_number'].astype('int64', errors='ignore')

    return portshow_aggregated_df
