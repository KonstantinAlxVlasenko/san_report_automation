"""
Module to define Access Gateway and NPV switches connection in portcmd DataFrame.
"""


import numpy as np
import pandas as pd
from common_operations_dataframe import dataframe_fillna


portcmd_columns_lst = [
    'configname',
    'Fabric_name',
    'Fabric_label',
    'chassis_name',
    'chassis_wwn',
    'switchName',
    'switchWwn',
    'Index_slot_port',
    'portIndex',
    'slot',
    'port',
    'Connected_portId',
    'Connected_portWwn',
    'portType',
    'Device_Host_Name',
    'Device_Port',
    'deviceType',
    'deviceSubtype',
    'Connected_NPIV'
    ]


def verify_gateway_link(portshow_aggregated_df):

    portshow_aggregated_df = portshow_aggregated_df.astype({'portIndex': 'str', 'slot': 'str', 'port': 'str'}, errors = 'ignore')
    portshow_aggregated_df['Index_slot_port'] = portshow_aggregated_df.portIndex + '-' + \
        portshow_aggregated_df.slot + '-' + portshow_aggregated_df.port
    portshow_aggregated_df['Connected_NPIV'] = np.where(portshow_aggregated_df.deviceType == 'VC', 'yes', pd.NA)
    master_native_df, master_native_cisco_df, master_ag_df, slave_native_df, slave_ag_df = portcmd_split(portshow_aggregated_df)
    master_native_df, master_ag_df, slave_native_df, slave_ag_df =  find_aglink_connected_port(master_native_df, master_ag_df, slave_native_df, slave_ag_df)
    portshow_aggregated_df, expected_ag_links_df = add_aglink_connected_port(portshow_aggregated_df,
                                master_native_df, master_native_cisco_df, master_ag_df, 
                                slave_native_df, slave_ag_df)

    return portshow_aggregated_df, expected_ag_links_df


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

    # add access gatwwae switches 
    ag_df = pd.concat([ag_df, master_ag_df, slave_ag_df])
    expected_ag_links_df = pd.concat([expected_ag_links_df, master_ag_df, slave_ag_df])
    # drop rows with undefined links
    ag_df.dropna(subset = ['Device_Host_Name'], inplace = True)
    # add information about connected switch nam and port number 
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