"""
Module to define Access Gateway and NPV switches connection.
"""


import numpy as np
import pandas as pd
from common_operations_dataframe import dataframe_fillna


def switches_gateway_mode(portshow_aggregated_df):

    portshow_aggregated_df = portshow_aggregated_df.astype({'portIndex': 'str', 'slot': 'str', 'port': 'str'}, errors = 'ignore')
    portshow_aggregated_df['Connected_index_slot_port'] = portshow_aggregated_df.portIndex + '-' + \
        portshow_aggregated_df.slot + '-' + portshow_aggregated_df.port
    portshow_aggregated_df['Connected_NPIV'] = np.where(portshow_aggregated_df.deviceType == 'VC', 'yes', np.nan)


def portcmd_split(portshow_aggregated_df):
    """
    Function to allocate presumed AG links from portcmd DataFrame.
    Split them into five logical groups for later merge with each other
    and thus confirm interswitch link status for some of them
    """

    portcmd_columns_lst = [
        'configname',
        'Fabric_name',
        'Fabric_label',
        'chassis_name',
        'chassis_wwn',
        'switchName',
        'switchWwn',
        'Connected_index_slot_port',
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
    4. Group of Trunk slave AG links without WWNp of Native mode switch.
        Merged with Group#5 to retrieve connected port information.
    5. Group of Trunk slave AG links without WWNp of AG mode switch.
        Merged with Group#3 to retrieve connected port information
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
