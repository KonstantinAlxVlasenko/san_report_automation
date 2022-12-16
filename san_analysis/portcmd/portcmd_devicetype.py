"""
Module to define connected device class (server, storage, library, switch, vc) and 
device type (emulex, qlogic, xp, 3par, eva, msa and etc) based on device oui
and informarion extracted from nameserver.
Auxiliary to analysis_portcmd module.
"""


import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def oui_join(portshow_aggregated_df, oui_df, switchshow_ports_df):
    """Function to add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)"""  
    
    portshow_aggregated_df['Connected_portWwn_switchshow_filled'] = portshow_aggregated_df['Connected_portWwn']
    switchshow_ports_df['Connected_portWwn_switchshow_filled'] = switchshow_ports_df['connected_portWwn'].str.lower()
    portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, switchshow_ports_df, join_lst=['switchWwn', 'slot', 'port'],
                                                    filled_lst=['Connected_portWwn_switchshow_filled'])
    # extract oui from WWNp
    portshow_aggregated_df['Connected_oui'] = portshow_aggregated_df['Connected_portWwn_switchshow_filled'].str.slice(start=6, stop=14)    
    # add device types from oui DataFrame
    oui_df.dropna(subset=['Connected_oui'], inplace=True)
    portshow_aggregated_df = portshow_aggregated_df.merge(oui_df, how = 'left', on = ['Connected_oui'])
    return portshow_aggregated_df


def type_check(series, switches_oui, blade_servers_df, synergy_servers_df):
    """Function to define device class and type"""
    
    if synergy_servers_df.empty:
        synergy_servers_df['Connected_portWwn'] = np.nan

    # drop rows with empty WWNp values
    blade_hba_df = blade_servers_df.dropna(subset = ['portWwn']).copy()
    synergy_hba_df = synergy_servers_df.dropna(subset = ['Connected_portWwn']).copy()

    if series[['type', 'subtype']].notnull().all():
        # servers type
        # if WWNp in blade hba DataFrame
        # if (blade_hba_df['portWwn'] == series['Connected_portWwn']).any():
        #     return pd.Series(('BLADE_SRV', series['subtype'].split('|')[0]))
        # elif (synergy_hba_df['Connected_portWwn'] == series['Connected_portWwn']).any():
        #     return pd.Series(('SYNERGY_SRV', series['subtype'].split('|')[0]))
        if (series['Connected_portWwn_switchshow_filled'] == blade_hba_df['portWwn']).any():
            return pd.Series(('SRV_BLADE', series['subtype'].split('|')[0]))
        elif (series['Connected_portWwn_switchshow_filled'] == synergy_hba_df['Connected_portWwn']).any():
            return pd.Series(('SRV_SYNERGY', series['subtype'].split('|')[0]))

        # devices with strictly defined type and subtype
        elif not '|' in series['type'] and not '|' in  series['subtype']:
            return pd.Series((series.type, series.subtype))
        # check SWITCH TYPE
        elif 'SRV|SWITCH' in series['type']:
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series.subtype))
            elif series['portType'] in ['E-Port', 'EX-Port']:
                return pd.Series(('SWITCH', series.subtype))
            elif (series['Connected_portWwn_switchshow_filled'][6:] == switches_oui).any():
                return pd.Series(('SWITCH', series.subtype))
            elif series['switchMode'] == 'Access Gateway Mode' and series['portType'] == 'N-Port':
                return pd.Series(('SWITCH', 'AG'))
            elif pd.notna(series['HBA_Manufacturer']) and 'AG Brocade' in series['HBA_Manufacturer']:
                return pd.Series(('SWITCH', series.subtype))
            elif series['Device_type'] == "Physical Unknown(initiator/target)":
                return pd.Series(('SWITCH', series.subtype))
            # when device behind NPIV port
            elif series['Connected_portId'][4:5] != '00':
                return pd.Series(('SRV', series.subtype))
            else:
                return pd.Series(('SRV', series.subtype))

        elif 'STORAGE|SWITCH' in series['type']:
            if series['Device_type'] == 'Physical Unknown(initiator/target)':
                return pd.Series(('SWITCH', series.subtype))
            elif series['Device_type'] == 'Physical Initiator+Target':
                return pd.Series(('STORAGE', series.subtype))           

        # check StoreOnce and D2D devices
        elif pd.notna(series.Device_Model) and 'storeonce' in series['Device_Model'].lower():
            return pd.Series(('LIB', 'StoreOnce'))
        elif pd.notna(series.Device_Model) and 'd2d' in series['Device_Model'].lower():
            return pd.Series(('LIB', 'D2D'))

        # if not d2d than it's storage
        elif series['type'] == 'STORAGE|LIB':
            if pd.notna(series.Device_Model) and 'ultrium' in series['Device_Model'].lower():
                return pd.Series(('LIB', series['subtype'].split('|')[1]))
            else:
                return pd.Series(('STORAGE', series['subtype'].split('|')[0]))
        
        # check if device server or library
        elif series['type'] == 'SRV|LIB': 
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            elif series['Device_type'] in ['Physical Target', 'NPIV Target']:
                return pd.Series(('LIB', series['subtype'].split('|')[1]))
            # if Device_type is empty (No Physical target or Initator) and no Device_Model 
            # and Device serial number then it's SRV
            elif pd.isna(series[['Device_Model', 'Device_SN']]).all() and 'Unknown' in series['Device_type']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
        # check if device server or storage
        elif series['type'] == 'SRV|STORAGE':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            elif pd.notna(series.HBA_Manufacturer) and 'qlogic' in series['HBA_Manufacturer'].lower():
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            else:
                return pd.Series(('STORAGE', series['subtype'].split('|')[1]))
            
        elif series['type'] == 'SRV|STORAGE|LIB':
            if series['Device_type'] in ['Physical Initiator', 'NPIV Initiator']:
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            # if ultrium type
            elif not pd.isna(series['NodeSymb']) and 'ultrium' in series['NodeSymb'].lower():
                return pd.Series(('LIB', series['subtype'].split('|')[2]))
            # if host and hba info present
            elif series[['HBA_Manufacturer', 'HBA_Model', 'Host_OS', 'HBA_Firmware', 'HBA_Driver']].notnull().all():
                return pd.Series(('SRV', series['subtype'].split('|')[0]))
            # if not initiator and not target ultrium than it's storage
            else:
                device_type = series['type'].split('|')[1]
                device_subtype = series['subtype'].split('|')[1]
                return pd.Series((device_type, device_subtype))

    # if device type is not strictly detected
    if pd.isna(series[['deviceType', 'deviceSubtype']]).any():
        # Connected_WWNp is not empty and device type and subtype defined
        if series[['type', 'subtype']].notnull().all():                                
            return pd.Series((series['type'], series['subtype']))
        # Connected_WWNp is empty and no device type and subtype
        else:
            # define link from AG to Native switch
            if pd.notna(series[['switchMode', 'portType']]).all() and \
                series[['switchMode', 'portType']].equals(pd.Series({'switchMode': 'Access Gateway Mode', 'portType': 'N-Port'})):
                return pd.Series(('SWITCH', 'SWITCH'))
            # define ISL link
            elif pd.notna(series[['portState', 'portType']]).all() and \
                ('E-Port' in series['portType'] or 'EX-Port' in series['portType']):
                return pd.Series(('SWITCH', 'SWITCH'))
            # slave F-Port trunk ports have Online status but devices are on master port
            elif pd.isna(series.Connected_portWwn_switchshow_filled) and series['portState'] == 'Online' \
                and pd.notna(series['portScn_details']) and 'Trunk port' in series['portScn_details']:
                return pd.Series((np.nan, np.nan))
            # when device_type is not defined, oui is not founded, 
            # and link is not slave AG or ISL but port is Online then device class is UNKNOWN
            elif series['portState'] == 'Online' and series['portType'] != 'D-Port':
                return pd.Series(('UNKNOWN', 'UNKNOWN'))
            else:
                return pd.Series((np.nan, np.nan))
    else:
        return pd.Series((series['deviceType'], series['deviceSubtype']))
