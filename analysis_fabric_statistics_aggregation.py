"""
Module to get aggreagated connected ports statistics DataFrame 
out of portshow_aggregated_df and switchshow_ports_df DataFrame 
"""

import numpy as np
import pandas as pd


def statisctics_aggregated(portshow_aggregated_df, switchshow_ports_df, 
                            fabricshow_ag_labels_df, nscamshow_df, portshow_df, report_data_lst):
    """Function to create aggregated statistics table by merging DataFrames"""

    # get labeled switchshow to perform pandas crosstab method
    # to count number of different type of ports values
    switchshow_df = switchshow_labeled(switchshow_ports_df, fabricshow_ag_labels_df)
    switchshow_df.switch_index = switchshow_df.switch_index.astype('int64')
    # crosstab to count ports state type (In_Sync, No_Light, etc) 
    # and state summary (Online, Total ports and percentage of occupied)
    port_state_type_df, port_state_summary_df = port_state_statistics(switchshow_df)
    # crosstab to count ports speed (8G, N16, etc) in fabric
    port_speed_df = port_speed_statistics(switchshow_df)
    # crosstab to cound device classes(SRV, STORAGE, SWITCH,VC)
    device_class_df = device_class_statistics(portshow_aggregated_df)
    # crosstab to count device type (Target, Initiator and etc) in fabric
    target_initiator_df = target_initiator_statistics(switchshow_df, nscamshow_df, portshow_df, report_data_lst)
    # crosstab to count ports types (E-port, F-port, etc) in fabric
    portType_df = portType_statistics(switchshow_df)
    # calculating ratio of device ports to inter-switch links
    # number and bandwidth
    port_ne_df = n_e_statistics(switchshow_df)

    # merge all statistics DataFrames in aggregated statistics_df DataFrame
    statistics_df = port_state_summary_df.copy()
    merge_lst = [target_initiator_df, portType_df, port_speed_df, device_class_df, port_ne_df, port_state_type_df]
    for df in merge_lst:
        statistics_df = statistics_df.merge(df, how='left', left_index=True, right_index=True)
    # renaming total row
    statistics_df.rename(index = {'All': 'Итого:'}, inplace = True)
    # reset index to drop unneccessary columns
    statistics_df.reset_index(inplace=True)

    return statistics_df


def switchshow_labeled(switchshow_ports_df, fabricshow_ag_labels_df):
    """Function to label switchshow with fabric names and labels"""

    # get from switchshow_ports and fabricshow_ag_labels Data Frames required columns
    switchshow_df = switchshow_ports_df.loc[:, ['chassis_name', 'chassis_wwn', 'switch_index', 
                                            'switchName', 'switchWwn', 'switchMode', 
                                            'portIndex', 'slot', 'port', 
                                            'speed', 'state', 'portType', 'connection_details']]    
    fabricshow_labels_df = fabricshow_ag_labels_df.loc[:, ['Worldwide_Name', 'Fabric_name', 'Fabric_label']]
    # rename column in fabricshow_labels DataFrame
    fabricshow_labels_df.rename(columns = {'Worldwide_Name': 'switchWwn'}, inplace=True)
    # merging both DataFrames to get labeled switcshow DataFrame
    switchshow_df = switchshow_df.merge(fabricshow_labels_df, how='left', on = 'switchWwn')
    
    return switchshow_df


def device_class_statistics(portshow_aggregated_df):
    """Function to count devce classes (SRV_BLADE, SRV, STORAGE, LIB, SWITCH, VC"""
    
    # filter ports without switch_index number
    mask_index = pd.notna(portshow_aggregated_df.switch_index)
    portshow_aggregated_idx_df = portshow_aggregated_df.loc[mask_index].copy()
    # convert switch_index to integer
    portshow_aggregated_idx_df.switch_index = portshow_aggregated_idx_df.switch_index.astype('float64')
    portshow_aggregated_idx_df.switch_index = portshow_aggregated_idx_df.switch_index.astype('int64')

    # crosstab from portshowaggregated_df
    device_class_df = pd.crosstab(index= [portshow_aggregated_idx_df.Fabric_name, portshow_aggregated_idx_df.Fabric_label, 
                                            portshow_aggregated_idx_df.chassis_name, portshow_aggregated_idx_df.switch_index,
                                            portshow_aggregated_idx_df.switchName, portshow_aggregated_idx_df.switchWwn], 
                                    columns = portshow_aggregated_idx_df.deviceType, margins = True)
    
    # droping 'All' columns
    device_class_df.drop(columns = 'All', inplace=True)

    return device_class_df


def portType_statistics(switchshow_df):
    """Function to count ports types (E-port, F-port, etc) number in fabric"""

    # crosstab from labeled switchshow
    portType_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                        switchshow_df.chassis_name, switchshow_df.switch_index, 
                                        switchshow_df.switchName, switchshow_df.switchWwn], 
                            columns=switchshow_df.portType, margins = True)
    # droping 'All' columns
    portType_df.drop(columns = 'All', inplace=True)
    
    return portType_df


def target_initiator_statistics(switchshow_df, nscamshow_df, portshow_df, report_data_lst):
    """Function to count device types (Targer, Initiator and etc) number in fabric"""

    # get required columns from portshow DataFrame
    # contains set of connected to the ports WWNs  
    portshow_connected_df = portshow_df.loc[:, ['chassis_name', 'chassis_wwn', 'slot', 'port', 
                                        'Connected_portId', 'Connected_portWwn']]
    # add connected devices WWNs to supportshow DataFrame 
    switchshow_portshow_df = switchshow_df.merge(portshow_connected_df, how='left', 
                                                    on = ['chassis_name', 'chassis_wwn', 'slot', 'port'])

    # get required columns from nscamshow DataFrame 
    # contains WWNp, WWNn and device type information (Target or Initiator)
    device_type_df = nscamshow_df.loc[:, ['PortName', 'NodeName', 'Device_type']]
    # drop rows with empty values
    device_type_df.dropna(inplace=True)
    # remove rows with duplicate WWNs
    device_type_df.drop_duplicates(subset=['PortName'], inplace=True)
    # add to switchshow device type information of connected WWNs 
    switchshow_portshow_devicetype_df = switchshow_portshow_df.merge(device_type_df, 
                                                                        how='left', left_on = 'Connected_portWwn', right_on= 'PortName')

    # if switch in AG mode then device type must be replaced to Physical instead of NPIV
    mask_ag = switchshow_portshow_devicetype_df.switchMode == 'Access Gateway Mode'
    switchshow_portshow_devicetype_df.loc[mask_ag, 'Device_type'] = \
        switchshow_portshow_devicetype_df.loc[mask_ag, 'Device_type'].str.replace('NPIV', 'Physical')

    # REMOVE to avoid drop NPIV connected ports
    # # drop duplicated ports rows in case of NPIV 
    # switchshow_portshow_devicetype_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'chassis_name', 'chassis_wwn', 
    #                                                             'switchName', 'switchWwn', 'slot', 'port'], inplace = True)

    # if no device type in cached name server (no ISLs links) use 'Unknown' label
    if switchshow_portshow_devicetype_df.Device_type.isna().all():
        mask_nport_fport = switchshow_portshow_devicetype_df['portType'].isin(['F-Port', 'N-Port'])
        switchshow_portshow_devicetype_df.loc[mask_nport_fport, 'Device_type'] = 'Unknown(initiator/target)'

    # crosstab DataFrame to count device types number in fabric
    target_initiator_df = pd.crosstab(index = [switchshow_portshow_devicetype_df.Fabric_name, 
                                                switchshow_portshow_devicetype_df.Fabric_label, 
                                                switchshow_portshow_devicetype_df.chassis_name, 
                                                switchshow_portshow_devicetype_df.switch_index, 
                                                switchshow_portshow_devicetype_df.switchName, 
                                                switchshow_portshow_devicetype_df.switchWwn], 
                                        columns=switchshow_portshow_devicetype_df.Device_type, margins = True)
    # drop All columns
    target_initiator_df.drop(columns = 'All', inplace = True)
    
    return target_initiator_df


def port_state_statistics(switchshow_df):
    """
    Function to count ports state type (In_Sync, No_Light, etc) number in fabric
    and state summary (Online, Total ports and percentage of occupied ports)
    """
    
    # crosstab switchshow DataFrame to count port state number in fabric
    port_state_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                switchshow_df.chassis_name, switchshow_df.switch_index, 
                                switchshow_df.switchName, switchshow_df.switchWwn], 
                        columns=switchshow_df.state, margins = True)
    # renaming column 'All' to Total_port_number
    port_state_df.rename(columns={'All': 'Total_ports_number'}, inplace=True)


    # count licensed vs not licences ports
    # mask_not_licensed = switchshow_df['connection_details'].str.contains('(?:no pod license)|(?:no ports on demand license)', case=False)
    mask_not_licensed = switchshow_df['connection_details'].str.contains('no (?:pod|ports on demand) license', case=False)
    switchshow_df['license'] = np.where(mask_not_licensed, 'Not_licensed', 'Licensed')
    port_license_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                switchshow_df.chassis_name, switchshow_df.switch_index, 
                                switchshow_df.switchName, switchshow_df.switchWwn], 
                        columns=switchshow_df.license, margins = True)
    port_license_df.drop(columns=['All'], inplace=True)
    if not 'Not_licensed' in port_license_df.columns:
        port_license_df['Not_licensed'] = 0

    port_state_df = port_state_df.merge(port_license_df, how='left', left_index=True, right_index=True)
    # countin percantage of occupied ports for each switch
    # ratio of Online ports(occupied) number to Total ports number
    # port_state_df['%_occupied'] = round(port_state_df.Online.div(port_state_df.Total_ports_number)*100, 1)
    port_state_df['%_occupied'] = round(port_state_df.Online.div(port_state_df.Licensed)*100, 1)

    # dividing DataFrame into summary DataFrame (Online, Total and percantage of occupied)
    # and rest of port states DataFrame
    # columns for In_Sync, No_Light, No_Module, No_SigDet, No_Sync port states 
    portstate_type_columns = port_state_df.columns.tolist()[:-5]
    # columns for summary DataFrame 
    portstate_summary_columns = port_state_df.columns.tolist()[-5:]
    # create column order Total, Online, % 
    portstate_summary_columns.insert(1, portstate_summary_columns.pop(0))
    # create port_state_type and port_state_summary DataFrames
    # from port_state DataFrame applying columns names
    port_state_summary_df = port_state_df.loc[:, portstate_summary_columns]
    port_state_type_df = port_state_df.loc[:, portstate_type_columns]
    
    return port_state_type_df, port_state_summary_df


def port_speed_statistics(switchshow_df):
    """Function to count ports speed (8G, N16, etc) values number in fabric"""

    # speed columns dictionary to rename DataFrame columns for correct sorting order
    speed_columns = {'2G': 'A', 'N2': 'B', '4G': 'C', 'N4': 'D', '8G': 'E', 
                     'N8': 'F', '16G': 'G', 'N16': 'H', '32G': 'I', 'N32': 'J' }
    # invert speed columns dictionary to rename back DataFrame columns after sorting 
    speed_columns_invert = {value: key for key, value in speed_columns.items()}
    # crosstab switchshow DataFrame to count port speed values number in fabric
    # check only Online state ports
    port_speed_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                    switchshow_df.chassis_name, switchshow_df.switch_index, 
                                    switchshow_df.switchName, switchshow_df.switchWwn], 
                           columns=switchshow_df[switchshow_df.state == 'Online'].speed, margins=True)
    # drop column with unknown port speed if they exist
    port_speed_df = port_speed_df.loc[:, port_speed_df.columns != '--']
    # rename speed columns in accordance to speed_columns dictionary
    # sort DataFrame by columns names
    # rename columns in accordance to speed_columns_invert dictionary
    port_speed_df = port_speed_df.rename(columns=speed_columns).sort_index(axis=1).rename(columns=speed_columns_invert)
    # drop column 'All'
    port_speed_df.drop(columns = 'All', inplace = True)
    
    return port_speed_df


def n_e_statistics(switchshow_df):
    """
    Function to create DataFrame with N:E ratio for each switch
    (device ports to inter-switch links, quantity and bandwidth ratio)
    """

    # dictionary to convert string speed represenation to integer number
    speed_columns_gbs = {'2G': 2, 'N2': 2, '4G': 4, 'N4': 4, '8G': 8, 
                         'N8': 8, '16G': 16, 'N16': 16, '32G': 32, 'N32': 32}
    # get required rows and columns from switchshow DataFrame
    # remove row with unknown speed '--'
    mask1 = switchshow_df.speed != '--'
    # check rows with Online state ports only
    mask2 = switchshow_df.state == 'Online'
    # get required columns
    switchshow_online_df = switchshow_df.loc[mask1 & mask2, ['Fabric_name', 'Fabric_label', 
                                                             'chassis_name', 'switch_index', 
                                                             'switchName','switchWwn', 
                                                             'portType', 'speed']]
    # convert speed string values to integer
    switchshow_online_df['speed'] = \
        switchshow_online_df.loc[:, 'speed'].apply(lambda x: speed_columns_gbs.get(x))
    # apply n_e auxiliary function to each group of switch ports to calculate N:E ratio
    # each group is one switch 
    port_ne_df = switchshow_online_df.groupby([switchshow_online_df.Fabric_name, 
                                               switchshow_online_df.Fabric_label, 
                                               switchshow_online_df.chassis_name,  switchshow_online_df.switch_index, 
                                               switchshow_online_df.switchName, switchshow_online_df.switchWwn]
                                              ).apply(n_e)
    return port_ne_df


def n_e(group):
    """
    Auxiliary function for n_e_statistics function to calculate
    N:E ratio for the group of ports belonging to one switch
    """

    # e-ports definition
    e_ports = ['E-Port', 'EX-Port', 'LS E-Port', 'LD E-Port', 'LE-Port', 'LE E-Port', 'N-Port']
    # f-ports definition (n-ports)
    f_ports = ['F-Port']
    # sum e-ports speed
    e_speed = group.loc[group.portType.isin(e_ports), 'speed'].sum()
    # count e-ports number
    e_num = group.loc[group.portType.isin(e_ports), 'speed'].count()
    # sum f-port(N-port) speed
    f_speed = group.loc[group.portType.isin(f_ports), 'speed'].sum()
    # count f-port(n-port) number
    f_num = group.loc[group.portType.isin(f_ports), 'speed'].count()


    if e_num != 0 and f_num != 0:

        # N:E quntity ratio
        if e_num <= f_num: 
            ne_num = int(f_num/e_num)
            ne_num_str = str(ne_num)+':1'
        else:
            ne_num = int(e_num/f_num)
            ne_num_str = '1:' + str(ne_num)

        # N:E bandwidth raio
        if e_speed <= f_speed:
            ne_bw = int(f_speed/e_speed)
            ne_bw_str = str(ne_bw)+':1'
        else:
            ne_bw = int(e_speed/f_speed)
            ne_bw_str = '1:' + str(ne_bw)
    else:
        ne_num = np.nan
        ne_bw = np.nan
        if e_num == 0 and f_num != 0:
            # ne_num = np.nan
            ne_num_str = 'No E-Ports'
            # ne_bw = np.nan
            ne_bw_str = 'No E-Ports'
        elif e_num != 0 and f_num == 0:
            ne_num = 0
            ne_num_str = 'No F-Ports'
            ne_bw = 0
            ne_bw_str = 'No F-Ports'
        else:
            # ne_num = np.nan
            ne_num_str = 'No connected ports'
            # ne_bw = np.nan
            ne_bw_str = 'No connected ports'

    columns_names = ['N:E_int', 'N:E_num', 'N:E_bw_int', 'N:E_bw']
        
    return pd.Series([ne_num, ne_num_str, ne_bw, ne_bw_str], index= columns_names)