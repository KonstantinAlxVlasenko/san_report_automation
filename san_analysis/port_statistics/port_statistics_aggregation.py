"""
Module to get aggreagated connected ports statistics DataFrame 
out of portshow_aggregated_df and switchshow_ports_df DataFrame 
"""

import numpy as np
import pandas as pd
import utilities.dataframe_operations as dfop
# from pandas.core.dtypes.missing import notna


def port_statisctics_aggregated(portshow_aggregated_df):
    """Function to create aggregated statistics table by merging DataFrames"""

    # count statistics for columns
    stat_columns = ['portState', 'license', 'portPhys', 'speed', 'deviceType', 'Device_type', 'portType', 'zoning_enforcement']
    stat_lst = [count_column_statistics(portshow_aggregated_df, column) for column in stat_columns]
    
    # merge all statistics DataFrames in aggregated DataFrame
    port_statistics_df = stat_lst[0].copy()
    for stat_df in stat_lst[1:]:
        port_statistics_df = port_statistics_df.merge(stat_df, how='left', left_index=True, right_index=True)
    port_statistics_df.reset_index(inplace=True)
    
    # count summary for fabric_name and fabric_label levels
    port_statistics_summary_df = dfop.count_summary(port_statistics_df, group_columns=['Fabric_name', 'Fabric_label'])
    # count row All with total values for all fabris
    port_statistics_all_df = dfop.count_all_row(port_statistics_summary_df)
    # concatenate all statistics DataFrames
    port_statistics_df = pd.concat([port_statistics_df, port_statistics_summary_df], ignore_index=True)
    port_statistics_df.sort_values(by=['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn'], inplace=True)
    # concatenate All row so it's at the bottom of statistics DataFrame
    port_statistics_df = pd.concat([port_statistics_df, port_statistics_all_df], ignore_index=True)
    # count percantage of occupied ports for each switch as ratio of Online ports(occupied) number to Licensed ports number
    port_statistics_df['%_occupied'] = round(port_statistics_df['Online'].div(port_statistics_df['Licensed'])*100, 1)
    # count N:E ratio
    port_ne_df = n_e_statistics(portshow_aggregated_df)
    port_statistics_df = port_statistics_df.merge(port_ne_df, how='left', on=['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn'])
    # move columns
    port_statistics_df = dfop.move_column(port_statistics_df, cols_to_move=['Total_ports_number', 'Online', 'Licensed', '%_occupied'],
                                        ref_col='switchWwn')
    return port_statistics_df


def count_column_statistics(portshow_aggregated_df, column: str):
    """Function to count statistics for column"""

    portshow_cp_df = portshow_aggregated_df.copy()

    # drop duplicated ports if statistics is counted for unique ports only
    if not column in ['Device_type', 'deviceType']:
        portshow_cp_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                        'switchName', 'switchWwn', 'slot', 'port'], inplace=True)
    if column == 'license':
        mask_not_licensed = portshow_cp_df['connection_details'].str.contains('no (?:pod|ports on demand) license', case=False)
        portshow_cp_df['license'] = np.where(mask_not_licensed, 'Not_licensed', 'Licensed')
    elif column == 'speed':
        mask1 = portshow_cp_df['speed'] != '--'
        # check rows with Online state ports only
        mask2 = portshow_cp_df['portState'] == 'Online'
        # get required columns
        portshow_cp_df = portshow_cp_df.loc[mask1 & mask2]
    
    column_statistics_df = pd.crosstab(index= [portshow_cp_df.Fabric_name, portshow_cp_df.Fabric_label, 
                                                portshow_cp_df.chassis_name,
                                                portshow_cp_df.switchName, portshow_cp_df.switchWwn], 
                                    columns = portshow_cp_df[column])
    
    if column == 'portState':
        column_statistics_df['Total_ports_number'] = column_statistics_df.sum(axis=1)
    elif column == 'license' and not 'Not_licensed' in column_statistics_df.columns:
        column_statistics_df['Not_licensed'] = 0
    elif column == 'speed':
        # speed columns dictionary to rename DataFrame columns for correct sorting order
        speed_columns = {'2G': 'A', 'N2': 'B', '4G': 'C', 'N4': 'D', '8G': 'E', 'N8': 'F', 
                            '16G': 'G', 'N16': 'H', '32G': 'I', 'N32': 'J',  '64G': 'K', 'N64': 'L'}
        # invert speed columns dictionary to rename back DataFrame columns after sorting 
        speed_columns_invert = {value: key for key, value in speed_columns.items()}
        column_statistics_df = column_statistics_df.rename(columns=speed_columns).sort_index(axis=1).rename(columns=speed_columns_invert)
    return column_statistics_df


def n_e_statistics(portshow_aggregated_df):
    """Function to create DataFrame with N:E ratio for each switch
    (device ports to inter-switch links, quantity and bandwidth ratio)"""    

    portshow_cp_df = portshow_aggregated_df.copy()
    portshow_cp_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                    'switchName', 'switchWwn', 'slot', 'port'], inplace=True)
    # remove row with unknown speed '--'
    mask1 = portshow_cp_df['speed'] != '--'
    # check rows with Online state ports only
    mask2 = portshow_cp_df['portState'] == 'Online'
    # get required columns
    portshow_online_df = portshow_cp_df.loc[mask1 & mask2].copy()
    portshow_online_df['speed'] = portshow_online_df['speed'].str.extract(r'(\d+)').astype('float64')
    # apply n_e auxiliary function to each group of switch ports to calculate N:E ratio
    # each group is one switch 
    port_ne_df = portshow_online_df.groupby([portshow_online_df.Fabric_name, 
                                               portshow_online_df.Fabric_label, 
                                               portshow_online_df.switchName, portshow_online_df.switchWwn]
                                              ).apply(n_e)
    port_ne_df.reset_index(inplace=True)
    return port_ne_df


def n_e(group):
    """Auxiliary function for n_e_statistics function to calculate
    N:E ratio for the group of ports belonging to one switch"""

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
            ne_num_str = 'No_E-Ports'
            # ne_bw = np.nan
            ne_bw_str = 'No_E-Ports'
        elif e_num != 0 and f_num == 0:
            ne_num = 0
            ne_num_str = 'No_F-Ports'
            ne_bw = 0
            ne_bw_str = 'No_F-Ports'
        else:
            # ne_num = np.nan
            ne_num_str = 'No_connected_ports'
            # ne_bw = np.nan
            ne_bw_str = 'No_connected_ports'

    columns_names = ['N:E_int', 'N:E_num', 'N:E_bw_int', 'N:E_bw']
    return pd.Series([ne_num, ne_num_str, ne_bw, ne_bw_str], index= columns_names)
