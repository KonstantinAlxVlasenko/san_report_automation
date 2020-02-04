import pandas as pd
# from datetime import date
from files_operations import status_info, load_data, save_data 
from files_operations import force_extract_check, save_xlsx_file, dct_from_columns

"""Module to count Fabric statistics"""


def fabricstatistics_main(report_columns_usage_dct, switchshow_ports_df, fabricshow_ag_labels_df, nscamshow_df, portshow_df, report_data_lst):
    """Main function to count Fabrics statistics
    """
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 17. FABRIC STATISTICS...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['Статистика', 'Статистика_Итого', 'fabric_statistics']
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    fabric_statistics_report_df, fabric_statistics_summary_df, fabric_statistics_df  = data_lst

    # data force extract check 
    # if data have been calculated on previous iterations but force key is ON 
    # then data re-calculated again and saved
    # force key for each DataFrame
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # check if data was loaded and not empty
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # flag if fabrics labels was forced to be changed 
    fabric_labels_change = True if report_steps_dct['fabric_labels'][1] else False

    chassis_column_usage = report_columns_usage_dct['chassis_info_usage']

    # when no data saved or force extract flag is on or fabric labels have been changed than 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or fabric_labels_change:
        # information string if fabric labels force changed was initiated
        # and statistics recounting required
        if fabric_labels_change and not any(force_extract_keys_lst) and all(data_check):
            info = f'Statistics force counting due to change in Fabrics labeling'
            print(info, end =" ")
            status_info('ok', max_title, len(info))             
        
        # get labeled switchshow to perform pandas crosstab method
        # to count number of different type of ports values
        switchshow_df = switchshow_labeled(switchshow_ports_df, fabricshow_ag_labels_df)
        # crosstab to count ports state type (In_Sync, No_Light, etc) 
        # and state summary (Online, Total ports and percentage of occupied)
        port_state_type_df, port_state_summary_df = port_state_statistics(switchshow_df)
        # crosstab to count ports speed (8G, N16, etc) in fabric
        port_speed_df = port_speed_statistics(switchshow_df)
        # crosstab to count device type (Targer, Initiator and etc) in fabric
        target_initiator_df = target_initiator_statistics(switchshow_df, nscamshow_df, portshow_df, report_data_lst)
        # crosstab to count ports types (E-port, F-port, etc) in fabric
        portType_df = portType_statistics(switchshow_df)
        # calculating ratio of device ports to inter-switch links
        # number and bandwidth
        port_ne_df = n_e_statistics(switchshow_df)
        # merge all counted above data into aggregated table and create summary for each fabric from it
        fabric_statistics_df, fabric_statistics_report_df, fabric_statistics_summary_df = \
            merge_statisctics(chassis_column_usage, portType_df, target_initiator_df, \
                port_speed_df, port_ne_df, port_state_type_df, port_state_summary_df, max_title)

        # current operation information string
        info = f'Counting up Fabrics statistics'
        print(info, end =" ")  

        # after finish display status
        status_info('ok', max_title, len(info)) 
        # saving fabric_statistics and fabric_statistics_summary DataFrames to csv file
        save_data(report_data_lst, data_names, fabric_statistics_report_df, fabric_statistics_summary_df, fabric_statistics_df)
    
    # save data to service file if it's required
    save_xlsx_file(fabric_statistics_report_df, 'Статистика', report_data_lst, report_type = 'SAN_Assessment_tables')
    save_xlsx_file(fabric_statistics_summary_df, 'Статистика_Итого', report_data_lst, report_type = 'SAN_Assessment_tables')
        
    return fabric_statistics_df, fabric_statistics_summary_df


def switchshow_labeled(switchshow_ports_df, fabricshow_ag_labels_df):
    """Function to label switchshow with fabric names and labels
    """
    # get from switchshow_ports and fabricshow_ag_labels Data Frames required columns
    switchshow_df = switchshow_ports_df.loc[:, ['chassis_name', 'chassis_wwn', 'switch_index', 
                                            'switchName', 'switchWwn', 'switchMode', 
                                            'portIndex', 'slot', 'port', 
                                            'speed', 'state', 'portType']]    
    fabricshow_labels_df = fabricshow_ag_labels_df.loc[:, ['Worldwide_Name', 'Fabric_name', 'Fabric_label']]
    # rename column in fabricshow_labels DataFrame
    fabricshow_labels_df.rename(columns = {'Worldwide_Name': 'switchWwn'}, inplace=True)
    # merging both DataFrames to get labeled switcshow DataFrame
    switchshow_df = switchshow_df.merge(fabricshow_labels_df, how='left', on = 'switchWwn')
    
    return switchshow_df


def merge_statisctics(chassis_column_usage, portType_df, target_initiator_df, 
                      port_speed_df, port_ne_df, 
                      port_state_type_df, port_state_summary_df, max_title):
    """Function to create aggregated statistics table by merging DataFrames
    """ 
    # port state summary (Online, Total) and target initiator (Physical target, Physical Initiator)
    statistics_df = port_state_summary_df.merge(target_initiator_df, how='left', left_index=True, right_index=True)    
    # statistic and port type (E-port, F-port)
    statistics_df = statistics_df.merge(portType_df, how='left', left_index=True, right_index=True)   
    # statistic and port speed (16G, N32)
    statistics_df = statistics_df.merge(port_speed_df, how='left', left_index=True, right_index=True)    
    # statistics and N:E (device ports to inter switch links ratio) summary
    statistics_df = statistics_df.merge(port_ne_df, how='left', left_index=True, right_index=True)   
    # statistics and port state (InSync, NoModule)
    statistics_df = statistics_df.merge(port_state_type_df, how='left', left_index=True, right_index=True)
    # renaming total row
    statistics_df.rename(index = {'All': 'Всего'}, inplace = True)
    # reset index to drop unneccessary columns
    statistics_df.reset_index(inplace=True)
    # calculating statistics for each fabric
    statistics_subtotal_df = statistics_df.copy()
    # grouping all switches by fabric names and labels 
    # and apply sum function to each group
    statistics_subtotal_df = statistics_subtotal_df.groupby([statistics_subtotal_df.Fabric_name, statistics_subtotal_df.Fabric_label]).sum()
    # reset index to aviod Multi indexing after groupby operation
    statistics_subtotal_df.reset_index(inplace = True)
    # droping columns with N:E ration data due to it pointless on fabric level
    statistics_subtotal_df.drop(columns=['N:E_int', 'N:E_bw_int'], inplace=True)
    # re-calculate percentage of occupied ports
    statistics_subtotal_df['%_occupied'] = round(statistics_subtotal_df.Online.div(statistics_subtotal_df.Total_ports_number)*100, 1)
    statistics_subtotal_df = statistics_subtotal_df.astype('int64', errors = 'ignore')
    # create statistic DataFrame copy
    fabric_statistics_report_df = statistics_df.copy()
    # drop columns 'switch_index', 'switchWwn', 'N:E_int', 'N:E_bw_int'
    fabric_statistics_report_df.drop(columns = ['switch_index', 'switchWwn', 'N:E_int', 'N:E_bw_int'], inplace=True)
    # drop column 'chassis_name' if it is not required
    if not chassis_column_usage:
        fabric_statistics_report_df.drop(columns = ['chassis_name'], inplace=True)
    # column titles used to create dictionary to traslate column names
    statistic_columns_lst = ['Статистика_eng', 'Статистика_ru']
    # dictionary used to translate column names
    statistic_columns_names_dct = dct_from_columns('customer_report', max_title, *statistic_columns_lst, \
        init_file = 'san_automation_info.xlsx')
    # translate columns in fabric_statistics_report and statistics_subtotal_df DataFrames
    fabric_statistics_report_df.rename(columns = statistic_columns_names_dct, inplace = True)
    statistics_subtotal_df.rename(columns = statistic_columns_names_dct, inplace = True)

    return statistics_df, fabric_statistics_report_df, statistics_subtotal_df


def portType_statistics(switchshow_df):
    """Function to count ports types (E-port, F-port, etc) number in fabric
    """
    # crosstab from labeled switchshow
    portType_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                        switchshow_df.chassis_name, switchshow_df.switch_index, 
                                        switchshow_df.switchName, switchshow_df.switchWwn], 
                            columns=switchshow_df.portType, margins = True)
    # droping 'All' columns
    portType_df.drop(columns = 'All', inplace=True)
    
    return portType_df


def target_initiator_statistics(switchshow_df, nscamshow_df, portshow_df, report_data_lst):
    """Function to count device types (Targer, Initiator and etc) number in fabric
    """
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
    """Function to count ports state type (In_Sync, No_Light, etc) number in fabric
    and state summary (Online, Total ports and percentage of occupied ports)
    """
    # crosstab switchshow DataFrame to count port state number in fabric
    port_state_df = pd.crosstab(index = [switchshow_df.Fabric_name, switchshow_df.Fabric_label, 
                                switchshow_df.chassis_name, switchshow_df.switch_index, 
                                switchshow_df.switchName, switchshow_df.switchWwn], 
                        columns=switchshow_df.state, margins = True)
    # renaming column 'All' to Total_port_number
    port_state_df.rename(columns={'All': 'Total_ports_number'}, inplace=True)
    # countin percantage of occupied ports for each switch
    # ratio of Online ports(occupied) number to Total ports number
    port_state_df['%_occupied'] = round(port_state_df.Online.div(port_state_df.Total_ports_number)*100, 1)
    
    # dividing DataFrame into summary DataFrame (Online, Total and percantage of occupied)
    # and rest of port states DataFrame
    # columns for In_Sync, No_Light, No_Module, No_SigDet, No_Sync port states 
    portstate_type_columns = port_state_df.columns.tolist()[:-3]
    # columns for summary DataFrame 
    portstate_summary_columns = port_state_df.columns.tolist()[-3:]
    # create column order Total, Online, % 
    portstate_summary_columns.insert(1, portstate_summary_columns.pop(0))
    # create port_state_type and port_state_summary DataFrames
    # from port_state DataFrame applying columns names
    port_state_summary_df = port_state_df.loc[:, portstate_summary_columns]
    port_state_type_df = port_state_df.loc[:, portstate_type_columns]
    
    return port_state_type_df, port_state_summary_df


def port_speed_statistics(switchshow_df):
    """Function to count ports speed (8G, N16, etc) values number in fabric
    """
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
    """Function to create DataFrame with N:E ratio for each switch
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
    """Auxiliary function to calculate
    N:E ratio for the group of ports belonging to one switch
    """
    # e-ports definition
    e_ports = ['E-Port', 'EX-Port', 'LS E-Port', 'LD E-Port', 'LE-Port', 'N-Port']
    # f-ports definition (n-ports)
    f_ports = ['F-Port', 'G-Port']
    # sum e-ports speed
    e_speed = group.loc[group.portType.isin(e_ports), 'speed'].sum()
    # count e-ports number
    e_num = group.loc[group.portType.isin(e_ports), 'speed'].count()
    # sum f-port(N-port) speed
    f_speed = group.loc[group.portType.isin(f_ports), 'speed'].sum()
    # count f-port(n-port) number
    f_num = group.loc[group.portType.isin(f_ports), 'speed'].count()

    # when e-ports number is not zero
    if e_num != 0:
        # N:E quntity ratio
        ne_num =int(f_num/e_num)
        # N:E bandwidth raio
        ne_bw = int(f_speed/e_speed)
        # string representation ratio
        if f_num != 0:
            ne_num_str = str(ne_num)+':1'
            ne_bw_str = str(ne_bw)+':1'
        else:
            ne_num_str = 'no F-Ports'
            ne_bw_str = 'no F-Ports'
    # when no e-ports on switch 
    else:
        ne_num = 0
        ne_num_str = 'no E-Ports'
        ne_bw = 0
        ne_bw_str = 'no E-Ports'
        
    columns_names = ['N:E_int', 'N:E_num', 'N:E_bw_int', 'N:E_bw']
        
    return pd.Series([ne_num, ne_num_str, ne_bw, ne_bw_str], index= columns_names)