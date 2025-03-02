
"""Module to set fabric names and labels automatically"""

import sys

import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop
import utilities.report_operations as report

# auxiliary global variables for auto_fabrics_labeling function
# variables changed globally each time function called
fabric_bb = False
fabric_num = 0
fabric_label = False


def auto_fabrics_labeling(switchshow_ports_df, switch_params_df, fabricshow_df, project_constants_lst):
    """Function to auto label fabrics  in fabricshow_df DataFrame"""

    # ls_type, customer defined fabric name, xisl usage mode
    switch_ls_type_df = verify_ls_type(switch_params_df)
    # counts statistics for port type (F-port, E-port) and port state (Online) for each switch in fabricshow
    fabricshow_porttype_state_df = fabricshow_porttype_state(switchshow_ports_df, switch_ls_type_df, fabricshow_df)
    # saving DataFrame to Excel if manual labeling required
    report.dataframe_to_excel(fabricshow_porttype_state_df, 'fabricshow_statistics', project_constants_lst, force_flag=True)
    # removing front domain and translate domain switches from DataFrame
    fabricshow_porttype_state_df = fabricshow_porttype_state_df.loc[fabricshow_porttype_state_df.Enet_IP_Addr != '0.0.0.0']
    # dividing fabricshow_porttype_state_df into groups. One group for each fabric
    fabricshow_grp = fabricshow_porttype_state_df.groupby(
        by=['chassis_name', 'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 'FC_Route'], dropna=False)

    # applying faricshow_summary for each fabric to summarize fabricshow_porttype_state_df DataFrame
    fabricshow_summary_df = fabricshow_grp.apply(fabricshow_summary)
    if fabricshow_summary_df.empty:
        print('\n')
        print('No PRINCIPAL switch found. Exiting ...')
        print('\n')
        sys.exit()
    # sorting data in such way that two rows (odd and even) are pair fabrics
    fabricshow_summary_df = fabricshow_summary_df.reset_index().sort_values(
        by=['FC_Route', 'Total_switch', 'Domain_IDs', 'Switch_names'], 
        ascending=[False, False, True, True]).reset_index(drop=True)
    # labeling fabrics with auto_fabrics_labeling fanction
    fabricshow_summary_df[['Fabric_name', 'Fabric_label']] = \
        fabricshow_summary_df.apply(lambda row: pd.Series(_auto_fabrics_labeling(row)), axis=1)

    # if single fabric with two fabric labels then fabric name is san
    if len(fabricshow_summary_df.index) == 2:
        fabricshow_summary_df['Fabric_name'] = 'san'

    fabricshow_summary_df.sort_values(by=['Fabric_name', 'Fabric_label', 'Principal_switch_name', 'Domain_IDs'],
                                            inplace=True, ignore_index=True)
    return fabricshow_summary_df


def verify_ls_type(switch_params_df):
    """Function to identify logical switch type"""

    switchtype_columns = ['chassis_name', 'switchName', 'switchWwn', 
                            'Fabric_Name', 'Allow_XISL_Use', 'LS_mode', 
                            'Base_Switch',	'Default_Switch']
    switch_ls_type_df = switch_params_df[switchtype_columns].copy()

    mask_base = switch_ls_type_df['Base_Switch'] == 'Yes'
    mask_not_base = switch_ls_type_df['Base_Switch'] == 'No'        
    mask_default = switch_ls_type_df['Default_Switch'] == 'Yes'
    mask_logical = switch_ls_type_df['Default_Switch'] == 'No'
    mask_non_ls_mode = switch_ls_type_df['LS_mode'] == 'OFF'
    # identify LS switch type
    switch_ls_type_df['LS_type'] = np.select([mask_base, mask_default, mask_not_base & mask_logical, mask_non_ls_mode],
                                            ['base', 'default', 'logical', 'physical'], default='unknown')

    switch_ls_type_df['Allow_XISL_Use'].replace(to_replace={'ON': 'xisl_on'}, inplace=True) 
    switch_ls_type_df.drop(columns=switchtype_columns[5:], inplace=True)
    switch_ls_type_df.drop_duplicates(subset=switchtype_columns[:3], inplace=True)
    switch_ls_type_df.set_index(switchtype_columns[:3], inplace=True)
    switch_ls_type_df['Fabric_Name'].fillna('Not assigned', inplace=True)

    return switch_ls_type_df


def fabricshow_porttype_state(switchshow_ports_df, switch_ls_type_df, fabricshow_df):
    """
    Function adding to fabricshow DataFrame summary
    about port type for each switch and total number of ports
    and online ports by crosstab switchshow DataFrame and
    joining with fabricshow DataFrame
    """

    merge_lst = ['chassis_name', 'switchName', 'switchWwn']

    # switchshow DataFrame contains Online switches operating in Native mode
    switchshow_df = switchshow_ports_df.loc[(switchshow_ports_df.switchState == 'Online') & 
                                         (switchshow_ports_df.switchMode == 'Native')
                                         ].copy()
    if switchshow_df.empty:
        print('\n')
        print("No Online switches in Fabric(s) found. Exiting ...")
        print('\n')
        sys.exit()
    # crosstab DataFrame contains summary for port states for switches from switchshow

    mask_device_port = switchshow_df['portType'].str.contains('F-Port', na=False)
    dfop.column_to_object(switchshow_df, 'device_port')
    switchshow_df.loc[mask_device_port,  ['device_port']] = 'Device_ports'

    port_state_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                         switchshow_df.switchWwn], columns = switchshow_df.state, margins = True)

    port_device_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                         switchshow_df.switchWwn], columns = switchshow_df.device_port)

    port_state_df = port_state_df.merge(port_device_df, how='left', on=['chassis_name', 'switchName', 'switchWwn'])
                    
    # DataFrame index need to be sorted before loc operation
    port_state_df.sort_index(inplace=True)
    # only total and online ports number are required
    port_state_df = port_state_df.loc[:, ['Online', 'Device_ports', 'All']]

    # drop row with summary for all ports in fabrics
    port_state_df.drop(index=('All'), inplace=True)
    
    # crosstab DataFrame contains summary for port types for switches from switchshow
    port_type_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                        switchshow_df.switchWwn], columns = switchshow_df.portType)
    
    # concatenating port_type port_state DataFrames by rightjoin
    porttype_state_df = port_type_df.merge(port_state_df, how='right', on = ['chassis_name', 'switchName', 'switchWwn'])
    # sorting index
    porttype_state_df.sort_index(inplace=True)
    # concatenate with ls info
    porttype_state_df = porttype_state_df.merge(switch_ls_type_df, how='left', on=merge_lst)
    # concatenating fabricshow and porttype_state DataFrames by leftjoin
    fabricshow_porttype_state_df = fabricshow_df.merge(porttype_state_df, how='left', 
                                                       left_on = ['Worldwide_Name'], right_on=['switchWwn'])
    # columns contain integer information
    non_object_columns = [column for column in fabricshow_porttype_state_df.columns if fabricshow_porttype_state_df[column].dtype != 'object']
    non_object_columns.extend(['Fabric_ID', 'Domain_ID'])                               
    # fill None values with 0
    fabricshow_porttype_state_df[non_object_columns]  = fabricshow_porttype_state_df[non_object_columns].fillna(0)
    # converting all values to integer
    fabricshow_porttype_state_df[non_object_columns] = fabricshow_porttype_state_df[non_object_columns].astype('int64', errors='ignore')
    # columns contains object type and required to fillna with 'unknown'
    object_columns =['Fabric_Name', 'LS_type']
    fabricshow_porttype_state_df[object_columns]  = fabricshow_porttype_state_df[object_columns].fillna('unknown')
    return fabricshow_porttype_state_df


def fabricshow_summary(group):
    """
    Function takes group of switches in fabric.
    Summarizes DomainIDs, Switchnames for each fabric.
    Calculates total switch and online port numbers for each fabric
    Return Series 
    """
    # calculates total switch number in fabric
    switch_nums = group.Name.count()
    # calculates total port number in fabric
    online_sum = group.Online.sum()
    # calculate total device ports
    device_sum = group.Device_ports.sum()
    # domain ids list
    domain_ids_lst = sorted(group.Domain_ID.tolist())
    if len(domain_ids_lst) == 1:
        domain_ids = domain_ids_lst[0]
    else:
        domain_ids = ', '.join(str(i) for i in domain_ids_lst)
    # switch names list
    names = ', '.join(group.Name.tolist())
    # ls switch types in fabric
    ls_type = ', '.join(set(group.LS_type.tolist()))
    # assigned fabric names list
    fabric_name = ', '.join(set(group.Fabric_Name.tolist()))
    # column names for Series
    columns_names = ['Total_switch', 'Domain_IDs', 'Switch_names', 'Device_ports', 'Online_ports', 'LS_type', 'Fabric_Name']    
    return pd.Series([switch_nums, domain_ids, names, device_sum, online_sum, ls_type, fabric_name], index= columns_names)


def _auto_fabrics_labeling(row):
    """
    Auxiliary function for auto_fabrics_labeling function to perform labeling fabric 
    in sorted DataFrame. Each row is one fabric. 
    Returns fabric_name (BB or number) and fabric_label (A or B)
    """
    # global variables changing its values each time function has been called called  
    global fabric_bb # Flag if fabric is Backbone fabric 
    global fabric_num # Number of current Edge Fabric 
    global fabric_label # if True then label A has been already assigned and current fabric should get B label
    
    # Online ports present in the fabric
    if row.loc['Online_ports'] != 0:
        # Backbone fabric
        if row.loc['FC_Route'] == 'ON':
            fabric_num_current = 'BB'
            # first BB fabric row
            if not fabric_bb:
                # label A assigned
                fabric_label_current = 'A'
                fabric_bb = True
            # second BB fabric row
            else:
                # label B assigned
                fabric_label_current = 'B'
        # Edge fabrics
        else:
            # second and more Edge fabric row
            if fabric_num:
                fabric_num_current = 'san' + str(fabric_num)
                # if before 'A' label has been assigned
                if fabric_label:
                    fabric_label = False
                    fabric_label_current = 'B'
                    fabric_num += 1
                # if before 'B' label has been assigned
                else:
                    fabric_label_current = 'A'
                    fabric_label = True
            # first Edge fabric row
            else:
                fabric_num += 1
                fabric_label_current = 'A'
                fabric_label = True
                fabric_num_current = 'san' + str(fabric_num)
    # if there are no Online ports in Fabric labels are not assigned
    else:
        fabric_num_current = None
        fabric_label_current = None
    return [fabric_num_current, fabric_label_current]
