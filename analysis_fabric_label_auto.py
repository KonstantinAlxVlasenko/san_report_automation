
"""Module to set fabric names and labels automatically"""

import pandas as pd

from common_operations_filesystem import save_xlsx_file

# auxiliary global variables for auto_fabrics_labeling function
# variables changed globally each time function called
fabric_bb = False
fabric_num = 0
fabric_label = False
called = False


def auto_fabrics_labeling(switchshow_ports_df, fabricshow_df, report_data_lst):
    """Function to auto label fabrics  in fabricshow_df DataFrame"""

    # counts statistics for port type (F-port, E-port) and port state (Online) for each switch in fabricshow
    fabricshow_porttype_state_df = fabricshow_porttype_state(switchshow_ports_df, fabricshow_df)
    # saving DataFrame to Excel if manual labeling required
    save_xlsx_file(fabricshow_porttype_state_df, 'fabricshow_statistics', report_data_lst)
    # removing front domain and translate domain switches from DataFrame
    fabricshow_porttype_state_df = fabricshow_porttype_state_df.loc[fabricshow_porttype_state_df.Enet_IP_Addr != '0.0.0.0']
    # dividing fabricshow_porttype_state_df into groups. One group for each fabric
    fabricshow_grp = fabricshow_porttype_state_df.groupby(
        by=['chassis_name', 'Principal_switch_name', 'Principal_switch_wwn', 'Fabric_ID', 'FC_Route'])
    # applying faricshow_summary for each fabric to summarize fabricshow_porttype_state_df DataFrame
    fabricshow_summary_df = fabricshow_grp.apply(fabricshow_summary)
    # sorting data in such way that two rows (odd and even) are pair fabrics
    fabricshow_summary_df = fabricshow_summary_df.reset_index().sort_values(
        by=['FC_Route', 'Total_switch', 'Domain_IDs', 'Switch_names'], 
        ascending=[False, False, True, True]).reset_index(drop=True)
    # labeling fabrics with auto_fabrics_labeling fanction
    fabricshow_summary_df[['Fabric_name', 'Fabric_label']] = \
        fabricshow_summary_df.apply(lambda row: pd.Series(_auto_fabrics_labeling(row)), axis=1)

    fabricshow_summary_df.sort_values(by=['Fabric_name', 'Fabric_label', 'Principal_switch_name', 'Domain_IDs'],
                                            inplace=True, ignore_index=True)

    return fabricshow_summary_df


def fabricshow_porttype_state(switchshow_ports_df, fabricshow_df):
    """
    Function adding to fabricshow DataFrame summary
    about port type for each switch and total number of ports
    and online ports by crosstab switchshow DataFrame and
    joining with fabricshow DataFrame
    """

    # switchshow DataFrame contains Online switches operating in Native mode
    switchshow_df = switchshow_ports_df.loc[(switchshow_ports_df.switchState == 'Online') & 
                                         (switchshow_ports_df.switchMode == 'Native')
                                         ]
    
    # crosstab DataFrame contains summary for port states for switches from switchshow
    port_state_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                         switchshow_df.switchWwn], columns = switchshow_df.state, margins = True)
    # DataFrame index need to be sorted before loc operation
    port_state_df.sort_index(inplace=True)
    # only total and online ports number are required
    port_state_df = port_state_df.loc[:, ['Online', 'All']]
    # drop row with summary for all ports in fabrics
    port_state_df.drop(index=('All'), inplace=True)
    
    # crosstab DataFrame contains summary for port types for switches from switchshow
    port_type_df = pd.crosstab(index = [switchshow_df.chassis_name, switchshow_df.switchName, 
                                        switchshow_df.switchWwn], columns = switchshow_df.portType)
    
    # concatenating port_type port_state DataFrames by rightjoin
    porttype_state_df = port_type_df.merge(port_state_df, how='right', on = ['chassis_name', 'switchName', 'switchWwn'])
    # fill None values with 0
    porttype_state_df.fillna(0, inplace=True)
    # converting all values to integer
    porttype_state_df = porttype_state_df.astype('int64', errors = 'ignore')
    # sorting index
    porttype_state_df.sort_index(inplace=True)
    
    # concatenating fabricshow and porttype_state DataFrames by leftjoin
    fabricshow_porttype_state_df = fabricshow_df.merge(porttype_state_df, how='left', 
                                                       left_on = ['Worldwide_Name'], right_on=['switchWwn'])
    # fill None values with 0
    fabricshow_porttype_state_df.fillna(0, inplace=True)
    # converting all values to integer
    fabricshow_porttype_state_df = fabricshow_porttype_state_df.astype('int64', errors='ignore')
    
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
    # collects domain ids
    domain_ids = ', '.join(str(i) for i in group.Domain_ID.tolist())
    # collects switch names
    names = ', '.join(group.Name.tolist())
    # column names for Series
    columns_names = ['Total_switch', 'Domain_IDs', 'Switch_names', 'Total_Online_ports']    
    
    return pd.Series([switch_nums, domain_ids, names, online_sum], index= columns_names)


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
    # apply method calls function on first row two times
    # flag to check if function has been already called 
    global called
    
    # called function flag is on
    if called:
        # Online ports present in the fabric
        if row.loc['Total_Online_ports'] != 0:
            # Backbone fabric
            if row.loc['FC_Route'] == 'ON':
                fabric_num_current = 'Fabric_BB'
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
                    fabric_num_current = 'Fabric_' + str(fabric_num)
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
                    fabric_num_current = 'Fabric_' + str(fabric_num)
        # if there are no Online ports in Fabric labels are not assigned
        else:
            fabric_num_current = None
            fabric_label_current = None

        return [fabric_num_current, fabric_label_current]
    # called function flag is off
    # first time function is called do nothing
    else:
        called = True  
