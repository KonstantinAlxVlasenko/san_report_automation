import re

import pandas as pd

import utilities.dataframe_operations as dfop


def licenseport_statisctics_aggregated(licenseport_df, portshow_aggregated_df, 
                                       switch_params_aggregated_df, pattern_dct):
    """Function to create licenseport statistics DataFrame"""

    # extract license port title, ports quantity related to the title and POD methiod
    licenseport_extracted_df = extract_licenseport_values(licenseport_df, pattern_dct)
    # remove remark 'indicated by', capitalize titles and replace single port to multiple ports
    licenseport_extracted_df = straighten_licenseport_titles(licenseport_extracted_df, pattern_dct)
    # filter portquantity related rows and find total ports for the same title of the same switch
    licenseport_num_df = get_portquantity(licenseport_extracted_df)
    # filter licence pod method
    licenseport_pod_df = licenseport_extracted_df.dropna(subset=['POD_method']).copy()
    # create statistics df using license port titles  as columns and ports quantity for each title as values 
    licenseport_statistics_df = licenseport_num_df.pivot_table('Value', ['configname', 'chassis_name'], 'Name')
    # calculate total ports for each port group
    # total ports number, available (installed) port licenses, used (assigned) port licenses 
    licenseport_statistics_df  = calculate_total_ports_in_group(licenseport_statistics_df)
    # count total and online ports for each chassis
    chassis_ports_statistics_df = count_chassis_ports(portshow_aggregated_df)
    # join chassis port statistics and licenseport statistics
    licenseport_statistics_df = chassis_ports_statistics_df.merge(licenseport_statistics_df, how='left', on=['configname', 'chassis_name'])
    # add switchClass, switchType, switch class weight
    licenseport_statistics_df = add_swclass_sw_type(licenseport_statistics_df, portshow_aggregated_df)
    # add fabric information
    licenseport_statistics_df, logical_sw_usage = add_fname_flabel(
        licenseport_statistics_df, portshow_aggregated_df, switch_params_aggregated_df)
    # add available lic port quantity for directors
    mask_director = licenseport_statistics_df['switchClass'].str.contains('DIR', na=False)
    licenseport_statistics_df.loc[mask_director, 'Port assignments are provisioned for use in this switch'] = \
        licenseport_statistics_df['Total_ports_number']
    # add fabric_name - fabric_label levels statistics summary
    licenseport_statistics_df, licenseport_statistics_summary_df = add_fname_flabel_stats_summary(licenseport_statistics_df)
    # add summary statistics row for all fabrics (metasan)
    licenseport_statistics_df = add_metasan_summary(licenseport_statistics_df, licenseport_statistics_summary_df, logical_sw_usage)
    # count free ports for which license is available, ports for which license is not availble
    # and % of online ports from licensed ports
    licenseport_statistics_df = count_ports(licenseport_statistics_df)
    # add pod method
    licenseport_statistics_df = dfop.dataframe_fillna(licenseport_statistics_df, licenseport_pod_df, 
                                                      join_lst=['configname', 'chassis_name'], 
                                                      filled_lst=['POD_method'])
    return licenseport_statistics_df
    
    
def extract_licenseport_values(licenseport_df, pattern_dct):
    """Function to extract license port title, ports quantity related to the title
    and POD methiod"""

    licenseport_extracted_df = licenseport_df.copy()  
    licenseport_extracted_df = dfop.extract_values_from_column(licenseport_extracted_df, extracted_column='licenseport',
                                                               pattern_column_lst=[
                                                                   (pattern_dct['ports_in_this_switch'], ['Value', 'Name']),
                                                                   (pattern_dct['ports_assigned'], ['Value', 'Name']),
                                                                   (pattern_dct['license_reservations'], ['Value', 'Name']),
                                                                   (pattern_dct['pod_method'], ['POD_method'])
                                                                   ])
    licenseport_extracted_df.dropna(subset=['Value', 'Name', 'POD_method'], how='all', inplace=True)
    licenseport_extracted_df.drop(columns=['licenseport'], inplace=True)
    return licenseport_extracted_df
    

def straighten_licenseport_titles(licenseport_extracted_df, pattern_dct):
    """Function to straighten license port titles (remove remark 'indicated by', capitalize titles
    and replace single port to multiple ports in licensees names"""
    
    # remove 'inicated_by'from license names
    licenseport_extracted_df = dfop.remove_substring(licenseport_extracted_df, column='Name', pattern=pattern_dct['inicated_by'])
    # capitalize license names starting from  lowercase 'port' and 'licence'
    mask_port_lic = licenseport_extracted_df['Name'].str.contains('^port|^license', na=False)
    licenseport_extracted_df.loc[mask_port_lic, 'Name'] = licenseport_extracted_df.loc[mask_port_lic, 'Name'].str.capitalize()
    # replace single port to multiple ports in licensees names to avoid column duplication
    rplcmnt_dct = {'License assignment is held by an offline port': 'License assignments are held by offline ports',
                   'License reservation is still available for use by unassigned ports': 'License reservations are still available for use by unassigned ports'}
    licenseport_extracted_df.replace({'Name': rplcmnt_dct}, inplace=True)
    return licenseport_extracted_df


def get_portquantity(licenseport_extracted_df):
    """Function to filter portquantity related rows from licenseport_extracted_df
    and find total ports for the same title of the same switch"""

    licenseport_num_df = licenseport_extracted_df.copy()
    licenseport_num_df.dropna(subset=['Value'], inplace=True)
    licenseport_num_df['Value'] = pd.to_numeric(licenseport_num_df['Value'], errors='ignore')
    # sum license reservations
    licenseport_num_df = licenseport_num_df.groupby(['configname', 'chassis_name', 'Name'])['Value'].sum()
    licenseport_num_df = licenseport_num_df.reset_index()
    return licenseport_num_df


def calculate_total_ports_in_group(licenseport_statistics_df):
    """Function to calculate total ports for each group in port_grp_type_lst.
    If ports are devided for SFP and QSFP (Gen6 switches from midrange class and upper) 
    then total ports number of the group calculated as sum of ports number within the group.
    If ports are not devivided for SFP and QSFP (Gen5, Gen7 and Gen6 lower than midrange) 
    then total ports number of the group is ports number of the group itself."""

    
    # port column names with port quantity for each port group from port_grp_type_lst
    # including total ports quantity and specific type ports quantity
    licensetotal_ports_columns = []
    
    # port group names for which total port quantity is calculated
    port_grp_type_lst = ['are available', 'are provisioned', 'are assigned', ]
    for port_grp_type in port_grp_type_lst:
        # columns with port quantity of the specific porttypes (SFP and QSFP) for the current group
        port_grp_columns = []
        # column name  of the total ports quantity for the current group
        total_ports_column = None
        # check if column names of the current port group port_grp_type are in the df
        for column in licenseport_statistics_df.columns:
            if port_grp_type in column:
                # column of the total ports quantity for the current group
                if column.startswith('Port'):
                    total_ports_column = column
                # column of the specific port types (SFP or QSFP) port quantity for the current group
                else:
                    port_grp_columns.append(column)
        # if column of the total ports quantity for the current group is not present in the df
        if not total_ports_column and port_grp_columns:
            # construct total ports quantity column name
            total_ports_column = re.search('.+? port(.+)', port_grp_columns[0]).group(1)
            total_ports_column = 'Port' + total_ports_column
        licensetotal_ports_columns.append(total_ports_column)
        if port_grp_columns:
            port_grp_columns.sort(reverse=True)
            licensetotal_ports_columns.extend(port_grp_columns)
            # count total ports quantity for the current group if it's missing 
            mask_total_ports_column_isna = licenseport_statistics_df[total_ports_column].isna()
            licenseport_statistics_df.loc[mask_total_ports_column_isna, total_ports_column] = licenseport_statistics_df[port_grp_columns].sum(axis=1)
        
    # df columns which are not part of any port groups port_grp_type_lst
    missing_columns = [column for column in licenseport_statistics_df.columns if column not in licensetotal_ports_columns]
    # reorder columns
    licenseport_statistics_df = licenseport_statistics_df[licensetotal_ports_columns + missing_columns].reset_index().copy()
    return licenseport_statistics_df


def count_chassis_ports(portshow_aggregated_df):
    """Function to count total and online ports for each chassis.
    Total ports counted from portshow_aggregated_df since directors
    doesn't have any licenseport command output"""
    
    portshow_cp_df = portshow_aggregated_df.copy()
    # drop duplicated chassis ports (npiv ports)
    portshow_cp_df.drop_duplicates(subset=['configname', 'chassis_name', 'chassis_wwn', 
                                                            'switchName', 'switchWwn', 'slot', 'port'], inplace=True)
    portshow_cp_df['Port_quantity'] = 'Total_ports_number'
    # remove all port states except online
    mask_online = portshow_cp_df['portState'] == 'Online'
    portshow_cp_df.loc[~mask_online, 'portState'] = None
    # count total ports and online ports
    chassis_ports_statistics_df = dfop.count_statistics(portshow_cp_df, 
                                                         connection_grp_columns=['configname', 'chassis_name', 'chassis_wwn'], 
                                                         stat_columns=['Port_quantity', 'portState'])
    return chassis_ports_statistics_df


def add_swclass_sw_type(licenseport_statistics_df, portshow_aggregated_df):
    """Function to add switchClass, switchType to the licenseport_statistics_df"""

    # add switch class
    licenseport_statistics_df = dfop.dataframe_fillna(licenseport_statistics_df, portshow_aggregated_df, 
                                                     join_lst=['configname', 'chassis_name', 'chassis_wwn'], 
                                                     filled_lst=['switchClass', 'switchType'])
    # add switch class weight
    dfop.add_swclass_weight(licenseport_statistics_df)
    return licenseport_statistics_df
    

def add_fname_flabel(licenseport_statistics_df, portshow_aggregated_df, switch_params_aggregated_df):
    """Function to add fabric information. If any switch has more than 1 logical switch
    fictional fabric name 'MetaSAN' is used for all switches  to avoid switch duplication in
    differrent virtual fabrics"""
    
    logical_sw_usage = (switch_params_aggregated_df['switch_index'] != 0).any()
    
    fabric_columns = ['Fabric_name', 'Fabric_label']
    if logical_sw_usage:
        fabric_columns.remove('Fabric_name')
        # add total fabric_name to count statistics summary
        # real fabric names are not used to avoid switch 'partitioning' 
        # in case of virtual fabrics usage
        licenseport_statistics_df['Fabric_name'] = 'MetaSAN'
    # licenseport_statistics_df['Fabric_name'] = 'MetaSAN'
    # add fabric label
    chassis_flabel_df = portshow_aggregated_df[fabric_columns + ['configname', 'chassis_name', 'chassis_wwn']].drop_duplicates()
    licenseport_statistics_df = dfop.dataframe_fillna(licenseport_statistics_df, chassis_flabel_df, 
                                                     join_lst=['configname', 'chassis_name', 'chassis_wwn'], 
                                                     filled_lst=fabric_columns, remove_duplicates=False)
    licenseport_statistics_df = dfop.move_column(licenseport_statistics_df, cols_to_move=['Fabric_name', 'Fabric_label'], ref_col='chassis_name', place='before')
    return licenseport_statistics_df, logical_sw_usage


def add_fname_flabel_stats_summary(licenseport_statistics_df):
    """Function to add fabric_name - fabric_label levels statistics summary to the licenseport_statistics_df"""
    
    # count summary for fabric_name and fabric_label levels
    licenseport_stats_cp_df = licenseport_statistics_df.copy()
    licenseport_stats_cp_df.drop(columns=['switchClass_weight', 'switchType'], inplace=True)
    licenseport_statistics_summary_df = dfop.count_summary(licenseport_stats_cp_df, group_columns=['Fabric_name', 'Fabric_label'])
    # concatenate chassis and fname_flabel_summary statistics DataFrames
    licenseport_statistics_df = pd.concat([licenseport_statistics_df, licenseport_statistics_summary_df], ignore_index=True)
    dfop.sort_fabric_swclass_swtype_swname(licenseport_statistics_df, switch_columns=['chassis_name'])
    return licenseport_statistics_df, licenseport_statistics_summary_df


def add_metasan_summary(licenseport_statistics_df, licenseport_statistics_summary_df, logical_sw_usage):
    """Function to add summary statistics row for all fabrics (metasan).
    If switches are not groupped by fabric names (logical switches are present) 
    than total row is not added since it's already in licenseport_statistics_df.
    If switches are groupped by fabric names (no logical switches)
    than total row is counted and added to the licenseport_statistics_df"""

    if logical_sw_usage:
        licenseport_statistics_df.drop(columns=['Fabric_name', 'switchClass',  'switchClass_weight', 'switchType'], inplace=True)
        # mark All Fabric label
        licenseport_statistics_df['Fabric_label'].fillna('All', inplace=True)
    else:
        # count row All with total values for all fabris
        licenseport_statistics_all_df = dfop.count_all_row(licenseport_statistics_summary_df)
        # concatenate All row so it's at the bottom of statistics DataFrame
        licenseport_statistics_df = pd.concat([licenseport_statistics_df, licenseport_statistics_all_df], ignore_index=True)
        licenseport_statistics_df.drop(columns=['switchClass',  'switchClass_weight', 'switchType'], inplace=True)
    return licenseport_statistics_df


def count_ports(licenseport_statistics_df):
    """Function to count free ports for which license is available,
    ports for which license is not availble and % of online ports from licensed ports"""

    # count available ports
    licenseport_statistics_df['Available_licensed_ports'] = \
        licenseport_statistics_df['Port assignments are provisioned for use in this switch'] - licenseport_statistics_df['Online']
    # count pots for which license is not avaialble
    licenseport_statistics_df['Not_licensed_ports'] = licenseport_statistics_df['Ports are available in this switch'] - \
        licenseport_statistics_df['Port assignments are provisioned for use in this switch']
    # count percantage of online ports from licenced ports
    licenseport_statistics_df['Online_from_licensed_%'] = round(
        licenseport_statistics_df['Online'].div(
            licenseport_statistics_df['Port assignments are provisioned for use in this switch'])*100, 1)
    return licenseport_statistics_df