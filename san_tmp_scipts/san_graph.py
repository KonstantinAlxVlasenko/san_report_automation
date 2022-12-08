# -*- coding: utf-8 -*-
"""
Created on Sun May  8 17:01:07 2022

@author: vlasenko
"""

import os
import warnings
import numpy as np
import re

import pandas as pd
script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

# MTS Moscow
db_path = r"D:\Documents\01.CUSTOMERS\MTS\SAN Assessment\JAN2022\mts_msc\database_MTS_msk"
db_file = r"MTS_msk_analysis_database.db"

# # MTS SPb
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_spb\database_MTS_spb"
# db_file = r"MTS_spb_analysis_database.db"

# # MTS Tech
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_tech\database_MTS_Techblock"
# db_file = r"MTS_Techblock_analysis_database.db"

# # NOVATEK MAR2022
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Novatek\SAN Assessment\MAR2022\database_Novatek"
# db_file = r"Novatek_analysis_database.db"

# # OTP
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\OTPBank\SAN Assessment DEC2020\database_OTPBank"
# db_file = r"OTPBank_analysis_database.db"

# # Mechel
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Mechel\SAN Assessment FEB21\database_Mechel"
# db_file = r"Mechel_analysis_database.db"

# # Lenenergo
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Lenenergo\FEB2021_verification\database_Lenenergo"
# db_file = r"Lenenergo_analysis_database.db"

# # Unicreadit
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Unicredit\SAN Assessment\MAR 2021\database_Unicredit"
# db_file = r"Unicredit_analysis_database.db"

# # DataLine Nord
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\NOV2022\database_DataLine Nord"
# db_file = r"DataLine Nord_analysis_database.db"


# # DataLine OST
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN OST\NOV2022\database_DataLine OST"
# db_file = r"DataLine OST_analysis_database.db"

# # DataLine MetroCluster
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN MetroCluster\NOV2022\database_DataLine MetroCluster"
# db_file = r"DataLine MetroCluster_analysis_database.db"


data_names = ['switch_params_aggregated', 'isl_aggregated', 'switch_pair', 'isl_statistics', 'NPIV_statistics']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)


# data_names = ['fcrxlateconfig']

switch_params_aggregated_df, isl_aggregated_df, switch_pair_df, isl_statistics_df, npiv_statistics_df, *_ = data_lst
# fcrxlateconfig_df, *_ = data_lst



# switch_params_aggregated_df['FC_Router'] = "ON"



# import san_graph
san_automation_file = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_automation_info.xlsx'

san_graph_details_df = pd.read_excel(san_automation_file, sheet_name='san_graph_grid', header=2)



# pattern_dct = {'native_speed': r'Native_(N?\d+G?)', 'speed': r'^(N?\d+G?)$', 'ls_mode': r'Long_Distance_(\w+)', 'distance': r'distance += +(\d+ (K|k)m)', 'str_contains_wwn': '([0-9a-f]{2}:){7}[0-9a-f]{2}', 'link_quantity': '(?:ISL|IFL|Link)_\d+'}

pattern_dct = {'native_speed': r'Native_(N?\d+G?)', 'speed': r'^(N?\d+G?)$', 'ls_mode': r'Long_Distance_(\w+)', 
                'distance': r'distance += +(\d+ (K|k)m)', 'wwn': '([0-9a-f]{2}:){7}[0-9a-f]{2}', 
                'link_quantity': '(?:ISL|IFL|Link)_\d+', 'enclosure_slot': r'(Enclosure .+?) slot (\d+)'}



SAN_TOTAL_NAME = 'Meta_SAN'


"""imported-fabric-id
The fabric ID of the fabric that contains the translate domain.

exported-fabric-id
The fabric ID of the remote fabric represented by this translate domain."""



##########
# add switches to which backbone fabric is connected to

def add_edge_sw_to_bb(switch_pair_df, isl_statistics_df):
    """Function to add connected switches to switch_pair_df if switch have connection 
    but connected switch doesn't present in the switch_pair_df for that fabric.
    This happens for examle for IFL links when router have connection to the edge fabric switch
    but connected edge switch is not part of backbone fabric. For san graph these connected
    edge switches are added to the backbone fabric althought they are not part of the
    backbone fabric."""

    # switch (ssave config) - connected switch (connection info from switch ssave config)
    # list of switches in san
    switch_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn']
    switch_pair_cp_df = switch_pair_df[['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn']].copy()

    # list of connected switches in san. 
    isl_connected_switch_df = isl_statistics_df[['Fabric_name', 'Fabric_label', 'Connected_SwitchName', 'Connected_switchWwn']].copy()
    isl_connected_switch_df.dropna(inplace=True)
    isl_connected_switch_df.drop_duplicates(inplace=True)
    isl_connected_switch_df.rename(columns={'Connected_SwitchName': 'switchName', 'Connected_switchWwn': 'switchWwn'}, inplace=True)
    
    # tag switch with 'both' if connected_switch has connection in fabric and present in fabric 
    # tag switch 'left_only' if connected_switch has connection with at least one switch in fabric
    # but not listed in fabric switches (for example in the case of bb router - edge switch connection)
    isl_connected_switch_df = pd.merge(isl_connected_switch_df, switch_pair_cp_df, on=['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn'], 
                                       how='left', indicator='Exist')
    
    # take connected_switches which have connection with switches in the fabric but not listed in that fabric
    absent_connected_switch_df = isl_connected_switch_df.loc[isl_connected_switch_df['Exist'] != 'both'].copy()
    absent_connected_switch_df.drop(columns='Exist', inplace=True)
    
    # add detected absent connected switches to the switch_pair_df
    san_graph_switch_df = pd.concat([switch_pair_df, absent_connected_switch_df])
    san_graph_switch_df.reset_index(drop=True, inplace=True)
    # fill connected_switch information based on switchWwn (connected switch present in switch_pair_df but in different fabric)
    filled_columns = [column for column in switch_pair_df.columns if column not in switch_columns]
    san_graph_switch_df = dfop.dataframe_fillna(san_graph_switch_df, switch_pair_df, 
                                               join_lst=['switchName', 'switchWwn'], filled_lst=filled_columns)
    san_graph_switch_df.sort_values(by=['Fabric_name', 'switchPair_id', 'Fabric_label'], inplace=True)
    return san_graph_switch_df



###############################
# fill switchClass_mode column

def concat_switch_class_mode(san_graph_switch_df):
    """Function to create combination of switch class (DIR, ENT, MID etc),
    switch mode (AG, NPV) and phantom domain type (FD, XD).
    The fututre choice of Visio master and coordinates of the shape on Visio page is based on switchClass_mode value"""

    san_graph_switch_df['switchClass_mode'] = san_graph_switch_df['switchClass']
    
    # switchType for four slot directors
    dir_4slot_type = [77, 121, 165, 179]
    san_graph_switch_df['switchType'] = pd.to_numeric(san_graph_switch_df['switchType'], errors='ignore')
    # add dir_4slot
    mask_dir_4slot = san_graph_switch_df['switchType'].isin(dir_4slot_type)
    san_graph_switch_df.loc[mask_dir_4slot, 'switchClass_mode'] = san_graph_switch_df['switchClass_mode'] + '_4SLOT'
    
    # add AG label
    mask_ag = san_graph_switch_df['switchMode'].str.contains('Access Gateway', na=False)
    san_graph_switch_df.loc[mask_ag, 'switchClass_mode'] = san_graph_switch_df['switchClass_mode'] + ' AG'
    # add NPV label
    mask_npv = san_graph_switch_df['switchMode'].str.contains('NPV', na=False)
    san_graph_switch_df.loc[mask_npv, 'switchClass_mode'] = 'NPV'
    
    # add VC label
    mask_vc = san_graph_switch_df['deviceType'] == 'VC'
    san_graph_switch_df.loc[mask_vc, 'switchClass_mode'] = 'VC'
    
    # add front doamin
    mask_fd = san_graph_switch_df['LS_type_report'] == 'front_domain'
    san_graph_switch_df.loc[mask_fd, 'switchClass_mode'] = 'FD'
    # add translate doamin
    mask_xd = san_graph_switch_df['LS_type_report'] == 'translate_domain'
    san_graph_switch_df.loc[mask_xd, 'switchClass_mode'] = 'XD'
    
    # fill rest with UNKNOWN
    san_graph_switch_df['switchClass_mode'].fillna('UNKNOWN', inplace=True)
    return san_graph_switch_df



def concat_switch_name_did(san_graph_switch_df):
    """Function to create combination of switchname and switch domain ID.
    This combination is used for shape Text in Visio diagram"""
    
    # add domain ID
    san_graph_switch_df = dfop.dataframe_fillna(san_graph_switch_df, switch_params_aggregated_df, join_lst=['switchWwn'], filled_lst=['Domain_ID'])
    # concatenate switchName and Domain ID if DID value exist
    mask_domain_id = san_graph_switch_df['Domain_ID'].notna()
    san_graph_switch_df.loc[mask_domain_id, 'switchName_DID'] = san_graph_switch_df['switchName'] + " (DID=" + san_graph_switch_df['Domain_ID'] + ")"
    # copy switc names for values where DID value is not applicable (VC, AG, NPV)
    san_graph_switch_df['switchName_DID'].fillna(san_graph_switch_df['switchName'], inplace=True)
    return san_graph_switch_df
    



# def concat_switch_name_wwn(san_graph_switch_df, pattern_dct):
#     """Function to concatenate switchname and switchWwn. 
#     If switchName contains wwn (for VC) then switchName is used only.
#     This combination is used for shape Name in Visio Diagram"""
    
#     # concatenate switchName and switchWwn
#     warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
#     mask_wwn_in_name = san_graph_switch_df['switchName'].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
#     san_graph_switch_df.loc[~mask_wwn_in_name, 'switchName_Wwn'] = san_graph_switch_df['switchName'] + " " + san_graph_switch_df['switchWwn']
#     # copy switchName which contains wwn
#     san_graph_switch_df['switchName_Wwn'].fillna(san_graph_switch_df['switchName'], inplace=True)
#     return san_graph_switch_df


def create_san_graph_switch(switch_pair_df, isl_statistics_df, san_graph_details_df):
    """Function to fill switch_pair_df DataFrame with data required to create Visio san graph"""
    
    # add edge switches to which bb routers are connected to bb fabric
    if not isl_statistics_df.empty:
        san_graph_switch_df = add_edge_sw_to_bb(switch_pair_df, isl_statistics_df)
    else:
        san_graph_switch_df = switch_pair_df.copy()
    # create combination of switchClass (DIR, ENT etc) and switchMode (AG, NPV)
    san_graph_switch_df = concat_switch_class_mode(san_graph_switch_df)
    # add san_graph_details (visio master name and shape coordinates on graph depending on switchClass_mode value)
    san_graph_switch_df = san_graph_switch_df.merge(san_graph_details_df, how="left", on="switchClass_mode")
    # create combination of switchName and switch did for shape Text
    san_graph_switch_df = concat_switch_name_did(san_graph_switch_df)
    # create combination of switchName and switchWwn for shape Name
    create_shape_name(san_graph_switch_df, 'switchName', 'switchWwn', 'switchName_Wwn')
    return san_graph_switch_df








######################
# group switches based on switchPair_ID in each fabric_name


def sw_pair_model(series):
    """Aggregation function. 
    If all elements of series are equal returns first one.
    If not returns comma separated string with elements.
    This allows to aviod duplication for model titles in shape Text"""
    
    if series.nunique() == 1:
        return series.reset_index(drop=True)[0]
    
    if series.notna().any():
        return ', '.join([name if name else 'None' for name in series])


def calculate_sw_grp_y_axis_coordinate(san_graph_sw_pair_df):
    """Function to calculate y-axis coordinate for switch groups (switches with the sane switchPair_ID).
    Depending on switchClass_weight switch groups are located in three levels.
    DIR - level 1, ENT, MID, ENTRY - level 2, EMB, AG, NPV, VC - level 3.
    Along the same group level neighboring switch groups are spaced along the y-axis 
    to avoid switch groups to be located in one line."""

    # enumerate rows in each group based in switchPair_ID (create switch group levels to apply y-axis offset)
    san_graph_sw_pair_df['group_level'] = san_graph_sw_pair_df.groupby(['Fabric_name', 'switchClass_weight'])['switchClass_weight'].cumcount()
    # replace even and odd with 0 and 1
    mask_even = san_graph_sw_pair_df['group_level'] % 2 == 0
    san_graph_sw_pair_df['group_level'] = np.select([mask_even, ~mask_even], [0, 1], default=0)
    # calculate y-axis coordinate for the switch group
    san_graph_sw_pair_df['y_group_level'] = san_graph_sw_pair_df['y_graph_level'] + san_graph_sw_pair_df['group_level'] * san_graph_sw_pair_df['y_group_offset']


def create_san_graph_sw_pair(san_graph_switch_df):
    """Function to group switches based on switchPair_ID in each fabric_name.
    Then calculate y-axis coordinate for each switch pair"""

    san_graph_sw_pair_df = san_graph_switch_df.groupby(by=['Fabric_name', 'switchPair_id']).agg(
        {"Fabric_label": ", ".join, "switchWwn": "count", "switchName_DID": "/ ".join, "switchClass_mode": ", ".join, 
         "switchName_Wwn": ", ".join, "switchClass_weight": "first", "graph_level": "first", 
         "y_graph_level": "first", "x_group_offset": "first", "y_group_offset": "first", 
         "x_shape_offset": "first", "y_shape_offset": "first", 
         "master_shape": ", ".join, 'ModelName': sw_pair_model})

    san_graph_sw_pair_df.reset_index(inplace=True)
    san_graph_sw_pair_df.rename(columns={'switchWwn': "quantity"}, inplace=True)
    # sort switch pairs by switchClass_weight so greater class located in the top 
    san_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], inplace=True, ignore_index=True)
    # calculate y-axis coordinate for each switch pair
    calculate_sw_grp_y_axis_coordinate(san_graph_sw_pair_df)
    return san_graph_sw_pair_df



##########################################
# auxiliary functions for links

def find_columns(df, column_pattern):
    """Find df columns corresponding to pattern"""
    
    return [column for column in df.columns if re.search(column_pattern, column)]

def rename_columns(df, column_name_pattern):
    """Function to rename df columns according to column_name_pattern.
    If name duplication occurs '_' symbol is added until column name is unique"""
    
    original_columns = [column for column in df.columns if re.search(column_name_pattern, column)]
    renamed_columns = [re.search(column_name_pattern, column).group(1) for column in original_columns]
    validation_lst = []
    for i, column in enumerate(renamed_columns):
        column_name_changed = False
        while column in validation_lst:
            column = column + "_"
            column_name_changed = True
        validation_lst.append(column)
        if column_name_changed:
            renamed_columns[i] = column
            
    rename_dct = dict(zip(original_columns, renamed_columns))
    df.rename(columns=rename_dct, inplace=True)
    return renamed_columns


def values_to_string(series, columns):
    """Function to convert each row values in columns to string with 
    comma separated values 'value X column_name' for values greater then zero"""

    values_lst = []
    for column in columns:
        if pd.notna(series[column]) and series[column] != 0:
            values_lst.append(str(int(series[column])) + "x" + column)
    if values_lst:
        return ", ".join(values_lst)


def create_shape_name(df, switch_name_column, switch_wwn_column, shape_name_column):
    
    # concatenate switchName and switchWwn
    warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
    mask_wwn_in_name = df[switch_name_column].str.contains("([0-9a-f]{2}:){7}[0-9a-f]{2}")
    df.loc[~mask_wwn_in_name, shape_name_column] = df[switch_name_column] + " " + df[switch_wwn_column]
    df[shape_name_column].fillna(df[switch_name_column], inplace=True)


# ########
# npiv_statistics preparation

def san_graph_npv_init(npiv_statistics_df, pattern_dct):
    """Function to initialize san_graph_npv DataFrame with links details"""
    
    san_graph_npiv_df = npiv_statistics_df.copy()
    # drop fabric summary rows
    san_graph_npiv_df.dropna(subset=['switchWwn', 'NodeName'], inplace=True)
    # drop excessive columns
    npiv_stat_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn', 
                         'Device_Host_Name', 'NodeName', 'Logical_link_quantity', 'Physical_link_quantity']
    native_speed_columns = find_columns(san_graph_npiv_df, pattern_dct['native_speed'])
    link_quantity_columns = find_columns(san_graph_npiv_df, pattern_dct['link_quantity'])
    npiv_stat_columns = [*npiv_stat_columns, *native_speed_columns, *link_quantity_columns]
    san_graph_npiv_df = san_graph_npiv_df[npiv_stat_columns].copy()
    return san_graph_npiv_df
    

def add_npv_link_description(san_graph_npiv_df, pattern_dct):
    """Function to create NPV Link description (link speed)"""

    # remove native tag from speed columns
    speed_columns = rename_columns(san_graph_npiv_df, pattern_dct['native_speed'])
    # convert speed columns to single string
    san_graph_npiv_df['speed_string'] = san_graph_npiv_df.apply(lambda series: values_to_string(series, speed_columns), axis=1)
    
    # check if trunk exist
    link_quantity_columns = find_columns(san_graph_npiv_df, pattern_dct['link_quantity'])
    san_graph_npiv_df['trunk_string'] = san_graph_npiv_df.apply(lambda series: count_trunk_links(series, link_quantity_columns), axis=1)
    # merge each group summary strings into 'Link_description'
    # san_graph_isl_df = dfop.merge_columns(san_graph_npiv_df, summary_column='Link_description', merge_columns=['speed_string', 'trunk_string'], drop_merge_columns=True)
    san_graph_npiv_df = dfop.merge_columns(san_graph_npiv_df, summary_column='Link_description', merge_columns=['speed_string', 'trunk_string'], drop_merge_columns=True)
    return san_graph_npiv_df





    # # column groups
    # speed_columns = find_columns(san_graph_isl_df, pattern_dct['speed'])
    # ls_mode_columns = rename_columns(san_graph_isl_df, pattern_dct['ls_mode'])
    # distance_columns = rename_columns(san_graph_isl_df, pattern_dct['distance'])
    # link_quantity_columns = find_columns(san_graph_isl_df, pattern_dct['link_quantity'])
    # column_grps = [speed_columns, ls_mode_columns, distance_columns,  ['LISL']]
    
    # summary_column_names = ['speed_string', 'ls_mode_string', 'distance_string', 'lisl_string']
    
    # # create summary string with joined values for each group of columns from column_grps
    # for summary_column_name, column_grp in zip(summary_column_names, column_grps):
    #     san_graph_isl_df[summary_column_name] = san_graph_isl_df.apply(lambda series: values_to_string(series, column_grp), axis=1)
    
    # # create IFL summary string
    # if 'Connected_Edge_FID' in isl_aggregated_df.columns:
    #     # add edge FID
    #     san_graph_isl_df = dfop.dataframe_fillna(san_graph_isl_df, isl_aggregated_df, join_lst=['switchWwn', 'Connected_switchWwn'], 
    #                                              filled_lst=['Connected_Edge_FID'])
    #     mask_ifl = san_graph_isl_df['IFL'] > 0
    #     # FID tag + Edge FID
    #     san_graph_isl_df.loc[mask_ifl, 'ifl_string'] = 'IFL FID ' + san_graph_isl_df['Connected_Edge_FID']
    #     summary_column_names.append('ifl_string')
    
    # san_graph_isl_df['trunk_string'] = san_graph_isl_df.apply(lambda series: count_trunk_links(series, link_quantity_columns), axis=1)
    # summary_column_names.append('trunk_string')
    
    
    # # merge each group summary strings into 'Link_description'
    # san_graph_isl_df = dfop.merge_columns(san_graph_isl_df, summary_column='Link_description', merge_columns=summary_column_names, drop_merge_columns=False)
    # return san_graph_isl_df


def create_npv_shape_names(san_graph_npiv_df):
    """Function to create switch, conneceted npv device and link Visio shape names"""

    # switch shape names
    create_shape_name(san_graph_npiv_df, "switchName", "switchWwn", "shapeName")
    # npv device shape names
    create_shape_name(san_graph_npiv_df, "Device_Host_Name", "NodeName", "Connected_shapeName")
    # link shape_name
    san_graph_npiv_df = dfop.merge_columns(san_graph_npiv_df, summary_column='Link_shapeName', 
                                           merge_columns=['shapeName', 'Connected_shapeName'], 
                                           sep=' - ', drop_merge_columns=False)
    return san_graph_npiv_df


def create_san_graph_npv(npiv_statistics_df, pattern_dct):
    """Function to create npiv DataFrame with details required for san diagram"""
    
    if npiv_statistics_df.empty:
        return pd.DataFrame()
    
    # extract required columns, drop summary rows
    san_graph_npiv_df = san_graph_npv_init(npiv_statistics_df, pattern_dct)
    #  create link description (speed)
    san_graph_npiv_df = add_npv_link_description(san_graph_npiv_df, pattern_dct)
    # create switch, npv device and link shape names
    san_graph_npiv_df = create_npv_shape_names(san_graph_npiv_df)
    return san_graph_npiv_df


###########
# san_graph_isl


def count_total_link_type(san_graph_isl_df):
    """"Function to count total number of IFL links"""

    # for link_type in ['LISL', 'IFL']:
    # link_type_pattern = link_type + '_\d+'
    link_type_pattern = 'IFL_\d+'
    link_type_columns = [column for column in san_graph_isl_df.columns if re.search(link_type_pattern, column)]
    # san_graph_isl_df[link_type] = san_graph_isl_df[link_type_columns].sum(axis=1)
    san_graph_isl_df['IFL'] = san_graph_isl_df[link_type_columns].sum(axis=1)



def san_graph_isl_init(isl_statistics_df, pattern_dct):
    """Function to initialize san_graph_isl DataFrame with ISL details"""
    
    san_graph_isl_df = isl_statistics_df.copy()
    # drop fabric summary rows
    san_graph_isl_df.dropna(subset=['switchWwn', 'Connected_switchWwn'], inplace=True)
    # count ifl quantity
    count_total_link_type(san_graph_isl_df)
    # drop excessive columns 
    isl_columns = ['Fabric_name', 'Fabric_label', 'SwitchName', 'switchWwn',
                   'Connected_SwitchName', 'Connected_switchWwn', 'Connection_ID',
                   'switchPair_id', 'Connected_switchPair_id',
                   'Logical_link_quantity', 'Physical_link_quantity']
    if not 'LISL' in san_graph_isl_df.columns:
        san_graph_isl_df['LISL'] = 0
    speed_columns = find_columns(san_graph_isl_df, pattern_dct['speed'])
    distance_columns = find_columns(san_graph_isl_df, pattern_dct['distance'])
    ls_mode_columns = find_columns(san_graph_isl_df, pattern_dct['ls_mode'])
    link_quantity_columns = find_columns(san_graph_isl_df, pattern_dct['link_quantity'])
    
    san_graph_isl_df = san_graph_isl_df[[*isl_columns, 'LISL', 'IFL', *speed_columns, *distance_columns, *ls_mode_columns, *link_quantity_columns]].copy()
    san_graph_isl_df.rename(columns={'SwitchName': 'switchName', 'Connected_SwitchName': 'Connected_switchName'}, inplace=True)
    return san_graph_isl_df


########
# add FD and XD connections

def add_fd_xd_virtual_links(san_graph_isl_df, switch_pair_df):
    """Function to create virtual links between Front Domain and Translate Domains.
    FD present switch from BB fabric to which edge fabric is connected to 
    Number of FD equals to number of connected BB switches. 
    XD represents imported edge fabric. Number of XDs is equal to number of remote edge fabrics.
    Each XD have connection to all FDs."""

    mask_fd = switch_pair_df['LS_type_report'] == 'front_domain'
    mask_xd = switch_pair_df['LS_type_report'] == 'translate_domain'
    
    if not all([mask_fd.any(), mask_xd.any()]):
        return san_graph_isl_df
    

    fd_xd_columns = ['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn', 'switchPair_id']
    # front domain swithes
    fd_df = switch_pair_df.loc[mask_fd, fd_xd_columns].copy()
    # xlate domain switches
    xd_df = switch_pair_df.loc[mask_xd, fd_xd_columns].copy()
    # xlate domain swithes are connected to front domain swithes
    xd_rename_columns = {column: 'Connected_' + column for column in ['switchName', 'switchWwn', 'switchPair_id']}
    xd_df.rename(columns=xd_rename_columns, inplace=True)
    # find active FD (FDs which are in san_graph_isl_df)
    mask_fd_isl = fd_df['switchWwn'].isin(san_graph_isl_df['Connected_switchWwn'])
    fd_df = fd_df.loc[mask_fd_isl].copy()
    # each FD in fabric is connected to each XD
    fd_xd_link_df = fd_df.merge(xd_df, how='left', on=['Fabric_name', 'Fabric_label'])
    # connection ID is sorted combination if FD ID and XD ID 
    fd_xd_link_df['switchPair_id_str'] = fd_xd_link_df['switchPair_id'].astype(int).astype(str)
    fd_xd_link_df['Connected_switchPair_id_str'] = fd_xd_link_df['Connected_switchPair_id'].astype(int).astype(str)
    fd_xd_link_df = dfop.merge_columns(fd_xd_link_df, summary_column='Connection_ID', 
                                                       merge_columns=['switchPair_id_str', 'Connected_switchPair_id_str'],
                                                       sort_summary=True)
    # number of virtual links is virtual
    fd_xd_link_df['Physical_link_quantity'] = 8
    # add FD XD links to san_graph_isl_df
    san_graph_isl_df = pd.concat([san_graph_isl_df, fd_xd_link_df])
    return san_graph_isl_df


def add_fcr_xd_virtual_links(san_graph_isl_df, switch_pair_df, fcrxlateconfig_df):
    """Function to create virtual links in backbone fabric between the router connectd to the edge fabric 
    and it's translate domain in the backbone fabric. First need to group switches in switch_pair_df based on
    fabric_name, fabric_label and check if ls_type_report contains router and translate_domain strings.
    Then attach xlate to the router with the same ifl 'Connection_Edge_ID' and exported-fabric-id"""
    
    pass
 

##############
# sort by switchClass

def drop_duplicated_links(san_graph_isl_df, switch_pair_df):
    """Function to drop duplicated Connection_ID rows in sorted san_graph_isl_df.
    Each link is presented from switch and connected switch sides.
    Link switchClass priority order DIR, ENT, MID, ENTRY and then switchType.
    Bigger switch link comes first"""

    # sort links within connection_pair_id
    san_graph_isl_df = dfop.dataframe_fillna(san_graph_isl_df, switch_pair_df, join_lst=['switchWwn'], filled_lst=['switchClass', 'switchType'])
    mask_dir = san_graph_isl_df['switchClass'].str.contains('DIR', na=False)
    mask_ent = san_graph_isl_df['switchClass'].str.contains('ENT', na=False)
    mask_mid = san_graph_isl_df['switchClass'].str.contains('MID', na=False)
    mask_entry = san_graph_isl_df['switchClass'].str.contains('ENTRY', na=False)
    san_graph_isl_df['switchWeight'] = np.select([mask_dir, mask_ent, mask_mid, mask_entry], [1, 2, 3, 4], default=5)
    san_graph_isl_df.sort_values(by=['Fabric_name', 'Fabric_label', 'Connection_ID', 
                                     'switchWeight', 'switchType', 'switchName'], 
                                 ascending=[*(True,)*4, False, True],  inplace=True)
    # drop duplicated links
    san_graph_isl_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'Connection_ID'], ignore_index=True, inplace=True)
    return san_graph_isl_df



def add_isl_link_description(san_graph_isl_df, isl_aggregated_df, pattern_dct):
    """Function to create ISL Link description (link speed, ls_mode, distance,
    lisl, ifl details)"""

    # column groups
    speed_columns = find_columns(san_graph_isl_df, pattern_dct['speed'])
    ls_mode_columns = rename_columns(san_graph_isl_df, pattern_dct['ls_mode'])
    distance_columns = rename_columns(san_graph_isl_df, pattern_dct['distance'])
    link_quantity_columns = find_columns(san_graph_isl_df, pattern_dct['link_quantity'])
    column_grps = [speed_columns, ls_mode_columns, distance_columns,  ['LISL']]
    
    summary_column_names = ['speed_string', 'ls_mode_string', 'distance_string', 'lisl_string']
    
    # create summary string with joined values for each group of columns from column_grps
    for summary_column_name, column_grp in zip(summary_column_names, column_grps):
        san_graph_isl_df[summary_column_name] = san_graph_isl_df.apply(lambda series: values_to_string(series, column_grp), axis=1)
    
    # create IFL summary string
    if 'Connected_Edge_FID' in isl_aggregated_df.columns:
        # add edge FID
        san_graph_isl_df = dfop.dataframe_fillna(san_graph_isl_df, isl_aggregated_df, join_lst=['switchWwn', 'Connected_switchWwn'], 
                                                 filled_lst=['Connected_Edge_FID'])
        mask_ifl = san_graph_isl_df['IFL'] > 0
        # FID tag + Edge FID
        san_graph_isl_df.loc[mask_ifl, 'ifl_string'] = 'IFL FID ' + san_graph_isl_df['Connected_Edge_FID']
        summary_column_names.append('ifl_string')
    
    san_graph_isl_df['trunk_string'] = san_graph_isl_df.apply(lambda series: count_trunk_links(series, link_quantity_columns), axis=1)
    summary_column_names.append('trunk_string')
    
    
    # merge each group summary strings into 'Link_description'
    san_graph_isl_df = dfop.merge_columns(san_graph_isl_df, summary_column='Link_description', merge_columns=summary_column_names, drop_merge_columns=False)
    return san_graph_isl_df





# tst_sr = pd.Series([2, 1, 5, 0, 4, 5])
# tst_str_sr = tst_sr.loc[tst_sr>1].astype(int).astype(str) + ' links'
# [str(value) + 'x' + key for key, value in tst_str_sr.value_counts().to_dict().items()]

# [(value, key) for value, key in tst_str_sr.value_counts().to_dict().items()]
# # tst_sr.loc[tst_sr>1].value_counts().to_dict()
# tst_str_sr.loc[tst_sr.isin(['Chicago'])]

def count_trunk_links(series, columns):
    
    if series['Logical_link_quantity'] == 1 and series['Physical_link_quantity']>1:
        return 'Trk'

    if series['Logical_link_quantity'] == series['Physical_link_quantity']:
        return None
    
    if pd.isna(series['Logical_link_quantity']):
        return None

    link_quantity_sr = series[columns]
    link_quantity_sr = link_quantity_sr.loc[link_quantity_sr>1].astype(int).astype(str) + ' links'
    trunk_links_lst = [str(value) + 'x' + key for key, value in link_quantity_sr.value_counts().to_dict().items()]
    return 'Trk: ' + ', '.join(trunk_links_lst)


def create_isl_shape_names(san_graph_isl_df):
    """Function to create switch, conneceted switch and link Visio shape names"""
    
    # switch shape name
    create_shape_name(san_graph_isl_df, "switchName", "switchWwn", "shapeName")
    # connected switch shape name
    create_shape_name(san_graph_isl_df, "Connected_switchName", "Connected_switchWwn", "Connected_shapeName")
    # link shape_name
    san_graph_isl_df = dfop.merge_columns(san_graph_isl_df, summary_column='Link_shapeName', 
                                          merge_columns=['shapeName', 'Connected_shapeName'], sep=' - ', 
                                          drop_merge_columns=False)
    return san_graph_isl_df
    
#  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
def create_san_graph_isl(isl_aggregated_df, isl_statistics_df, switch_pair_df, pattern_dct):
    """Function to create isl DataFrame with details required for san diagram"""
    
    if isl_statistics_df.empty:
        return pd.DataFrame()
    
    # extract required columns, drop summary rows
    san_graph_isl_df = san_graph_isl_init(isl_statistics_df, pattern_dct)
    # add front and translate domain connections information
    san_graph_isl_df = add_fd_xd_virtual_links(san_graph_isl_df, switch_pair_df)
    # each link represented from both switches. drop duplicated link rows
    san_graph_isl_df = drop_duplicated_links(san_graph_isl_df, switch_pair_df)
    #  create link description (speed, ls mode, distance, edge fid)
    san_graph_isl_df = add_isl_link_description(san_graph_isl_df, isl_aggregated_df, pattern_dct)
    # create switch, conneceted switch and link shape names
    san_graph_isl_df = create_isl_shape_names(san_graph_isl_df)
    return san_graph_isl_df



def add_meta_san_graph_sw_pair(san_graph_sw_pair_df, switch_params_aggregated_df):
    """Function checks if FC Router present in any fabric.
    If yes then creates summary SAN switch pairs containing all fabrics Native mode switches only.
    Then adds summary SAN switch pairs to the switch pairs of all other fabrics."""
    
    if (switch_params_aggregated_df['FC_Router'] == "ON").any():
        # drop all devices exept Native mode switches
        mask_native_only = ~san_graph_sw_pair_df['switchClass_mode'].str.contains('AG|NPV|VC|FD|XD', na=False)
        san_total_graph_sw_pair_df = san_graph_sw_pair_df.loc[mask_native_only].copy()
        # assign same name to all fabrics 
        san_total_graph_sw_pair_df['Fabric_name'] = SAN_TOTAL_NAME
        # same switchWwn might present in the Backbone and edge fabrics
        san_total_graph_sw_pair_df.drop_duplicates(subset='switchName_Wwn', inplace=True)
        san_total_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], inplace=True, ignore_index=True)
        # calculate y-axis coordinate for each switch pair
        calculate_sw_grp_y_axis_coordinate(san_total_graph_sw_pair_df)    
        # concatenatenate san and san_total
        san_graph_sw_pair_df = pd.concat([san_total_graph_sw_pair_df, san_graph_sw_pair_df]).copy()
    return san_graph_sw_pair_df


def add_meta_san_graph_isl(san_graph_isl_df, san_graph_switch_df, switch_params_aggregated_df):
    """Function checks if FC Router present in any fabric.
    If yes then creates summary SAN switch isls containing all fabrics Native mode switch isls only.
    Then adds summary SAN switch isls to the switch isls of all other fabrics."""
    
    if (switch_params_aggregated_df['FC_Router'] == "ON").any():
        # find FD, XD wwns
        mask_xd_fd = san_graph_switch_df['LS_type_report'].isin(['translate_domain', 'front_domain'])
        fd_xd_df = san_graph_switch_df.loc[mask_xd_fd, ['switchName', 'switchWwn', 'LS_type_report']]
        # drop links to and from xd and fd
        mask_phantom_in_switchwwn = san_graph_isl_df['switchWwn'].isin(fd_xd_df['switchWwn'])
        mask_phantom_in_connected_switchwwn = san_graph_isl_df['Connected_switchWwn'].isin(fd_xd_df['switchWwn'])
        san_total_graph_isl_df = san_graph_isl_df.loc[~mask_phantom_in_switchwwn & ~mask_phantom_in_connected_switchwwn].copy()
        # assign same name to all fabrics
        san_total_graph_isl_df['Fabric_name'] = SAN_TOTAL_NAME
        # concatenatenate san and san_total
        san_graph_isl_df = pd.concat([san_total_graph_isl_df, san_graph_isl_df]).copy()
    return san_graph_isl_df
        

san_graph_switch_df = create_san_graph_switch(switch_pair_df, isl_statistics_df, san_graph_details_df)
san_graph_sw_pair_df = create_san_graph_sw_pair(san_graph_switch_df)
san_graph_sw_pair_df = add_meta_san_graph_sw_pair(san_graph_sw_pair_df, switch_params_aggregated_df)
san_graph_isl_df = create_san_graph_isl(isl_aggregated_df, isl_statistics_df, switch_pair_df, pattern_dct)
san_graph_isl_df = add_meta_san_graph_isl(san_graph_isl_df, san_graph_switch_df, switch_params_aggregated_df)
san_graph_npiv_df = create_san_graph_npv(npiv_statistics_df, pattern_dct)

# san_graph_switch_df_b = san_graph_switch_df.copy()
# san_graph_sw_pair_df_b = san_graph_sw_pair_df.copy()
# san_graph_isl_df_b = san_graph_isl_df.copy()
# san_graph_npiv_df_b = san_graph_npiv_df.copy()


# data_before_lst = [san_graph_switch_df_b, san_graph_sw_pair_df_b, san_graph_isl_df_b, san_graph_npiv_df_b]
# data_after_lst = [san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df]

# def find_differences(data_before_lst, data_after_lst, data_names):

#     for df_name, before_df, after_df in zip(data_names, data_before_lst, data_after_lst):
#         df_equality = after_df.equals(before_df)
#         print(f"\n{df_name} equals {df_equality}")
#         if not df_equality:
#             print("   column names are equal: ", before_df.columns.equals(after_df.columns))
#             print("      Unmatched columns:")
#             for column in before_df.columns:
#                 if not before_df[column].equals(after_df[column]):
#                     print("        ", column)  

# find_differences(data_before_lst, data_after_lst, ['san_graph_switch_df', 'san_graph_sw_pair_df', 'san_graph_isl_df', 'san_graph_npiv_df'])


# ###########
# # san_graph_sw_pair_df for SAN_Total (All Native switches in Fabric with BB)
# if (switch_params_aggregated_df['FC_Router'] == "ON").any():


#     mask_native_only = ~san_graph_sw_pair_df['switchClass_mode'].str.contains('AG|NPV|VC|FD|XD', na=False)
#     san_total_graph_sw_pair_df = san_graph_sw_pair_df.loc[mask_native_only].copy()
#     san_total_graph_sw_pair_df['Fabric_name'] = SAN_TOTAL_NAME

#     san_total_graph_sw_pair_df.drop_duplicates(subset='switchName_Wwn', inplace=True)
#     san_total_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], inplace=True, ignore_index=True)
#     # calculate y-axis coordinate for each switch pair
#     calculate_sw_grp_y_axis_coordinate(san_total_graph_sw_pair_df)


#     # find FD, XD links
    
#     mask_xd_fd = san_graph_switch_df['LS_type_report'].isin(['translate_domain', 'front_domain'])
#     fd_xd_df = san_graph_switch_df.loc[mask_xd_fd, ['switchName', 'switchWwn', 'LS_type_report']]
#     # mask_phantom_in_switchwwn = san_graph_isl_df[['switchWwn', 'Connected_switchWwn']].isin(fd_xd_df['switchWwn']).any(axis=1)
#     mask_phantom_in_switchwwn = san_graph_isl_df['switchWwn'].isin(fd_xd_df['switchWwn'])
#     mask_phantom_in_connected_switchwwn = san_graph_isl_df['Connected_switchWwn'].isin(fd_xd_df['switchWwn'])
#     san_total_graph_isl_df = san_graph_isl_df.loc[~mask_phantom_in_switchwwn & ~mask_phantom_in_connected_switchwwn].copy()
#     san_total_graph_isl_df['Fabric_name'] = SAN_TOTAL_NAME
    
#     # concatenatenate san and san_total
#     san_graph_sw_pair_df = pd.concat([san_total_graph_sw_pair_df, san_graph_sw_pair_df]).copy()
#     san_graph_isl_df = pd.concat([san_total_graph_isl_df, san_graph_isl_df]).copy()
