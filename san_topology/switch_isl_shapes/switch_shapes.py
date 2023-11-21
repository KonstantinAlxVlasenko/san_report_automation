"""Module to create switch shape related DataFrame. 
Switch shape DataFrame used to drop switch pairs on Visio page"""


import pandas as pd

import utilities.dataframe_operations as dfop
from san_automation_constants import DIR_4SLOTS_TYPE

from ..shape_details import (calculate_sw_grp_y_axis_coordinate,
                             create_shape_name)


def create_san_graph_switch(switch_params_aggregated_df, switch_pair_df, isl_statistics_df, san_graph_grid_df):
    """Function to fill switch_pair_df DataFrame with data required to create Visio san graph"""
    
    # add edge switches to which bb routers are connected to bb fabric
    if not isl_statistics_df.empty:
        san_graph_switch_df = add_edge_sw_to_bb(switch_pair_df, isl_statistics_df)
    else:
        san_graph_switch_df = switch_pair_df.copy()
    # create combination of switchClass (DIR, ENT etc) and switchMode (AG, NPV)
    san_graph_switch_df = concat_switch_class_mode(san_graph_switch_df)
    # add san_graph_details (visio master name and shape coordinates on graph depending on switchClass_mode value)
    san_graph_switch_df = san_graph_switch_df.merge(san_graph_grid_df, how="left", on="switchClass_mode")
    # create combination of switchName and switch did for shape Text
    san_graph_switch_df = concat_switch_name_did(san_graph_switch_df, switch_params_aggregated_df)
    # create combination of switchName and switchWwn for shape Name
    create_shape_name(san_graph_switch_df, 'switchName', 'switchWwn', 'switchName_Wwn')
    


    return san_graph_switch_df


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
    san_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], 
                                        inplace=True, ignore_index=True)
    # calculate y-axis coordinate for each switch pair
    calculate_sw_grp_y_axis_coordinate(san_graph_sw_pair_df)
    return san_graph_sw_pair_df


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
    isl_connected_switch_df = pd.merge(isl_connected_switch_df, switch_pair_cp_df, 
                                        on=['Fabric_name', 'Fabric_label', 'switchName', 'switchWwn'], 
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


def concat_switch_class_mode(san_graph_switch_df):
    """Function to create combination of switch class (DIR, ENT, MID etc),
    switch mode (AG, NPV) and phantom domain type (FD, XD).
    The fututre choice of Visio master and coordinates of the shape on Visio page is based on switchClass_mode value"""

    san_graph_switch_df['switchClass_mode'] = san_graph_switch_df['switchClass']
    
    san_graph_switch_df['switchType'] = pd.to_numeric(san_graph_switch_df['switchType'], errors='ignore')
    # add dir_4slot
    mask_dir_4slot = san_graph_switch_df['switchType'].isin(DIR_4SLOTS_TYPE)
    san_graph_switch_df.loc[mask_dir_4slot, 'switchClass_mode'] = san_graph_switch_df['switchClass_mode'] + '_4SLOT'
    # add PRINCIPAL label
    mask_principal = san_graph_switch_df['switchRole'].str.contains('Principal', na=False)
    san_graph_switch_df.loc[mask_principal, 'switchClass_mode'] = san_graph_switch_df['switchClass_mode'] + ' PRINCIPAL'
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


def concat_switch_name_did(san_graph_switch_df, switch_params_aggregated_df):
    """Function to create combination of switchname, switch domain ID and rack.
    This combination is used for shape Text in Visio diagram."""
    
    # add domain ID and rack
    san_graph_switch_df = dfop.dataframe_fillna(san_graph_switch_df, switch_params_aggregated_df, 
                                                join_lst=['switchWwn'], filled_lst=['Domain_ID', 'Device_Rack'])
    # add 'DID=' tag
    mask_domain_id = san_graph_switch_df['Domain_ID'].notna()
    san_graph_switch_df.loc[mask_domain_id, 'Domain_ID_tagged'] = "DID=" + san_graph_switch_df['Domain_ID']
    
    # switch role removed from information string
    # # сut first symbol in swithRole
    # mask_role_notna = san_graph_switch_df['switchRole'].notna()
    # san_graph_switch_df.loc[mask_role_notna, 'switchRole'] = san_graph_switch_df['switchRole'].str[0]
    
    # concatenate DID and rack
    san_graph_switch_df = dfop.merge_columns(san_graph_switch_df, 
                                                summary_column='switchDetails', merge_columns=['Domain_ID_tagged', 'Device_Rack'])
    # concatenate switchName and switchDetails DID value exist
    mask_sw_details = san_graph_switch_df['switchDetails'].notna()
    san_graph_switch_df.loc[mask_sw_details, 'switchName_DID'] = \
        san_graph_switch_df['switchName'] + " (" + san_graph_switch_df['switchDetails'] + ")"
    # copy switc names for values where DID value is not applicable (VC, AG, NPV)
    san_graph_switch_df['switchName_DID'].fillna(san_graph_switch_df['switchName'], inplace=True)
    return san_graph_switch_df


def sw_pair_model(series):
    """Aggregation function. 
    If all elements of series are equal returns first one.
    If not returns comma separated string with elements.
    This allows to aviod duplication for model titles in shape Text"""
    
    if series.nunique() == 1:
        return series.reset_index(drop=True)[0]
    
    if series.notna().any():
        return ', '.join([name if name else 'None' for name in series])





