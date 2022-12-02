"""Module to add Meta SAN information to switch and isl shapes related DataFrames."""


import pandas as pd

from ..shape_details import calculate_sw_grp_y_axis_coordinate


def add_meta_san_graph_sw_pair(san_graph_sw_pair_df, switch_params_aggregated_df, META_SAN_NAME):
    """Function checks if FC Router present in any fabric.
    If yes then creates summary SAN switch pairs containing all fabrics Native mode switches only.
    Then adds summary SAN switch pairs to the switch pairs of all other fabrics."""

    if (switch_params_aggregated_df['FC_Router'] == "ON").any():
        # drop all devices exept Native mode switches
        mask_native_only = ~san_graph_sw_pair_df['switchClass_mode'].str.contains('AG|NPV|VC|FD|XD', na=False)
        san_total_graph_sw_pair_df = san_graph_sw_pair_df.loc[mask_native_only].copy()
        # assign same name to all fabrics 
        san_total_graph_sw_pair_df['Fabric_name'] = META_SAN_NAME
        # same switchWwn might present in the Backbone and edge fabrics
        san_total_graph_sw_pair_df.drop_duplicates(subset='switchName_Wwn', inplace=True)
        san_total_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], inplace=True, ignore_index=True)
        # calculate y-axis coordinate for each switch pair
        calculate_sw_grp_y_axis_coordinate(san_total_graph_sw_pair_df)    
        # concatenatenate san and san_total
        san_graph_sw_pair_df = pd.concat([san_total_graph_sw_pair_df, san_graph_sw_pair_df]).copy()
    return san_graph_sw_pair_df


def add_meta_san_graph_isl(san_graph_isl_df, san_graph_switch_df, switch_params_aggregated_df, META_SAN_NAME):
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
        san_total_graph_isl_df['Fabric_name'] = META_SAN_NAME
        # concatenatenate san and san_total
        san_graph_isl_df = pd.concat([san_total_graph_isl_df, san_graph_isl_df]).copy()
    return san_graph_isl_df