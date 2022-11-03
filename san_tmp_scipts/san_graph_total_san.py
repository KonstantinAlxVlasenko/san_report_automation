# -*- coding: utf-8 -*-
"""
Created on Sun May 22 23:56:49 2022

@author: vlasenko
"""

import os
import warnings
import numpy as np
import re

import pandas as pd
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop




# san_graph_sw_pair_df for SAN_Total (All Native switches in Fabric with BB)
mask_native_only = ~san_graph_sw_pair_df['switchClass_mode'].str.contains('AG|NPV|VC|FD|XD', na=False)
san_graph_sw_pair_df = san_graph_sw_pair_df.loc[mask_native_only]
san_graph_sw_pair_df['Fabric_name'] = 'SAN_Total'

san_graph_sw_pair_df.drop_duplicates(subset='switchName_Wwn', inplace=True)
san_graph_sw_pair_df.sort_values(by=['Fabric_name', 'switchClass_weight', 'switchName_DID'], inplace=True, ignore_index=True)
# calculate y-axis coordinate for each switch pair
calculate_sw_grp_y_axis_coordinate(san_graph_sw_pair_df)


# find FD, XD links

mask_xd_fd = san_graph_switch_df['LS_type_report'].isin(['translate_domain', 'front_domain'])
fd_xd_df = san_graph_switch_df.loc[mask_xd_fd, ['switchName', 'switchWwn', 'LS_type_report']]
# mask_phantom_in_switchwwn = san_graph_isl_df[['switchWwn', 'Connected_switchWwn']].isin(fd_xd_df['switchWwn']).any(axis=1)
mask_phantom_in_switchwwn = san_graph_isl_df['switchWwn'].isin(fd_xd_df['switchWwn'])
mask_phantom_in_connected_switchwwn = san_graph_isl_df['Connected_switchWwn'].isin(fd_xd_df['switchWwn'])
san_graph_isl_df = san_graph_isl_df.loc[~mask_phantom_in_switchwwn & ~mask_phantom_in_connected_switchwwn].copy()
san_graph_isl_df['Fabric_name'] = 'SAN_Total'