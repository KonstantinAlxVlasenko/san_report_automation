# -*- coding: utf-8 -*-
"""
Created on Wed Feb  9 22:08:56 2022

@author: vlasenko
"""

import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_south\database_MTS_south"
db_file = r"MTS_south_analysis_database.db"

data_names = ['switch_params_aggregated', 'portshow_aggregated']

switch_params_aggregated_df, portshow_aggregated_df = dfop.read_database(db_path, db_file, *data_names)