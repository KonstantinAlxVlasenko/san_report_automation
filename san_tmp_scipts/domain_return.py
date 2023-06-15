# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 14:27:09 2023

@author: kavlasenko
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

db_path = r"D:\Documents\01.CUSTOMERS\Rostrud\SAN\APR2023\database_Rostrud"
db_file = r"Rostrud_analysis_database.db"



data_names = ['portshow_aggregated']
data_lst = dfop.read_database(db_path, db_file, *data_names)
data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)


portshow_aggregated_df, *_ = data_lst



portshow_aggregated_df['Domain_name_dropped_quantity'] = portshow_aggregated_df.groupby(['Device_Host_Name'])['Domain_name_dropped'].transform('nunique')
mask_domain_name_freeze = portshow_aggregated_df['Domain_name_dropped_quantity'] > 1
portshow_aggregated_df.loc[mask_domain_name_freeze, 'Domain_drop_status'] = 'domain_freezed'
portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name'] = portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name_w_domain']



columns = ['Connected_portWwn', 'Device_Host_Name', 'Device_Host_Name_w_domain', 'Domain_name_dropped']
mask_dropped_domains = portshow_aggregated_df['Domain_name_dropped'].notna()

hostname_dropped_domain_df = portshow_aggregated_df.loc[mask_dropped_domains, columns].copy()

hostname_dropped_domain_df['Device_Host_Name_wo_domain_count'] = hostname_dropped_domain_df.groupby(['Device_Host_Name'])['Device_Host_Name'].transform('count')
hostname_dropped_domain_df['Device_Host_Name_w_domain_count'] = hostname_dropped_domain_df.groupby(['Device_Host_Name_w_domain'])['Device_Host_Name_w_domain'].transform('count')


# portshow_aggregated_df['Device_Host_Name_wo_domain_count'] = portshow_aggregated_df.groupby(['Device_Host_Name'])['Device_Host_Name'].transform('count')
# portshow_aggregated_df['Device_Host_Name_w_domain_count'] = portshow_aggregated_df.groupby(['Device_Host_Name_w_domain'])['Device_Host_Name_w_domain'].transform('count')

portshow_aggregated_df = dfop.dataframe_fillna(portshow_aggregated_df, hostname_dropped_domain_df, 
                                               join_lst=columns, filled_lst=['Device_Host_Name_wo_domain_count', 'Device_Host_Name_w_domain_count'])

mask_domain_name_freeze = portshow_aggregated_df['Device_Host_Name_wo_domain_count'] > portshow_aggregated_df['Device_Host_Name_w_domain_count']




portshow_aggregated_df.loc[mask_domain_name_freeze, 'Domain_drop_status'] = 'domain_freezed'
portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name'] = portshow_aggregated_df.loc[mask_domain_name_freeze, 'Device_Host_Name_w_domain']