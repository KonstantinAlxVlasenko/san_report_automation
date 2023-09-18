# -*- coding: utf-8 -*-
"""
Created on Wed Sep 13 14:19:28 2023

@author: kavlasenko
"""


import os
import warnings
import numpy as np
import re
import warnings

import pandas as pd
script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop
import general_re_module as reop



# # DataLine VC67
# db_path = r"D:\Documents\01.CUSTOMERS\DataLine\SAN NORD\VC67\JUL2023\database_DataLine_VC6-VC7"

# db_file = r"DataLine_VC6-VC7_analysis_database.db"
# data_names = ['portshow_aggregated', 'switch_params_aggregated']
# data_lst = dfop.read_database(db_path, db_file, *data_names)
# data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
# portshow_aggregated_df, switch_params_aggregated_df, *_ = data_lst


# db_file = r"DataLine_VC6-VC7_collection_database.db"
# data_names = ['licenseport']
# data_lst = dfop.read_database(db_path, db_file, *data_names)
# data_lst = dfop.verify_read_data(20, data_names, *data_lst,  show_status=True)
# licenseport_df, *_ = data_lst




# pattern_names = ['^(.+?)\: +(.+)',
#     ]

pattern_names = ['sscmd_chassisshow', 'chassisshow_slot_unit', 'chassis_param',
                 'sscmd_chassisshow_unit_end', 'chassis_factory_serial',
                 'switchcmd_end'
    ]

patterns = ['^(CHASSISCMD|SWITCHCMD)? *(/fabos/cliexec/)?chassisshow:',
            '^(.+?) +(Slot|Unit): +(\d+)', '^([\w .-]+) ?(?:=|:) ?([-\w. :;/]+)$',
            'Slot|Unit|Chassis Factory Serial Num|(real\s[\w.]+)|(\*\* SS CMD END \*\*)',
            'Chassis Factory Serial.+?: +(.+)',
            '^(real [\w.]+)|(\*\* SS CMD END \*\*)$'
            ]
    
patterns = [re.compile(fr"{element}", re.IGNORECASE) for element in patterns]
pattern_dct = dict(zip(pattern_names, patterns))

max_title = 50


switch_config = r'D:\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\APR21\santoolbox_parsed_data_MegafonMSK\supportshow\swDC_141def-10.99.248.141-S2cp-202103261058-SupportShow.txt'


# search control dictionary. continue to check file until all parameters groups are found

collected = {'chassisshow': False}

# initialize structures to store collected data for current storage
# dictionary to store all DISCOVERED parameters



 


with open(switch_config, encoding='utf-8', errors='ignore') as file:
    # line = file.readline()
    # check file until all groups of parameters extracted
    while not all(collected.values()):
        line = file.readline()
        if not line:
            break
        if re.search(pattern_dct['sscmd_chassisshow'], line) and not collected['chassisshow']:
            print('sscmd_chassisshow')
            collected['chassisshow'] = True
            while not re.search(pattern_dct['switchcmd_end'], line):
                if re.search(pattern_dct['chassisshow_slot_unit'], line):
                    chassisshow_dct = {}
                    slot_unit_lst = reop.line_to_list(pattern_dct['chassisshow_slot_unit'], line)
                    print(slot_unit_lst)
                    line = file.readline()
                    line = reop.extract_key_value_from_line(chassisshow_dct, pattern_dct, line, file, 
                                                    extract_pattern_name='chassis_param', 
                                                    stop_pattern_name='sscmd_chassisshow_unit_end', first_line_skip=False)
                    print(chassisshow_dct)
                elif re.search(pattern_dct['chassis_factory_serial'], line):
                    chassis_serial = re.search(pattern_dct['chassis_factory_serial'], line).group(1)
                    line = file.readline()
                    print(chassis_serial)
                    
                else:
                    line = file.readline()
        
        # else:
        #     line = file.readline()
        #     if not line:
        #         break
            
            
            

        
    


