# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 16:44:29 2022

@author: vlasenko
"""


import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

# import numpy as np

# full MegafonMSK San Assessment
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\MAY21\database_MegafonMSK"
# db_file = r"MegafonMSK_collection_database.db"

# # ROSNEFT
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Rosneft\JAN2022\database_rnc.rosneft"
# db_file = r"rnc.rosneft_collection_database.db"

# # MTS Moscow
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\JAN2022\mts_msc\database_MTS_msk"
# db_file = r"MTS_msk_collection_database.db"

# # MEGAFON PERFOMANCE
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Megafon\MegafonMSK\SAN Assessment\OCT21\database_MegafonMSK"
# db_file = r"MegafonMSK_collection_database.db"

# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Novatek\SAN Assessment\MAR2022\database_Novatek"
# db_file = r"Novatek_collection_database.db"

# NOVATEK SEP2021
db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\Novatek\SAN Assessment\SEP2021\database_Novatek"
db_file = r"Novatek_collection_database.db"

# # IBS APR2021
# db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\IBS\SAN APR21\database_IBS"
# db_file = r"IBS_collection_database.db"  

# data_names = ['chassis_parameters', 'chassis_slots']
# data_names = ['maps_parameters']
# data_names = ['switch_parameters', 'switchshow_ports']
# data_names = ['fabricshow', 'ag_principal']
# data_names = ['portcmd']
# data_names = ['sfpshow', 'portcfgshow']
# data_names = ['fdmi', 'nsshow', 'nscamshow', 'nsshow_dedicated', 'nsportshow']
# data_names = ['isl', 'trunk', 'porttrunkarea', 'lsdb']
# data_names = ['fcrfabric', 'fcrproxydev', 'fcrphydev', 'lsan', 'fcredge', 'fcrresource', 'fcrxlateconfig']
# data_names = ['cfg', 'zone', 'alias', 'cfg_effective', 'zone_effective', 'peerzone', 'peerzone_effective']
# data_names = ['sensor']
# data_names = ['errdump']
# data_names = ['blade_interconnect', 'blade_servers', 'blade_vc']
data_names = ['synergy_interconnect', 'synergy_servers']
# data_names = ['system_3par', 'port_3par', 'host_3par']



data_before_lst = dfop.read_database(db_path, db_file, *data_names)


dfop.load_and_compare(data_before_lst, db_path, db_file, data_names)






data_after_lst = dfop.read_database(db_path, db_file, *data_names)



ns_file_df = data_before_lst[3]

'NO DATA FOUND' in ns_file_df.iloc[0].values


def find_differences(data_before_lst, data_after_lst, data_names):

    for df_name, before_df, after_df in zip(data_names, data_before_lst, data_after_lst):
        df_equality = after_df.equals(before_df)
        print(f"\n{df_name} equals {df_equality}")
        if not df_equality:
            print("   column names are equal: ", before_df.columns.equals(after_df.columns))
            print("      Unmatched columns:")
            for column in before_df.columns:
                if not before_df[column].equals(after_df[column]):
                    print("        ", column)      

ns_df, nscam_df = data_after_lst[1:3]
ns_df.equals(nscam_df)                    

_, after_df, *_ = data_after_lst
import re
pattern = re.compile('[a-z\d:]+')

after_df['chassis_wwn'] = after_df['chassis_wwn'].replace(pattern, '0', regex=True, inplace=True)

after_df['Trunking_GroupNumber'].fillna(method='ffill', inplace=True)

import pandas as pd
trunking_columns = after_df.columns.tolist()
empty_trunk_df = pd.DataFrame(columns=trunking_columns)
empty_trunk_df['Trunking_GroupNumber'].fillna(method='ffill', inplace=True)

tst_dct ={'a': '^char_a', 'b': '^char_b'}
tst_str = f"{tst_dct['a']}|{tst_dct['b']}"


tst_df = pd.DataFrame()

def change_df(df):
    df.loc[0,1] = 'tst1'
    
change_df(tst_df)
import re
line = "LS Attributes: [FID: 120, Base Switch: No, Default Switch: Yes, Ficon Switch: No, Address Mode 0]"
re.findall('^LS +Attributes: +\[FID: +\d+, +(?:([\w]+ +Switch): +(\w+), +){3}(Address +Mode) +(\d+)\]', line)[0]



module_columns_dct = {'enclosurename': 'Enclosure_Name',
                    'enclosure_serialnumber': 'Enclosure_SN',
                    'enclosuretype': 'Enclosure_Type',
                    'baynumber': 'Interconnect_Bay',
                    'interconnectmodel': 'Interconnect_Model',
                    'serialnumber': 'Interconnect_SN',
                    'switchfwversion': 'Interconnect_Firmware',
                    'hostname': 'Interconnect_Name',
                    'switchbasewwn': 'NodeName',
                    'device_location': 'Device_Location'}


server_columns_dct = {'enclosurename': 'Enclosure_Name',
                    'position': 'Enclosure_Slot',
                    'servername': 'Host_Name',
                    'model': 'Device_Model',
                    'serialnumber': 'Device_SN',
                    'oshint': 'Host_OS',
                    'Mezz': 'HBA_Description',
                    'Mezz_WWPN': 'Connected_portWwn',
                    'device_location': 'Device_Location',
                    'componentversion': 'HBA_Firmware'}


for key in module_columns_dct.keys():
    if key in server_columns_dct and module_columns_dct[key] != server_columns_dct[key]:
        print(key)
        
for key in server_columns_dct.keys():
    if key in module_columns_dct and module_columns_dct[key] != server_columns_dct[key]:
        print(key)
        
        
module_columns_dct = {'enclosurename': 'Enclosure_Name',
                    'enclosure_serialnumber': 'Enclosure_SN',
                    'enclosuretype': 'Enclosure_Type',
                    'baynumber': 'Interconnect_Bay',
                    'interconnectmodel': 'Interconnect_Model',
                    'switchfwversion': 'Interconnect_Firmware',
                    'hostname': 'Interconnect_Name',
                    'switchbasewwn': 'NodeName',
                    'device_location': 'Device_Location'}


server_columns_dct = {'enclosurename': 'Enclosure_Name',
                    'position': 'Enclosure_Slot',
                    'servername': 'Host_Name',
                    'model': 'Device_Model',
                    'oshint': 'Host_OS',
                    'Mezz': 'HBA_Description',
                    'Mezz_WWPN': 'Connected_portWwn',
                    'device_location': 'Device_Location',
                    'componentversion': 'HBA_Firmware'}

rename_columns_dct = {**module_columns_dct, **server_columns_dct}

tst_dct = {'a': 1}

tst_dct['b'] = 2 if 'c' in tst_dct else 3