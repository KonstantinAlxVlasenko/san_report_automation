"""Module to extract Synergy system information from meddler configuration file"""


import os

import numpy as np
import pandas as pd
import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .synergy_sections import server_mezz_extract, interconnect_module_extract


def synergy_system_extract(project_constants_lst):
    """Function to extract Synergy systems information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst
    synergy_folder = report_requisites_sr['synergy_meddler_folder']

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'synergy_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    if force_run:
        # data imported from init file to extract values from config file
        pattern_dct, *_ = sfop.regex_pattern_import('synergy', max_title)

        synergy_module_aggregated_df = pd.DataFrame()
        synergy_servers_aggregated_df = pd.DataFrame()

        if synergy_folder:
            print('\nEXTRACTING SYNERGY SYSTEM INFORMATION ...\n')

            # collects files in folder with xlsm extension
            synergy_config_lst = fsop.find_files(synergy_folder, max_title, filename_extension='xlsm')
            # number of files to check
            configs_num = len(synergy_config_lst)

            if configs_num:
                for i, synergy_config in enumerate(synergy_config_lst):
                    # file name with extension
                    configname_wext = os.path.basename(synergy_config)
                    # remove extension from filename
                    configname, _ = os.path.splitext(configname_wext)

                    # current operation information string
                    info = f'[{i+1} of {configs_num}]: {configname} system.'
                    print(info, end =" ")

                    # interconnect modules information
                    synergy_module_df = interconnect_module_extract(synergy_config)
                    synergy_module_aggregated_df = pd.concat([synergy_module_aggregated_df, synergy_module_df], ignore_index=True)
                    # server and mezzanine information
                    synergy_servers_df = server_mezz_extract(synergy_config, pattern_dct)
                    synergy_servers_aggregated_df = pd.concat([synergy_servers_aggregated_df, synergy_servers_df], ignore_index=True)
                    
                    if not all((synergy_servers_df.empty, synergy_module_df.empty)):
                        meop.status_info('ok', max_title, len(info))
                    else:
                        meop.status_info('no data', max_title, len(info))

                # lower case wwn, rename columns and replace blanks with nan
                modify_df_representation(synergy_module_aggregated_df, pattern_dct)
                modify_df_representation(synergy_servers_aggregated_df, pattern_dct)
        else:
            # current operation information string
            info = f'Collecting synergy details'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
        data_lst = [synergy_module_aggregated_df, synergy_servers_aggregated_df]
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        synergy_module_aggregated_df, synergy_servers_aggregated_df = data_lst

    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return synergy_module_aggregated_df, synergy_servers_aggregated_df


def modify_df_representation(aggregated_df, pattern_dct):
    """Function to modify dataframe representation. 
    Make wwn columns lower case, replace blanks with nan and rename columns"""
    
    rename_columns = {'enclosurename': 'Enclosure_Name',
                        'enclosure_serialnumber': 'Enclosure_SN',
                        'enclosuretype': 'Enclosure_Type',
                        'baynumber': 'Interconnect_Bay',
                        'interconnectmodel': 'Interconnect_Model',
                        'switchfwversion': 'Interconnect_Firmware',
                        'hostname': 'Interconnect_Name',
                        'switchbasewwn': 'NodeName',
                        'device_location': 'Device_Location',
                        'position': 'Enclosure_Slot',
                        'servername': 'Host_Name',
                        'model': 'Device_Model',
                        'oshint': 'Host_OS',
                        'Mezz': 'HBA_Description',
                        'Mezz_WWPN': 'Connected_portWwn',
                        'componentversion': 'HBA_Firmware'}

    # synergy_module or synergy_server DataFrames
    for wwn_column, serial_column in [('switchbasewwn', 'Interconnect_SN'), ('Mezz_WWPN', 'Device_SN')]:
        if wwn_column in aggregated_df.columns:
            rename_columns['serialnumber'] = serial_column
            if aggregated_df[wwn_column].notna().any():
                aggregated_df[wwn_column] = aggregated_df[wwn_column].str.lower()
        
    aggregated_df.rename(columns=rename_columns, inplace=True)
    aggregated_df.replace(pattern_dct['none_blank_line'], value=np.nan, regex=True, inplace=True)
