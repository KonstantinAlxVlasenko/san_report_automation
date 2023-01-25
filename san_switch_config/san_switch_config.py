"""Main module to search ssave files in supportsave_folder, redistribute files from the same switch by folders,
create sshow and extract maps files for parsing and analysis in the next modules"""


import os
import re
import sys
from collections import defaultdict

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
from san_automation_constants import LEFT_INDENT

from .ssave_distribute import distribute_ssave_files
from .ssave_export import export_ssave_files
# from .santoolbox_parser import export_ssave_files, santoolbox_process
from .ssave_search import search_ssave_files


def switch_configuration_discover(project_constants_lst, software_path_sr):
    """Function to discover and export switch configuration files"""
    
    _, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst

    ssave_folder = report_requisites_sr['supportsave_folder']
    sshow_export_folder = report_requisites_sr['sshow_export_folder']
    other_export_folder = report_requisites_sr['other_export_folder']

    # data titles obtained after module execution (output data)
    data_names = dfop.list_from_dataframe(io_data_names_df, 'switch_config_discover')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    *_, ssave_sections_stats_df = data_lst
    
    # flag if first iteration of sshow parsing takes place
    first_run = True if ssave_sections_stats_df is None else False
    # data imported from init file (regular expression patterns) to extract values from data columns
    pattern_dct, *_ = sfop.regex_pattern_import('ssave', max_title)
    
    # check ssave_folder, distribute files by folders, check sshow_sys duplication
    ssave_precheck(ssave_folder, pattern_dct, max_title)
    # search ssave and ams_maps files 
    discovered_sw_cfg_files_lst = search_ssave_files(ssave_folder, pattern_dct, max_title)
    discovered_sw_cfg_files_df, *_ = dfop.list_to_dataframe(['sshow', 'ams_maps'], discovered_sw_cfg_files_lst)

    # parsed_sshow_maps_lst, parsed_sshow_maps_filename_lst, santoolbox_run_status_lst = \
    #     santoolbox_process(unparsed_sshow_maps_lst, sshow_export_folder, other_export_folder, software_path_sr, ssave_sections_stats_df, max_title)

    # export ssave files to text configuration files
    exported_sw_cfg_files_lst, exported_sw_cfg_filenames_lst, ssave_sections_stats_df, export_status_lst = \
        export_ssave_files(discovered_sw_cfg_files_lst, 
                            sshow_export_folder, other_export_folder, 
                            ssave_sections_stats_df, pattern_dct, max_title)

    # export parsed config filenames to DataFrame and saves it to excel file
    exported_sw_cfg_files_df, *_ = dfop.list_to_dataframe(['chassis_name', 'sshow', 'ams_maps'], exported_sw_cfg_filenames_lst)
                                    
    # save files list to database and excel file
    data_lst = [discovered_sw_cfg_files_df, exported_sw_cfg_files_df, ssave_sections_stats_df]
    for df in data_lst[:2]:
        df['ams_maps'] = df['ams_maps'].astype('str')
        df['ams_maps'] = df['ams_maps'].str.strip('[]()')

    dbop.write_database(project_constants_lst, data_names, *data_lst)
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)    
    
    # check if any switch configuration export failed
    validate_sw_cfg_export_status(export_status_lst)
    if first_run:
        # checks if any new switch configuration is exported
        detect_new_sw_cfg_export(export_status_lst, project_constants_lst)
    return exported_sw_cfg_files_lst


def ssave_precheck(ssave_path, pattern_dct, max_title):
    """Function check if ssave_path is valid, 
    distributes ssave files from the same switches by folders,
    validates each folder contains only one instance of sshow_sys Scp
    (only one ssave collection)"""

    print(f'\n\nPREREQUISITES 2. SUPPORTSAVE CONFIGURATION FILES PRECHECK\n')
    print(f'Switch configurations folder \n{ssave_path}\n')

    # check if ssave_path folder exist
    fsop.check_valid_path(ssave_path)
    # rellocate files for each switch in separate folder
    distribute_ssave_files(ssave_path, pattern_dct, max_title)
    # check if any directory contains more then one instance of sshow_sys for the same switch
    verify_sshow_sys_duplication(ssave_path, pattern_dct, max_title)


def verify_sshow_sys_duplication(ssave_path, pattern_dct, max_title):
    """Function to check if there is SSHOW_SYS with the same S#cp in the folder"""

    sshow_sys_section_pattern = pattern_dct['sshow_sys_section']
    sshow_duplicated = False

    for root, _, files in os.walk(ssave_path):
        # dictionary with sshow_sys Scp numbers in the current folder
        scp_dct = defaultdict(int)
        for file in files:
            if re.match(sshow_sys_section_pattern, file):                
                scp_dct[re.search(sshow_sys_section_pattern, file).group(3)] += 1
        # if scp duplication met (folder contains more than one ssave collection)
        multiple_scp_lst = [key for key in scp_dct if scp_dct[key] > 1]
        if multiple_scp_lst:
            info = ' '*LEFT_INDENT + f'Mutltiple SHOW_SYS {", ".join(multiple_scp_lst)} instances in folder {os.path.basename(root)}'
            print(info, end =" ")             
            meop.status_info('fail', max_title, len(info))
            sshow_duplicated = True
    if sshow_duplicated:
        sys.exit()


def validate_sw_cfg_export_status(export_status_lst):
    """Function validates if any switch configuration export failed"""

    # requst to continue program execution if any configuration export failed
    if any(item in export_status_lst for item in ('FAIL')):
        print('\nSome configs have FAILED export status.')
        query = 'Do you want to continue? (y)es/(n)o: '
        reply = meop.reply_request(query)
        if reply == 'n':
            print("\nExecution successfully finished\n")
            sys.exit()


def detect_new_sw_cfg_export(export_status_lst, project_constants_lst):
    """Function checks if any new switch configuration is exported.
    If so then request for complete file parsing and and analysis is shown"""
  
    # if any new configs found on any iterations except first
    if any(item in export_status_lst for item in ('OK')):
        print('\nSome new switch configs found')
        query = 'Do you want to initialize collection and analysis for all switches? (y)es/(n)o: '
        reply = meop.reply_request(query)
        if reply == 'y':
            project_constants_lst[0]['force_run'] = 1
            print("\nAll switches configurations parsing and analysis initialized\n")


