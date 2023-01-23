import os
import re
import shutil
import subprocess
import sys

import pandas as pd

import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT

from .sshow_build import build_sshow_file, export_single_section_file
from .sshow_stats import (count_ssave_section_files_stats,
                                   filter_fault_sections,
                                   update_ssave_sections_stats)


def santoolbox_process(all_files_to_parse_lst, path_to_move_parsed_sshow, path_to_move_parsed_others, software_path_sr, ssave_sections_stats_df, max_title):    
    """Check through unparsed list for configuration data sets for each switch.  
    Unparsed list format  [[unparsed sshow, (unparsed ams_maps, unparsed amps_maps ...)], []]"""
    
    santoolbox_path = software_path_sr['santoolbox']


    # list to save parsed configuration data files with full path
    parsed_files_lst = []
    # list to save parsed configuration data files names
    parsed_filenames_lst = []
    # number of configuration data sets (one set is config files for one switch)
    config_set_num = len(all_files_to_parse_lst)

    # 
    santoolbox_run_status_lst = []
    
    print('\n\nPREREQUISITES 4. PROCESSING SUPPORTSAVE FILES WITH SANTOOLBOX\n')
    print(f'Parsed configuration files are moved to\n{os.path.dirname(path_to_move_parsed_sshow)}\n')
    
    # going throgh each configuration set (switch) in unpased list
    for i,switch_files_to_parse_lst in enumerate(all_files_to_parse_lst):
        
        # extracts switchname from supportshow filename
        switchname = re.search(r'^([\w-]+)(-\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})?-S\d(cp)?-\d+.SSHOW_SYS.(txt.)?gz$', 
                                os.path.basename(switch_files_to_parse_lst[0])).group(1)
        # number of ams_maps_log files in current configuration set (switch)
        ams_maps_config_num = len(switch_files_to_parse_lst[1])
        print(f'[{i+1} of {config_set_num}]: {switchname}. Number of configs: {ams_maps_config_num+1} ...')
        
        # calling santoolbox_parser function which parses SHOW_SYS file with SANToolbox
        parsed_sshow_file = santoolbox_parser(switch_files_to_parse_lst[0], path_to_move_parsed_sshow, santoolbox_path, max_title, santoolbox_run_status_lst)
        
        parsed_sshow_file = pull_switch_configuration_file_(switch_files_to_parse_lst[0], 
                                                            path_to_move_parsed_sshow, 
                                                            ssave_sections_stats_df, max_title, santoolbox_run_status_lst)
        
        parsed_sshow_filename = os.path.basename(parsed_sshow_file)
        
        # tmp lists to save parsed AMS_MAPS_LOG filenames and filepaths
        ams_maps_files_lst_tmp = []
        ams_maps_filenames_lst_tmp = []
        if ams_maps_config_num > 0:
            for ams_maps_config in switch_files_to_parse_lst[1]:
                # calling santoolbox_parser function which parses AMS_MAPS_LOG file with SANToolbox
                parsed_amsmaps_file = santoolbox_parser(ams_maps_config, path_to_move_parsed_others, santoolbox_path, max_title, santoolbox_run_status_lst)
                parsed_amsmaps_file = pull_switch_configuration_file_(ams_maps_config, 
                                                    path_to_move_parsed_others, 
                                                    ssave_sections_stats_df, max_title, santoolbox_run_status_lst)

                # append filenames and filepaths to tmp lists
                ams_maps_files_lst_tmp.append(parsed_amsmaps_file)
                ams_maps_filenames_lst_tmp.append(os.path.basename(parsed_amsmaps_file))
        else:
            info = ' '*LEFT_INDENT + 'No AMS_MAPS configuration found.'
            print(info, end =" ")
            meop.status_info('skip', max_title, len(info))
            # ams_maps_files_lst_tmp.append(None)
            # ams_maps_filenames_lst_tmp.append(None)
            ams_maps_files_lst_tmp = None
            ams_maps_filenames_lst_tmp = None
        
        # append parsed configuration data filenames and filepaths to the final lists 
        parsed_files_lst.append([switchname, parsed_sshow_file, ams_maps_files_lst_tmp])
        # ams_maps_filenames_str = ', '.join(ams_maps_filenames_lst_tmp) if ams_maps_filenames_lst_tmp else None
        # parsed_filenames_lst.append([switchname, parsed_sshow_filename, ', '.join(ams_maps_filenames_lst_tmp)])
        parsed_filenames_lst.append([switchname, parsed_sshow_filename, ams_maps_filenames_lst_tmp])    
    print('\n')
    return parsed_files_lst, parsed_filenames_lst, santoolbox_run_status_lst


def santoolbox_parser(file, path_to_move_parsed_data, santoolbox_path, max_title, santoolbox_run_status_lst):
    """Function to process unparsed ".SSHOW_SYS.txt.gz" and  "AMS_MAPS_LOG.txt.gz" files with SANToolbox."""

    # split filepath to directory and filename
    filedir, filename = os.path.split(file)
    if filename.endswith(".SSHOW_SYS.txt.gz"):
        ending1 = '.SSHOW_SYS.txt.gz'
        ending2 = '-SupportShow.txt'
        option = 'r'
    elif filename.endswith(".SSHOW_SYS.gz"):
        ending1 = '.SSHOW_SYS.gz'
        ending2 = '-SupportShow.txt'
        option = 'r'
    elif filename.endswith('AMS_MAPS_LOG.txt.gz'):     
        ending1 = 'AMS_MAPS_LOG.txt.gz'
        ending2 = 'AMS_MAPS_LOG.txt.gz.txt'
        option = 'd'
    elif filename.endswith('AMS_MAPS_LOG.tar.gz'):     
        ending1 = 'AMS_MAPS_LOG.tar.gz'
        ending2 = 'AMS_MAPS_LOG.tar.gz.txt'
        option = 'd'
    
    # information string
    info = ' '*LEFT_INDENT+f'{filename} processing'
    print(info, end =" ")
    
    # after parsing filename is changed
    filename = filename.replace(ending1, ending2)
    
    # run SANToolbox if only config file wasn't parsed before 
    if not os.path.isfile(os.path.join(path_to_move_parsed_data, filename)):
        try:
            subprocess.call(f'"{santoolbox_path}" -{option} "{file}"', shell=True)
        except subprocess.CalledProcessError:
            santoolbox_run_status_lst.append(meop.status_info('fail', max_title, len(info)))
        except OSError:
            meop.status_info('fail', max_title, len(info))
            print('SANToolbox program is not found ')
            sys.exit()
        else:
            santoolbox_run_status_lst.append(meop.status_info('ok', max_title, len(info)))
        
        # moving file to destination config folder
        info = ' '*LEFT_INDENT + f'{filename} moving'
        print(info, end =" ") 
        try:
            shutil.move(os.path.join(filedir, filename),path_to_move_parsed_data)
        except shutil.Error:
            meop.status_info('fail', max_title, len(info))
            sys.exit()
        except FileNotFoundError:
            meop.status_info('fail', max_title, len(info))
            print('The system cannot find the file specified.\nCHECK that SANToolbox is CLOSED before run the script.')
            sys.exit()
        else:
            meop.status_info('ok', max_title, len(info))
    else:
        meop.status_info('skip', max_title, len(info))
    return os.path.normpath(os.path.join(path_to_move_parsed_data, filename))