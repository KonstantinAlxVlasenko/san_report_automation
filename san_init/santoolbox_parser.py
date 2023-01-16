import os
import re
import shutil
import subprocess
import sys

import pandas as pd

import utilities.module_execution as meop
from san_automation_constants import LEFT_INDENT

from .sshow_creation import create_sshow_file, export_single_section_file
from .sshow_sections_stats import (count_ssave_section_files_stats,
                                   filter_fault_sections,
                                   update_ssave_sections_stats)


def santoolbox_process(all_files_to_parse_lst, path_to_move_parsed_sshow, path_to_move_parsed_others, software_path_sr, max_title):    
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
        parsed_sshow_filename = os.path.basename(parsed_sshow_file)
        
        # tmp lists to save parsed AMS_MAPS_LOG filenames and filepaths
        ams_maps_files_lst_tmp = []
        ams_maps_filenames_lst_tmp = []
        if ams_maps_config_num > 0:
            for ams_maps_config in switch_files_to_parse_lst[1]:
                # calling santoolbox_parser function which parses AMS_MAPS_LOG file with SANToolbox
                parsed_amsmaps_file = santoolbox_parser(ams_maps_config, path_to_move_parsed_others, santoolbox_path, max_title, santoolbox_run_status_lst)
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


pattern_dct = {'sshow_sys_section': r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$', 
'single_filename': '^(.+?.\.(\w+))\.(?:tar|txt).gz', 
'amps_maps_section': r'^(.+?)\.AMS_MAPS_LOG\.(?:tar|txt).gz'}


def pull_switch_configuration_file(ssave_section_file, output_dir, max_title, export_status_lst):
    """Function to process unparsed ".SSHOW_SYS.txt.gz" and  "AMS_MAPS_LOG.txt.gz" files with SANToolbox."""

    ssave_section_filename = os.path.basename(ssave_section_file)

    # information string
    info = ' '*LEFT_INDENT+f'{ssave_section_filename} processing'
    print(info, end =" ")
    

    if re.search(pattern_dct['sshow_sys_section'], ssave_section_filename):
        exported_switch_config_filepath = get_sshow_filepath(ssave_section_filename, output_dir)
        exported_switch_config_secondary_filepath = ''
        config_type = 'sshow'
    elif re.search(pattern_dct['amps_maps_section'], ssave_section_filename):
        exported_switch_config_filepath = get_single_section_output_filepath(ssave_section_filename, output_dir)
        exported_switch_config_secondary_filepath = get_single_section_output_secondary_filepath(ssave_section_filename, output_dir)
        config_type = 'maps'
    else:
        meop.status_info('unknown', max_title, len(info))
        return None

    if os.path.isfile(exported_switch_config_filepath):
        meop.status_info('skip', max_title, len(info))
        return exported_switch_config_filepath
    elif os.path.isfile(exported_switch_config_secondary_filepath):
        meop.status_info('skip', max_title, len(info))
        return exported_switch_config_secondary_filepath
        

    if config_type == 'sshow':

        ssave_sections_stat_columns = ['directory_path', 'directory_name', 'section_name', 
                                        'hight_priority', 'ssave_filename', 'count', 'size_KB',  
                                        'ssave_basename', 'sshow_filename']

        sw_ssave_sections_stat_df, ssave_sections_stat_current_df = \
            count_ssave_section_files_stats(sshow_sys_section_file=ssave_section_file, sshow_file=exported_switch_config_filepath)
        sw_fault_sections_df = filter_fault_sections(sw_ssave_sections_stat_df)
        ssave_sections_stat_df = pd.DataFrame(columns=ssave_sections_stat_columns)
        ssave_sections_stat_df = update_ssave_sections_stats(ssave_sections_stat_df, ssave_sections_stat_current_df)
        create_sshow_file(ssave_sections_stat_current_df, exported_switch_config_filepath)
        meop.status_info('ok', max_title, len(info))
    elif config_type == 'maps':
        export_single_section_file(input_filepath=ssave_section_file, output_filepath=exported_switch_config_filepath)
        meop.status_info('ok', max_title, len(info))

    return exported_switch_config_filepath


def get_single_section_output_filepath(input_filepath, output_dir):
    """Function takes configuration file and directory to export unpacked file as input parameters.
    Returns output filepath which is used later to export file content"""
    
    # input_filename_pattern = '^(.+?.\.(\w+))\.(?:tar|txt).gz'
    input_filename_pattern = pattern_dct['single_filename']
    output_filename = re.search(input_filename_pattern, os.path.basename(input_filepath)).group(1) + '.txt'
    output_filepath = os.path.normpath(os.path.join(output_dir, output_filename))
    return output_filepath


def get_single_section_output_secondary_filepath(input_filepath, output_dir):
    """Function takes configuration file and directory to export unpacked file as input parameters.
    Returns output filepath which is used later to export file content"""

    output_filename = os.path.basename(input_filepath) + '.txt'
    output_filepath = os.path.normpath(os.path.join(output_dir, output_filename))
    return output_filepath


def get_sshow_filepath(sshow_sys_section_file, sshow_dir):
    """Function takes sshow_sys configuration file and
    directory to export unpacked and concatenated sshow files as input parameters.
    Returns output filepath which is used later to export concatenated sshow files"""
    
    # sshow_name_pattern = r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$'
    sshow_name_pattern = pattern_dct['sshow_sys_section']
    # get filename from absolute filepath
    sshow_sys_section_filename = os.path.basename(sshow_sys_section_file)
    # drop characters after sshow collection datetime
    sshow_filename = re.search(sshow_name_pattern, sshow_sys_section_filename).group(1)
    # create filename used to export sshow sections
    sshow_filename = sshow_filename + '-SupportShow.txt'
    # combine output directory and sshow filename
    sshow_filepath = os.path.normpath(os.path.join(sshow_dir, sshow_filename))
    return sshow_filepath