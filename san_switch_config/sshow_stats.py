"""Module to count sshow sections which are used to build supportshow files for switches in san (sshow section statistics)"""

import os
import re

import numpy as np
import pandas as pd

SSHOW_SECTIONS = ['SSHOW_PLOG', 'SSHOW_OS', 'SSHOW_EX', 'SSHOW_FABRIC', 
                  'SSHOW_CONDB', 'SSHOW_SERVICE', 'SSHOW_SEC', 'SSHOW_NET', 
                  'SSHOW_SYS', 'SSHOW_FICON', 'SSHOW_ISWITCH', 'SSHOW_ISCSI', 'SSHOW_ASICDB', 
                  'SSHOW_AG', 'SSHOW_FCIP', 'SSHOW_APM', 'SSHOW_AMP', 'SSHOW_CRYP', 'SSHOW_PORT', 
                  'SSHOW_DCEHSL', 'SSHOW_FLOW']

HIGH_PRIORITY_SECTIONS = ['SSHOW_EX', 'SSHOW_FABRIC', 'SSHOW_SERVICE', 
                          'SSHOW_SEC', 'SSHOW_SYS', 'SSHOW_AG', 'SSHOW_PORT']


def get_sshow_basename(sshow_sys_section_file, pattern_dct):
    """Function returns SSHOW_SYS file basename (hostname, ip address, S#cp) 
    to filter sshow files related to sshow_sys_section_file (same S#cp)"""
    
    # sshow_name_pattern = r'((.+S\d+(?:cp)?)-\d+)\.SSHOW_SYS.(?:txt.)?gz$'
    sshow_name_pattern = pattern_dct['sshow_sys_section']
    # get filename from absolute filepath
    sshow_sys_section_filename = os.path.basename(sshow_sys_section_file)
    # extract hostname, ip address, S#cp
    sshow_section_basename = re.search(sshow_name_pattern, sshow_sys_section_filename).group(2)
    return sshow_section_basename
    

def count_ssave_section_files_stats(sshow_sys_section_file, pattern_dct):
    """Function counts statistics for the sections from  SSHOW_SECTIONS list 
    with the same basename as sshow_sys_section_file in directory where sshow_sys_section_file is located"""
    
    # section statistics dataframe to filter failed sections
    sw_ssave_sections_stats_df = pd.DataFrame(index=SSHOW_SECTIONS, columns=['Quantity', 'Size_KB', 'Priority'])
    sw_ssave_sections_stats_df.index.name = 'Section_name'
    # create pattern to filter required sshow files
    sshow_sys_section_basename = get_sshow_basename(sshow_sys_section_file, pattern_dct)
    ssave_sections_dir = os.path.dirname(sshow_sys_section_file)
    pattern = fr"{sshow_sys_section_basename}-\d+\.({'|'.join(SSHOW_SECTIONS)})\.(?:txt.)?gz$"
    # tmp
    ssave_sections_stats_columns = ['directory_path', 'directory_name', 'section_name', 
                                    'priority', 'ssave_filename', 'count', 'size_KB',  
                                    'ssave_basename']
    # section statistics dataframe used to concatenate sshow files
    ssave_sections_stats_current_df = pd.DataFrame(columns=ssave_sections_stats_columns)

    for file in os.listdir(ssave_sections_dir):
        # skip if file is a directory
        if not os.path.isfile(os.path.join(ssave_sections_dir, file)):
            continue
        # if file is related to sshow_sys_section_file basename
        if re.search(pattern, file):
            section_name = re.search(pattern, file).group(1)
            # count files with the same basename and section_name
            if pd.isna(sw_ssave_sections_stats_df.loc[section_name, 'Quantity']):
                sw_ssave_sections_stats_df.loc[section_name, 'Quantity'] = 1
            else:
                sw_ssave_sections_stats_df.loc[section_name, 'Quantity'] += 1
            # print(ssave_sections_stats_df.loc[section_name, 'Quantity'])
            # sshow section file size in KB
            sw_ssave_sections_stats_df.loc[section_name, 'Size_KB'] = round(os.path.getsize(os.path.join(ssave_sections_dir, file)) / 1024, 2)
            # section_names used to san audit have priority 1
            priority = 1 if section_name in HIGH_PRIORITY_SECTIONS else np.nan
            sw_ssave_sections_stats_df.loc[section_name, 'Priority'] = priority
            # print(ssave_sections_stats_df.loc[section_name, 'Size_KB'])
            # fill values for the ssave section file in dataframe used to concatenate files later
            ssave_sections_stats_current_df.loc[len(ssave_sections_stats_current_df)] = [ssave_sections_dir, os.path.basename(ssave_sections_dir),
                                                                                   section_name, priority, file, 
                                                                                   sw_ssave_sections_stats_df.loc[section_name, 'Quantity'],
                                                                                   sw_ssave_sections_stats_df.loc[section_name, 'Size_KB'],
                                                                                   sshow_sys_section_basename]
    # add empty rows for the sections for which files are absent in the directory
    absent_sections = [section for section in sw_ssave_sections_stats_df.index 
                        if not section in ssave_sections_stats_current_df['section_name'].values]
    absent_sections_proirity = [1  if section in HIGH_PRIORITY_SECTIONS else None for section in absent_sections ]

    for section, priority in zip(absent_sections, absent_sections_proirity):
        ssave_sections_stats_current_df.loc[len(ssave_sections_stats_current_df), ['directory_path', 'directory_name', 'section_name', 'priority']] = \
            (ssave_sections_dir, os.path.basename(ssave_sections_dir), section, priority)
        # add priority for absent sections
        sw_ssave_sections_stats_df.loc[section, 'Priority'] = priority
    return sw_ssave_sections_stats_df, ssave_sections_stats_current_df


def filter_fault_sections(sw_ssave_sections_stats_df):
    """Function filters failed sections (sections with high piority with absent files or files with zero size,
    section with any piority and multiple files)"""
    
    # zero or multiple files for section in the folder
    mask_quantity_failure = sw_ssave_sections_stats_df['Quantity'] != 1
    # sections with high priority
    mask_high_priority = sw_ssave_sections_stats_df['Priority'] == 1
    # zero size section files
    mask_size_zero = sw_ssave_sections_stats_df['Size_KB'] == 0
    # multiple files for section
    mask_multiple_files = sw_ssave_sections_stats_df['Quantity'] > 1
    mask_failure = (mask_high_priority & mask_quantity_failure) | (mask_high_priority & mask_size_zero) | mask_multiple_files
    # sections with warnings
    sw_fault_sections_df = sw_ssave_sections_stats_df.loc[mask_failure]
    # warning flag if any section have multiple files
    multiple_sshow_section_files_warning_on = mask_multiple_files.any()
    return sw_fault_sections_df, multiple_sshow_section_files_warning_on


def update_ssave_sections_stats(ssave_sections_stats_df, ssave_sections_stats_current_df):
    """Function updates dataframe with statistics for all directories with ssave files.    
    It drops rows with old statistics for the directory for which 
    new sssave_sections_stats_current_df statistic is added """
    
    # drop rows for the directory for which statistics is updated
    mask_dropped_dirs = ssave_sections_stats_df['directory_path'].isin(ssave_sections_stats_current_df['directory_path'].unique())
    ssave_sections_stats_df.drop(ssave_sections_stats_df.index[mask_dropped_dirs], inplace = True)
    # add new statistics
    ssave_sections_stats_df = pd.concat([ssave_sections_stats_df, ssave_sections_stats_current_df])
    return ssave_sections_stats_df