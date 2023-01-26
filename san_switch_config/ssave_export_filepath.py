"""Module to find file path to export switch configuration"""


import os
import re


def get_sshow_filepath(sshow_sys_section_file, sshow_dir, pattern_dct):
    """Function takes sshow_sys configuration file and
    directory to export unpacked and concatenated sshow files as input parameters.
    Returns output filepath which is used later to export concatenated sshow files"""
    
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


def get_single_section_output_filepath(input_filepath, output_dir, pattern_dct):
    """Function takes configuration file and directory to export unpacked file as input parameters.
    Returns output filepath which is used later to export file content"""
    
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


