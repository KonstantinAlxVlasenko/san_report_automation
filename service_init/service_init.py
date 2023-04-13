"""Module to initialize san audit service. Import service entry data and create required folders.
Service entry data contains customer name, directories with configuration data to analyze and 
additional files for audit (visio template, device rack locations"""


import os
import sys
from datetime import date

from .requisites_import import (import_requisites, import_service_dataframes,
                               validate_device_rack_file)

import utilities.filesystem_operations as fsop


def service_initialization():
    """Function to import project requisites from service files, create project folders"""

    # initial max filename title for status represenation
    start_max_title = 60
    # get report entry values from report file
    report_requisites_sr = import_requisites(start_max_title)

    # find longest file_name for display status purposes
    max_title = find_max_title(report_requisites_sr['supportsave_folder'])

    print('\n\n')
    info = f"{report_requisites_sr['project_title'].upper()}. CUSTOMER {report_requisites_sr['customer_name']}."
    print(info.center(max_title + 80, '.'))
    print('\n')

    # create folders in SAN Assessment project folder and add it to the report_entry_sr
    create_service_folders(report_requisites_sr, max_title)
    # check if device rack file is valid format
    report_requisites_sr['device_rack_path'] = validate_device_rack_file(report_requisites_sr['device_rack_path'], max_title)
    
    project_steps_df, io_data_names_df, report_headers_df, software_path_sr, san_graph_grid_df, san_topology_constantants_sr = import_service_dataframes(max_title)
    project_constants_lst = [project_steps_df, max_title, io_data_names_df, report_requisites_sr, report_headers_df]
    return project_constants_lst, software_path_sr, san_graph_grid_df, san_topology_constantants_sr


def find_max_title(ssave_path):
    """Function to find maximum cinfiguration file length for display puproses"""

    # check if ssave_path folder exist
    fsop.check_valid_path(ssave_path)
    
    # list to save length of config data file names to find max
    # required in order to proper alighnment information in terminal
    filename_size = []

    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(ssave_path):
        for file in files:
            if file.endswith(".SSHOW_SYS.txt.gz") or file.endswith(".SSHOW_SYS.gz"):
                filename_size.append(len(file))
            elif file.endswith("AMS_MAPS_LOG.txt.gz") or file.endswith("AMS_MAPS_LOG.tar.gz"):
                filename_size.append(len(file))
    if not filename_size:
        print('\nNo confgiguration data found')
        sys.exit()  
    return max(filename_size)


def create_service_folders(report_requisites_sr, max_title):
    """
    Function to create three folders.
    Folder to save parsed with SANToolbox supportshow data files.
    Folder to save parsed with SANToolbox all others data files.
    Folder to save excel file with parsed configuration data files
    If it is not possible to create any folder script stops.
    """
    
    customer_title = report_requisites_sr['customer_name']
    project_path = os.path.normpath(report_requisites_sr['project_folder'])

    # check if project folders exist
    fsop.check_valid_path(project_path)
    # current date
    current_date = str(date.today())   
    
    print(f'\n\nPREREQUISITES 1. CREATING REQUIRED DIRECTORIES\n')
    print(f'Project folder {project_path}')
    
    # define folder and subfolders to save configuration data (supportsave and ams_maps files)
    santoolbox_parsed_dir = f'santoolbox_parsed_data_{customer_title}'
    sshow_export_path = os.path.join(project_path, santoolbox_parsed_dir, 'supportshow')
    other_export__path = os.path.join(project_path, santoolbox_parsed_dir, 'others')
    fsop.create_folder(sshow_export_path, max_title)
    fsop.create_folder(other_export__path, max_title)

    report_requisites_sr['sshow_export_folder'] = sshow_export_path
    report_requisites_sr['other_export_folder'] = other_export__path
        
    # define folder san_assessment_report to save excel file with parsed configuration data
    san_assessment_report_dir = f'report_{customer_title}_' + current_date
    san_assessment_report_path = os.path.join(os.path.normpath(project_path), san_assessment_report_dir)   
    # TO_REMOVE report folder is created if any excel file need to be saved
    # fsop.create_folder(san_assessment_report_path, max_title)
    report_requisites_sr['today_report_folder'] = san_assessment_report_path
    
    # define folder to save obects extracted from configuration files
    database_dir = f'database_{customer_title}'
    database_path = os.path.join(os.path.normpath(project_path), database_dir)
    fsop.create_folder(database_path, max_title)
    report_requisites_sr['database_folder'] = database_path
    return report_requisites_sr
