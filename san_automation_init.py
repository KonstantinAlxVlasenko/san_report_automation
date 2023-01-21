import os.path
import sys
from datetime import date
import pandas as pd

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop
import os


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

    project_steps_df, io_data_names_df, report_headers_df, software_path_sr, san_graph_grid_df, san_topology_constantants_sr = import_service_dataframes(max_title)
    project_constants_lst = [project_steps_df, max_title, io_data_names_df, report_requisites_sr, report_headers_df]
    return project_constants_lst, software_path_sr, san_graph_grid_df, san_topology_constantants_sr


def import_service_dataframes(max_title):
    """Function to import tables related information: table name, type, description, order in report,
    headers, correlation, force_extract and export to excel keys."""

    print(f'\n\nPREREQUISITES 1. IMPORTING STEPS AND TABLES RELATED INFORMATION\n')

    project_steps_df = import_project_steps(max_title)

    # DataFrame with report column names
    report_headers_df = sfop.dataframe_import('customer_report', max_title)
    # software path
    software_path_df = sfop.dataframe_import('software', max_title, init_file = 'report_info.xlsx')
    software_path_df['path'] = software_path_df.apply(lambda series: software_path(series), axis=1)
    software_path_sr = dfop.series_from_dataframe(software_path_df, index_column='name', value_column='path')
    # DataFrame with input and output data names of each module
    io_data_names_df = sfop.dataframe_import('in_out_data_names', max_title, init_file='report_info.xlsx')
    # constants to create SAN topology in Visio
    san_topology_constantants_df = sfop.dataframe_import('san_topology_constants', max_title, init_file='report_info.xlsx')
    san_topology_constantants_sr = dfop.series_from_dataframe(san_topology_constantants_df, index_column='name', value_column='value')
    # import data with switch models, firmware and etc
    san_graph_grid_df = sfop.dataframe_import('san_graph_grid', max_title)
    return project_steps_df, io_data_names_df, report_headers_df, software_path_sr, san_graph_grid_df, san_topology_constantants_sr


def import_project_steps(max_title):
    """Function to import project steps details (step description, 'export_to_excel', 'force_run' keys 
    and sort weights for sorting sheets in report) """

    project_steps_df = sfop.dataframe_import('project_steps', max_title, init_file='report_info.xlsx')
    
    # replace all nan values with 0 and all notna values (except 0) with 1
    project_steps_df.fillna({'export_to_excel': 0, 'force_run': 0, 'sort_weight': 1, 
                            'report_type': 'unknown', 'module_info': '-', 'step_info': '-', 'description': '-', }, inplace=True)
    for column in ['export_to_excel', 'force_run']: 
        mask_force_run = project_steps_df[column].notna() & ~project_steps_df[column].isin([0, '0'])
        project_steps_df.loc[mask_force_run, column] = 1

    numeric_columns = ['export_to_excel', 'force_run', 'sort_weight']
    project_steps_df[numeric_columns] = project_steps_df[numeric_columns].apply(pd.to_numeric, errors='ignore')

    info = "Global export report key"
    print(info, end =" ") 

    # check if global export report key is set
    mask_global_report = project_steps_df['keys'] == 'report'
    if project_steps_df.loc[mask_global_report, 'export_to_excel'].values != 0:
        mask_report_type = project_steps_df['report_type'] == 'report'
        project_steps_df.loc[mask_report_type, 'export_to_excel'] = 1
        meop.status_info('on', max_title, len(info))
    else:
        meop.status_info('off', max_title, len(info))
    
    project_steps_columns =  ['keys', 'report_type', 'export_to_excel', 'force_run', 
                                'module_info', 'step_info', 'description', 'sort_weight']
    project_steps_df = project_steps_df[project_steps_columns].copy()
    project_steps_df.set_index('keys', inplace=True)
    return project_steps_df


def software_path(series):
    """Function to find software pathes for san_toolbox and s3mft"""
    
    sw_path = os.path.join(series['directory'], series['file'])
    sw_path = os.path.normpath(sw_path)
    return sw_path


def import_requisites(max_title):
    """Function to import entry report values:
    customer_name, project_title, hardware configuration files folders"""

    report_requisites_df = sfop.dataframe_import('report_requisites', max_title, 'report_info.xlsx', display_status=False)

    report_requisites_sr = dfop.series_from_dataframe(report_requisites_df, index_column='name', value_column='value')


    # fields which shouldn't be empty
    entry_lst = ('customer_name', 'project_title', 'project_folder', 'supportsave_folder')
    empty_entry_lst = [entry for entry in  entry_lst if pd.isna(report_requisites_sr[entry])]

    if empty_entry_lst:
        print(f"{', '.join(empty_entry_lst)} {'are' if len(empty_entry_lst)>1 else 'is'} not defined in the report_info file.")
        exit()
    
    report_requisites_sr['project_title'] = report_requisites_sr['project_title'].replace(' ', '_')
    report_requisites_sr['customer_name'] = report_requisites_sr['customer_name'].replace(' ', '_')

    # normpath all folders in requisites and check if they exist
    for index, _ in report_requisites_sr.items():
        if 'folder' in index:
            if pd.notna(report_requisites_sr[index]):
                report_requisites_sr[index] = os.path.normpath(report_requisites_sr[index])
                fsop.check_valid_path(report_requisites_sr[index])
            else:
                report_requisites_sr[index] = None
    return report_requisites_sr
    

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
    
    print(f'\n\nPREREQUISITES 2. CREATING REQUIRED DIRECTORIES\n')
    print(f'Project folder {project_path}')
    # define folder and subfolders to save configuration data (supportsave and ams_maps files)
    santoolbox_parsed_dir = f'santoolbox_parsed_data_{customer_title}'
    santoolbox_parsed_sshow_path = os.path.join(project_path, santoolbox_parsed_dir, 'supportshow')
    santoolbox_parsed_others_path = os.path.join(project_path, santoolbox_parsed_dir, 'others')
    fsop.create_folder(santoolbox_parsed_sshow_path, max_title)
    fsop.create_folder(santoolbox_parsed_others_path, max_title)

    report_requisites_sr['parsed_sshow_folder'] = santoolbox_parsed_sshow_path
    report_requisites_sr['parsed_other_folder'] = santoolbox_parsed_others_path
        
    # define folder san_assessment_report to save excel file with parsed configuration data
    san_assessment_report_dir = f'report_{customer_title}_' + current_date
    san_assessment_report_path = os.path.join(os.path.normpath(project_path), san_assessment_report_dir)   
    fsop.create_folder(san_assessment_report_path, max_title)
    report_requisites_sr['today_report_folder'] = san_assessment_report_path
    
    # define folder to save obects extracted from configuration files
    database_dir = f'database_{customer_title}'
    database_path = os.path.join(os.path.normpath(project_path), database_dir)
    fsop.create_folder(database_path, max_title)
    report_requisites_sr['database_folder'] = database_path
    return report_requisites_sr


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