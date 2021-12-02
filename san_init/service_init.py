import os.path
import sys
from datetime import date
import pandas as pd
# from common_operations_filesystem import check_valid_path, create_folder
# from common_operations_servicefile import dataframe_import
# from common_operations_dataframe import dct_from_dataframe, series_from_dataframe

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def service_initialization():

    # initial max filename title for status represenation
    start_max_title = 60

    # get report entry values from report file
    report_entry_sr = import_titles_and_folders(start_max_title)

    # customer_name, project_title, project_folder, ssave_folder,  *_ =  report_entry_lst

    # find longest file_name for display status purposes
    max_title = find_max_title(report_entry_sr['supportsave_folder'])

    print('\n\n')
    info = f"{report_entry_sr['project_title'].upper()}. CUSTOMER {report_entry_sr['customer_name']}."
    print(info.center(max_title + 80, '.'))
    print('\n')

    # create folders in SAN Assessment project folder and add it to the report_entry_sr
    create_service_folders(report_entry_sr, max_title)

    # creates list with report service values
    report_constant_lst = [report_entry_sr['customer_name'], 
                            report_entry_sr['current_report_folder'], 
                            report_entry_sr['database_folder'], 
                            max_title]    

    project_steps_df, report_headers_df, software_path_df, report_steps_dct = import_service_dataframes(max_title)


    report_creation_info_lst = [report_constant_lst, report_steps_dct, report_headers_df]
    # future replace to [report_entry_sr, project_steps_df, report_headers_df, max_title]

    return report_entry_sr, report_creation_info_lst, project_steps_df, software_path_df


def import_service_dataframes(max_title):
    """Function to import tables related information: table name, type, description, order in report,
    headers, correlation, force_extract and export to excel keys."""

    print(f'\n\nPREREQUISITES 1. IMPORTING STEPS AND TABLES RELATED INFORMATION\n')

    project_steps_df = sfop.dataframe_import('service_tables', max_title, init_file = 'report_info.xlsx')
    numeric_columns = ['export_to_excel', 'force_extract', 'sort_weight']
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

    report_steps_dct = dfop.dct_from_dataframe(project_steps_df, 'keys', 'export_to_excel', 'force_extract', 'report_type', 'step_info', 'description')

    # Data_frame with report columns
    report_headers_df = sfop.dataframe_import('customer_report', max_title)
    # software path
    software_path_df = sfop.dataframe_import('software', max_title, init_file = 'report_info.xlsx')

    return project_steps_df, report_headers_df, software_path_df, report_steps_dct


def import_titles_and_folders(max_title):
    """
    Function to import entry report values:
    customer_name, project_title, hardware configuration files folders,  
    """

    report_entry_df = sfop.dataframe_import('report', max_title, 'report_info.xlsx', display_status=False)
    report_entry_sr = dfop.series_from_dataframe(report_entry_df, index_column='name', value_column='value')

    entry_lst = ('customer_name', 'project_title', 'project_folder', 'supportsave_folder')
    empty_entry_lst = [entry for entry in  entry_lst if pd.isna(entry)]
    if empty_entry_lst:
        print(f"{', '.join(empty_entry_lst)} {'is' if len(empty_entry_lst)>1 else 'are'} not defined in report_info file.")
        exit()
    
    return report_entry_sr
    
    

def create_service_folders(report_entry_sr, max_title):
    """
    Function to create three folders.
    Folder to save parsed with SANToolbox supportshow data files.
    Folder to save parsed with SANToolbox all others data files.
    Folder to save excel file with parsed configuration data files
    If it is not possible to create any folder script stops.
    """
    
    customer_title = report_entry_sr['customer_name']
    project_path = os.path.normpath(report_entry_sr['project_folder'])

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

    report_entry_sr['parsed_sshow_folder'] = santoolbox_parsed_sshow_path
    report_entry_sr['parsed_other_folder'] = santoolbox_parsed_others_path
        
    # define folder san_assessment_report to save excel file with parsed configuration data
    san_assessment_report_dir = f'report_{customer_title}_' + current_date
    san_assessment_report_path = os.path.join(os.path.normpath(project_path), san_assessment_report_dir)   
    fsop.create_folder(san_assessment_report_path, max_title)
    report_entry_sr['current_report_folder'] = san_assessment_report_path
    
    # define folder to save obects extracted from configuration files
    database_dir = f'database_{customer_title}'
    database_path = os.path.join(os.path.normpath(project_path), database_dir)
    fsop.create_folder(database_path, max_title)
    report_entry_sr['database_folder'] = database_path

    return report_entry_sr


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
            elif file.endswith("AMS_MAPS_LOG.txt.gz"):
                filename_size.append(len(file))
    if not filename_size:
        print('\nNo confgiguration data found')
        sys.exit()
              
    return max(filename_size)