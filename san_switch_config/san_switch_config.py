"""Main module to search ssave files in supportsave_folder, redistribute files from the same switch by folders,
create sshow and extract maps files for parsing and analysis in the next modules"""


import sys

import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .ssave_export import export_ssave_files
# from .santoolbox_parser import export_ssave_files, santoolbox_process
from .ssave_search import search_ssave_files


def switch_configuration_discover(project_constants_lst, software_path_sr):
    
    _, max_title, io_data_names_df, report_requisites_sr, *_ = project_constants_lst

    ssave_folder = report_requisites_sr['supportsave_folder']
    sshow_export_folder = report_requisites_sr['sshow_export_folder']
    other_export_folder = report_requisites_sr['other_export_folder']

    # data titles obtained after module execution (output data)
    data_names = dfop.list_from_dataframe(io_data_names_df, 'switch_config_discover')

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    *_, ssave_sections_stats_df = data_lst


    # data imported from init file (regular expression patterns) to extract values from data columns
    pattern_dct, *_ = sfop.regex_pattern_import('ssave', max_title)
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
    # requst to continue program execution
    if any(item in export_status_lst for item in ('FAIL')):
        print('\nSome configs have FAILED export status.')
        query = 'Do you want to continue? (y)es/(n)o: '
        reply = meop.reply_request(query)
        if reply == 'n':
            print("\nExecution successfully finished\n")
            sys.exit()
    return exported_sw_cfg_files_lst



