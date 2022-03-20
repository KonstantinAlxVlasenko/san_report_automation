"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""

import san_analysis
import san_init
import san_parser
import utilities.dataframe_operations as dfop


def main():

    project_constants_lst, software_path_sr = san_init.service_initialization()

    parsed_sshow_maps_lst = san_init.switch_config_preprocessing(project_constants_lst, software_path_sr)

    extracted_configuration_lst = san_parser.system_configuration_extract(parsed_sshow_maps_lst, project_constants_lst, software_path_sr)

    san_analysis.system_configuration_analysis(extracted_configuration_lst, project_constants_lst)

    dfop.report_format_completion(project_constants_lst)
    print("\nExecution successfully finished\n")


if __name__ == "__main__":
    main()
    
