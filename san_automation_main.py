"""
Main module to run
Define folders where configuration data is stored
Define SAN Assessment project folder to store all data
Define customer name
File report_info_xlsx
"""

import sys

import san_analysis
import san_init
import san_parser
import san_topology
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop


def main():

    # project executation related service information
    project_constants_lst, software_path_sr, san_graph_grid_df, san_topology_constantants_sr = san_init.service_initialization()
    # supportsave parsing
    parsed_sshow_maps_lst = san_init.switch_config_preprocessing(project_constants_lst, software_path_sr)



    # extract information from configuration files
    extracted_configuration_lst = san_parser.system_configuration_extract(parsed_sshow_maps_lst, project_constants_lst, software_path_sr)
    # perform analysis of extracted configuraion data
    analyzed_configuration_lst = san_analysis.system_configuration_analysis(extracted_configuration_lst, project_constants_lst)
    # sort sheets and table of contents in excel report
    dfop.report_format_completion(project_constants_lst)
    # create san topology in Visio
    san_topology.visualize_san_topology(analyzed_configuration_lst, project_constants_lst, software_path_sr, san_graph_grid_df, san_topology_constantants_sr)

    print("\nExecution successfully finished\n")


if __name__ == "__main__":
    main()
    
