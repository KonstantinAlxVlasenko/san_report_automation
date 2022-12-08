import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.regular_expression_operations as reop
from itertools import chain


def visio_diagram_init(san_graph_switch_df, san_graph_sw_pair_df, 
                        san_graph_isl_df, san_graph_npiv_df, 
                        storage_shape_links_df, server_shape_links_df, 
                        san_graph_sw_pair_group_df, 
                        fabric_name_duplicated_sr, fabric_name_dev_sr, 
                        project_constants_lst, 
                        software_path_sr, san_topology_constantants_sr):
    """Function to initialize SAN topology drawing in Visio"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    project_steps_df, max_title, _, report_requisites_sr, *_ = project_constants_lst


    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'visio_diagram')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:
        # current operation information string
        info = f'Creating Visio SAN topology'
        print(info, end =" ") 

        # aggregated DataFrames
        visio_diagram_sr = visio_diagram_aggregated(san_graph_switch_df, san_graph_sw_pair_df, 
                                                    san_graph_isl_df, san_graph_npiv_df, 
                                                    storage_shape_links_df, server_shape_links_df, 
                                                    san_graph_sw_pair_group_df, 
                                                    fabric_name_duplicated_sr, fabric_name_dev_sr, 
                                                    max_title, software_path_sr, san_topology_constantants_sr)
        # after finish display status
        meop.status_info('ok', max_title, len(info))
        # create list with partitioned DataFrames
        data_lst = [visio_diagram_sr]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst) 
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        sensor_aggregated_df, *_ = data_lst
    
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return visio_diagram_sr


def visio_diagram_aggregated(san_graph_switch_df, san_graph_sw_pair_df, 
                            san_graph_isl_df, san_graph_npiv_df, 
                            storage_shape_links_df, server_shape_links_df, 
                            san_graph_sw_pair_group_df, 
                            fabric_name_duplicated_sr, fabric_name_dev_sr, 
                            max_title, software_path_sr, san_topology_constantants_sr):
    
    pass