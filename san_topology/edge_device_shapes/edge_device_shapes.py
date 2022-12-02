"""Module to generate switch, VC modules and interconnect link shapes. 
These shape tables are used to create SAN topology in Visio."""

import pandas as pd
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .edge_device_ports import find_connected_devices
from .device_link_description import create_device_link_description
from .storage_server_shape_links import create_device_shape_links


def edge_device_shapes_compilation_init(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, 
                                        switch_pair_df, san_graph_sw_pair_df,
                                        project_constants_lst, san_graph_grid_df, san_topology_constantants_sr):
    """Main function to create switch, VC modules and interconnect link shapes tables"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'san_topology_device_out', 'san_topology_device_in')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)

    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output or input data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, 
                                            max_title, analyzed_data_names)
    if force_run:
        # data imported from init file (regular expression patterns) to extract values from data columns
        pattern_dct, _ = sfop.regex_pattern_import('topology_regex', max_title)
        

        # current operation information string
        info = f'Generating edge device link shapes table'
        print(info, end =" ") 

        connected_devices_df, storage_shape_links_df, server_shape_links_df, \
            san_graph_sw_pair_group_df, fabric_name_duplicated_sr, fabric_name_dev_sr = \
                edge_device_shapes_compilation(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, 
                                                switch_pair_df, san_graph_sw_pair_df,
                                                san_graph_grid_df, pattern_dct, san_topology_constantants_sr)
        # after finish display status
        meop.status_info('ok', max_title, len(info))    
        # create list with partitioned DataFrames
        data_lst = [san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df]
        # writing data to sql
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df, *_ = data_lst
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df


def edge_device_shapes_compilation(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, 
                                    switch_pair_df, san_graph_sw_pair_df, 
                                    san_graph_grid_df, pattern_dct, san_topology_constantants_sr):
    """Aggregated function to create switch, VC modules and interconnect link shapes DataFrames"""
    
    # find unique device ports in each fabric_name
    connected_devices_df = find_connected_devices(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df)
    # create link description for switch -> device_name rows on fabric_name level
    connected_devices_df = create_device_link_description(connected_devices_df, switch_pair_df, pattern_dct)
    # create device shapes and link shapes to the switch shapes
    connected_devices_df, storage_shape_links_df, server_shape_links_df, \
        san_graph_sw_pair_group_df, fabric_name_duplicated_lst, fabric_name_dev_lst = \
            create_device_shape_links(connected_devices_df, san_graph_sw_pair_df, san_graph_grid_df, 
                                        pattern_dct, san_topology_constantants_sr)

    fabric_name_duplicated_sr = pd.Series(fabric_name_duplicated_lst)
    fabric_name_dev_sr = pd.Series(fabric_name_dev_lst)

    return (connected_devices_df, storage_shape_links_df, server_shape_links_df, 
            san_graph_sw_pair_group_df, fabric_name_duplicated_sr, fabric_name_dev_sr)