
"""Module to generate switch, VC modules and interconnect link shapes. 
These shape tables are used to create SAN topology in Visio."""


import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .switch_shapes import create_san_graph_switch, create_san_graph_sw_pair
from .meta_san import add_meta_san_graph_sw_pair, add_meta_san_graph_isl
from .isl_shapes import create_san_graph_isl
from .npiv_link_shapes import create_san_graph_npiv_links


def switch_isl_shapes_compilation_init(switch_params_aggregated_df, switch_pair_df, 
                                isl_aggregated_df, isl_statistics_df, npiv_statistics_df, 
                                project_constants_lst, san_graph_grid_df, san_topology_constantants_sr):
    """Main function to create switch, VC modules and interconnect link shapes tables"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution (output data)
    # data titles which module is dependent on (input data)
    data_names, analyzed_data_names = dfop.list_from_dataframe(io_data_names_df, 'san_topology_switch_out', 'san_topology_switch_in')
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
        info = f'Generating switch and switch link shapes table'
        print(info, end =" ") 

        san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df  = \
            switch_isl_shapes_compilation(switch_params_aggregated_df, switch_pair_df, 
                                            isl_aggregated_df, isl_statistics_df, npiv_statistics_df, 
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
    return san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df


def switch_isl_shapes_compilation(switch_params_aggregated_df, switch_pair_df, 
                                    isl_aggregated_df, isl_statistics_df, npiv_statistics_df, 
                                    san_graph_grid_df, pattern_dct, san_topology_constantants_sr):
    """Aggregated function to create switch, VC modules and interconnect link shapes DataFrames"""
    
    META_SAN_NAME = san_topology_constantants_sr['meta_san_name']
    
    # switch shapes
    san_graph_switch_df = create_san_graph_switch(switch_params_aggregated_df, switch_pair_df, isl_statistics_df, san_graph_grid_df)
    # switch pair shapes
    san_graph_sw_pair_df = create_san_graph_sw_pair(san_graph_switch_df)
    
    if (san_graph_sw_pair_df['Fabric_name'] == META_SAN_NAME).any():
        META_SAN_NAME = META_SAN_NAME + '_Total'    
    
    # add meta san to switch pair df in case if routing is present
    san_graph_sw_pair_df = add_meta_san_graph_sw_pair(san_graph_sw_pair_df, switch_params_aggregated_df, META_SAN_NAME)
    # isl shapes
    san_graph_isl_df = create_san_graph_isl(isl_aggregated_df, isl_statistics_df, switch_pair_df, pattern_dct)
    # add meta san isl shapes
    san_graph_isl_df = add_meta_san_graph_isl(san_graph_isl_df, san_graph_switch_df, switch_params_aggregated_df, META_SAN_NAME)
    # npiv link to VC modules, AG and NPV mode switches shapes 
    san_graph_npiv_df = create_san_graph_npiv_links(npiv_statistics_df, pattern_dct)
    return san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df