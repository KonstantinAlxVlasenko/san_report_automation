"""Main module to visualize SAN topology"""

from .edge_device_shapes import edge_device_shapes_compilation_init
from .switch_isl_shapes import switch_isl_shapes_compilation_init
from .visio_diagram import visio_diagram_init


def visualize_san_topology(analyzed_configuration_lst, project_constants_lst, software_path_sr, 
                            san_graph_grid_df, san_topology_constantants_sr):
    """Main function of san_topology. Creates switch and edge device shapes,
    draws san topology in Visio"""


    switch_params_aggregated_df, switch_pair_df, \
        isl_aggregated_df, isl_statistics_df, npiv_statistics_df, \
            portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df = analyzed_configuration_lst

    # switch and switch interconnect shapes
    san_graph_switch_df, san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df = \
        switch_isl_shapes_compilation_init(switch_params_aggregated_df, switch_pair_df, 
                                            isl_aggregated_df, isl_statistics_df, npiv_statistics_df, 
                                            project_constants_lst, san_graph_grid_df, san_topology_constantants_sr)
    # edge device and link shapes
    storage_shape_links_df, server_shape_links_df, \
        san_graph_sw_pair_group_df, \
            fabric_name_duplicated_sr, fabric_name_dev_sr = \
                edge_device_shapes_compilation_init(portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df, 
                                                    switch_pair_df, san_graph_sw_pair_df,
                                                    project_constants_lst, san_graph_grid_df, san_topology_constantants_sr)

    visio_diagram_init(san_graph_switch_df, san_graph_sw_pair_df, 
                        san_graph_isl_df, san_graph_npiv_df, 
                        storage_shape_links_df, server_shape_links_df, 
                        san_graph_sw_pair_group_df, 
                        fabric_name_duplicated_sr, fabric_name_dev_sr, 
                        project_constants_lst, 
                        software_path_sr, san_topology_constantants_sr)


                                                                