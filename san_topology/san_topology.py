"""Main module to visualize SAN topology"""

from .switch_isl_shapes import switch_isl_shapes_compilation_init


def visualize_san_topology(analyzed_configuration_lst, project_constants_lst, san_graph_grid_df, san_topology_constantants_sr):
    """Main function of san_analysis package. Performs analysis of extracted configuration data, 
    save data to database and report file"""


    switch_params_aggregated_df, switch_pair_df, \
        isl_aggregated_df, isl_statistics_df, npiv_statistics_df, \
            portshow_aggregated_df, npv_ag_connected_devices_df, fcr_xd_proxydev_df = analyzed_configuration_lst


    san_graph_sw_pair_df, san_graph_isl_df, san_graph_npiv_df = switch_isl_shapes_compilation_init(switch_params_aggregated_df, switch_pair_df, 
                                                                                                    isl_aggregated_df, isl_statistics_df, npiv_statistics_df, 
                                                                                                    project_constants_lst, san_graph_grid_df, san_topology_constantants_sr)