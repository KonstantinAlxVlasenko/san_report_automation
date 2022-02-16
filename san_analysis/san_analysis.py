"""Main module to analysis extracted configuration files"""

from .fabric_label import fabric_label_analysis
from .blade_system import blade_system_analysis
from .switch_params import switch_params_analysis, switch_params_sw_pair_update
from .isl import isl_analysis
from .isl import isl_sw_pair_update
from .portcmd import portcmd_analysis
from .port_err_sfp_cfg import port_err_sfp_cfg_analysis
from .maps_npiv import maps_npiv_ports_analysis
from .zoning import zoning_analysis
from .storage_host import storage_host_analysis
from .sensor import sensor_analysis
from .port_statistics import port_statistics_analysis
from .errdump import errdump_analysis
from .switch_pair import switch_pair_analysis


def system_configuration_analysis(extracted_configuration_lst, report_creation_info_lst):
    """Main function of san_analysis package. Performs analysis of extracted configuration data, 
    save data to database and report file"""


    chassis_params_df, maps_params_df, \
        switch_params_df, switchshow_ports_df,\
            fabricshow_df, ag_principal_df, \
                portshow_df, sfpshow_df, portcfgshow_df,\
                    fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df, \
                        isl_df, trunk_df, porttrunkarea_df, lsdb_df,\
                            fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df,\
                                cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df,\
                                    sensor_df, errdump_df,\
                                        blade_module_df, blade_servers_df, blade_vc_df,\
                                            synergy_module_df, synergy_servers_df,\
                                                system_3par_df, port_3par_df, host_3par_df = extracted_configuration_lst
    

    # set fabric names and labels
    fabricshow_ag_labels_df = \
        fabric_label_analysis(switchshow_ports_df, switch_params_df, fabricshow_df, ag_principal_df, report_creation_info_lst)
    blade_module_loc_df = blade_system_analysis(blade_module_df, synergy_module_df, report_creation_info_lst)
    report_columns_usage_dct, switch_params_aggregated_df, fabric_clean_df = \
            switch_params_analysis(fabricshow_ag_labels_df, chassis_params_df, switch_params_df, maps_params_df, blade_module_loc_df, ag_principal_df, report_creation_info_lst)

    if len(report_creation_info_lst) == 3:
        report_creation_info_lst.append(report_columns_usage_dct)

    isl_aggregated_df, fcredge_aggregated_df = \
        isl_analysis(fabricshow_ag_labels_df, switch_params_aggregated_df, isl_df, trunk_df, lsdb_df, 
                            fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_creation_info_lst)

    portshow_aggregated_df = \
        portcmd_analysis(portshow_df, switchshow_ports_df, switch_params_df, switch_params_aggregated_df, isl_aggregated_df, 
                                nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df, 
                                ag_principal_df, porttrunkarea_df, alias_df, fdmi_df, blade_module_df, 
                                blade_servers_df, blade_vc_df, synergy_module_df, synergy_servers_df, 
                                system_3par_df, port_3par_df, report_creation_info_lst)

    switch_pair_df = switch_pair_analysis(switch_params_aggregated_df, portshow_aggregated_df, report_creation_info_lst)

    switch_params_aggregated_df = switch_params_sw_pair_update(switch_params_aggregated_df, switch_pair_df, report_creation_info_lst)

    isl_aggregated_df, isl_statistics_df = isl_sw_pair_update(isl_aggregated_df, fcredge_aggregated_df, switch_pair_df, report_creation_info_lst)

    portshow_sfp_aggregated_df =  port_err_sfp_cfg_analysis(portshow_aggregated_df, sfpshow_df, portcfgshow_df, report_creation_info_lst)


    portshow_npiv_df = maps_npiv_ports_analysis(portshow_sfp_aggregated_df, switch_params_aggregated_df, 
                                                isl_statistics_df, blade_module_loc_df, switch_pair_df, report_creation_info_lst)

    zoning_aggregated_df, alias_aggregated_df, portshow_zoned_aggregated_df = \
        zoning_analysis(switch_params_aggregated_df, portshow_aggregated_df, cfg_df, zone_df, alias_df, 
                            cfg_effective_df, fcrfabric_df, lsan_df, peerzone_df, report_creation_info_lst)

    storage_host_aggregated_df = storage_host_analysis(host_3par_df, system_3par_df, port_3par_df, 
                                                            portshow_aggregated_df, zoning_aggregated_df, report_creation_info_lst)

    sensor_aggregated_df = sensor_analysis(sensor_df, switch_params_aggregated_df, report_creation_info_lst)

    fabric_port_statistics_df = port_statistics_analysis(portshow_aggregated_df, report_creation_info_lst)

    errdump_aggregated_df, raslog_counter_df = \
        errdump_analysis(errdump_df, switchshow_ports_df, switch_params_aggregated_df, portshow_aggregated_df, report_creation_info_lst)