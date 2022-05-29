"""Main module to extract data from switch, blade system, synergy system, 3PAR configuration files"""

from .bladesystem import blade_system_extract, synergy_system_extract
from .fabric_routing import (fabric_membership_extract, fcr_membership_extract,
                             interswitch_connection_extract)
from .fabric_services import connected_devices_extract, zoning_extract
from .storage_3par import storage_3par_extract
from .switch_monitoring import log_extract, sensor_extract
from .switch_params import (chassis_params_extract, maps_params_extract,
                            switch_params_extract)
from .switch_ports import portcfg_sfp_extract, portcmd_extract


def system_configuration_extract(parsed_sshow_maps_lst, project_constants_lst, software_path_sr):
    """Main function to extract system configuration files"""

    # chassis parameters parsing
    chassis_params_df, slot_status_df = chassis_params_extract(parsed_sshow_maps_lst, project_constants_lst)
    # maps parameters parsing
    maps_params_df = maps_params_extract(parsed_sshow_maps_lst, project_constants_lst)
    # switch parameters parsing
    switch_params_df, switchshow_ports_df = switch_params_extract(chassis_params_df, project_constants_lst)
    # fabric membership pasing (AG swithe information extracted from Principal switches)
    fabricshow_df, ag_principal_df = fabric_membership_extract(switch_params_df, project_constants_lst)
    # portshow statistics parsing
    portshow_df = portcmd_extract(chassis_params_df, project_constants_lst)
    # port sfp and cfg parsing
    sfpshow_df, portcfgshow_df = portcfg_sfp_extract(switch_params_df, project_constants_lst)
    # nameserver parsing
    fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df = connected_devices_extract(switch_params_df, project_constants_lst)
    # inter switch connection parsing
    isl_df, trunk_df, porttrunkarea_df, lsdb_df = interswitch_connection_extract(switch_params_df, project_constants_lst)
    # fabric routing parsing
    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, fcrxlateconfig_df = \
        fcr_membership_extract(switch_params_df, project_constants_lst)
    # zoning configuration parsing
    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = \
        zoning_extract(switch_params_df, project_constants_lst)
    # switch sensors parsing
    sensor_df = sensor_extract(chassis_params_df, project_constants_lst)
    # error log parsing
    errdump_df = log_extract(chassis_params_df, project_constants_lst)
    # blade system configuration parsing
    blade_module_df, blade_servers_df, blade_vc_df = blade_system_extract(project_constants_lst)
    # synergy system configuration parsing
    synergy_module_df, synergy_servers_df = synergy_system_extract(project_constants_lst)
    # 3PAR storage system configuration download and parsing
    system_3par_df, port_3par_df, host_3par_df = \
            storage_3par_extract(nsshow_df, nscamshow_df, project_constants_lst, software_path_sr)

    extracted_configuration_lst = [chassis_params_df, slot_status_df, maps_params_df, 
                                    switch_params_df, switchshow_ports_df,
                                    fabricshow_df, ag_principal_df, 
                                    portshow_df, sfpshow_df, portcfgshow_df,
                                    fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df,
                                    isl_df, trunk_df, porttrunkarea_df, lsdb_df,
                                    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, fcrxlateconfig_df,
                                    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df,
                                    sensor_df, errdump_df,
                                    blade_module_df, blade_servers_df, blade_vc_df,
                                    synergy_module_df, synergy_servers_df,
                                    system_3par_df, port_3par_df, host_3par_df]
    return extracted_configuration_lst
        