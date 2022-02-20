"""Main module to extract data from switch, blade system, synergy system, 3PAR configuration files"""

from .chassis_params import chassis_params_extract
from .maps_params import maps_params_extract
from .switch_params import switch_params_extract
from .fabric_membership import fabric_membership_extract
from .portcmd import portcmd_extract
from .portcfg_sfp import portcfg_sfp_extract
from .nameserver import connected_devices_extract
from .isl import interswitch_connection_extract
from .fcrfabric_membership import fcr_membership_extract
from .zoning import zoning_extract
from .sensor import sensor_extract
from .log import log_extract
from .bladesystem import blade_system_extract
from .synergy import synergy_system_extract
from .storage_3par import storage_3par_extract


def system_configuration_extract(parsed_sshow_maps_lst, report_entry_sr, report_creation_info_lst):
    """Main function to extract system configuration files"""

    # chassis parameters parsing
    chassis_params_df = chassis_params_extract(parsed_sshow_maps_lst, report_creation_info_lst)
    # maps parameters parsing
    maps_params_df = maps_params_extract(parsed_sshow_maps_lst, report_creation_info_lst)
    # switch parameters parsing
    switch_params_df, switchshow_ports_df = switch_params_extract(chassis_params_df, report_creation_info_lst)
    # fabric membership pasing (AG swithe information extracted from Principal switches)
    fabricshow_df, ag_principal_df = fabric_membership_extract(switch_params_df, report_creation_info_lst)
    # portshow statistics parsing
    portshow_df = portcmd_extract(chassis_params_df, report_creation_info_lst)
    # port sfp and cfg parsing
    sfpshow_df, portcfgshow_df = portcfg_sfp_extract(switch_params_df, report_creation_info_lst)
    # nameserver parsing
    fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df = connected_devices_extract(switch_params_df, report_entry_sr, report_creation_info_lst)
    # inter switch connection parsing
    isl_df, trunk_df, porttrunkarea_df, lsdb_df = interswitch_connection_extract(switch_params_df, report_creation_info_lst)
    # fabric routing parsing
    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df = fcr_membership_extract(switch_params_df, report_creation_info_lst)
    # zoning configuration parsing
    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = \
        zoning_extract(switch_params_df, report_creation_info_lst)
    # switch sensors parsing
    sensor_df = sensor_extract(chassis_params_df, report_creation_info_lst)
    # error log parsing
    errdump_df = log_extract(chassis_params_df, report_creation_info_lst)
    # blade system configuration parsing
    blade_module_df, blade_servers_df, blade_vc_df = blade_system_extract(report_entry_sr, report_creation_info_lst)
    exit()
    # synergy system configuration parsing
    synergy_module_df, synergy_servers_df = synergy_system_extract(report_entry_sr, report_creation_info_lst)
    # 3PAR storage system configuration download and parsing
    system_3par_df, port_3par_df, host_3par_df = \
            storage_3par_extract(nsshow_df, nscamshow_df, report_entry_sr, report_creation_info_lst)

    extracted_configuration_lst = [chassis_params_df, maps_params_df, 
                                    switch_params_df, switchshow_ports_df,
                                    fabricshow_df, ag_principal_df, 
                                    portshow_df, sfpshow_df, portcfgshow_df,
                                    fdmi_df, nsshow_df, nscamshow_df, nsshow_dedicated_df, nsportshow_df,
                                    isl_df, trunk_df, porttrunkarea_df, lsdb_df,
                                    fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df,
                                    cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df,
                                    sensor_df, errdump_df,
                                    blade_module_df, blade_servers_df, blade_vc_df,
                                    synergy_module_df, synergy_servers_df,
                                    system_3par_df, port_3par_df, host_3par_df]

    return extracted_configuration_lst
        