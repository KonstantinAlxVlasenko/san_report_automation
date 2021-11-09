"""Module to create Effective zoning configuration summary statistics"""

import numpy as np
import pandas as pd
# from common_operations_dataframe import count_frequency, find_mean_max_min
from .zoning_statistics_aux_fn import active_vs_configured_ports, merge_df

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop



def cfg_dashborad(zonemember_statistics_df, portshow_zoned_aggregated_df, zoning_aggregated_df, alias_aggregated_df):
    """Main function to count effective zoning configuration statistics."""

    def split_statistics(zonemember_statistics_df):
        """Function to split up zonemember_statistics_df into three DataFrames: 
        zonelevel statistics for effective zones only,
        zonelevel statitics for effective + defined zones,
        effective config summary statistics."""
 
        mask_zone_effective = zonemember_statistics_df['cfg_type'] == 'effective'
        mask_cfg_summary = zonemember_statistics_df['zone'].isna()
        # DataFrame with information for each zone (zonelevel) effective and effective+defined
        zonelevel_statistics_effective_df = zonemember_statistics_df.loc[mask_zone_effective & ~mask_cfg_summary].copy()
        zonelevel_statistics_df = zonemember_statistics_df.loc[~mask_cfg_summary].copy()
        for column in ['zone_duplicated', 'zone_paired', 'zone_absorber']:
            if not column in zonelevel_statistics_df.columns:
                zonelevel_statistics_effective_df[column] = np.nan
        # add zone_duplicated_tag
        mask_not_duplicated_zone = zonelevel_statistics_effective_df['zone_duplicated'].isna()
        zonelevel_statistics_effective_df['zone_duplicated'] = \
            zonelevel_statistics_effective_df['zone_duplicated'].where(mask_not_duplicated_zone, 'zone_duplicated_tag')
        # add zone_paired tag
        mask_not_paired_zone = zonelevel_statistics_effective_df['zone_paired'].isna()
        zonelevel_statistics_effective_df['zone_paired'] = \
            zonelevel_statistics_effective_df['zone_paired'].where(mask_not_paired_zone, 'zone_paired_tag')
        # add zone_absorbed tag
        mask_not_absorbed_zone = zonelevel_statistics_effective_df['zone_absorber'].isna()
        zonelevel_statistics_effective_df['zone_absorber'] = \
            zonelevel_statistics_effective_df['zone_absorber'].where(mask_not_absorbed_zone, 'zone_absorbed_tag')

        # DataFrame with summary for all zones in Fabric (cfg level)
        cfglevel_statistics_effective_df = zonemember_statistics_df.loc[mask_zone_effective & mask_cfg_summary].copy()
        return zonelevel_statistics_effective_df, zonelevel_statistics_df, cfglevel_statistics_effective_df


    def count_zone_type(cfglevel_statistics_effective_df):
        """Count quantity of each zone type (all zones, lsan and peer zones)"""

        zone_type_columns = ['Fabric_name', 'Fabric_label', 'zone_tag', 'qos_tag', 'lsan_tag', 'property', 'tdz_tag']
        zone_type_columns = [column for column in zone_type_columns if column in cfglevel_statistics_effective_df.columns]
        zone_type_summary_df = cfglevel_statistics_effective_df.loc[:, zone_type_columns].copy()
        zone_type_summary_df.set_index(['Fabric_name', 'Fabric_label'], inplace=True)
        zone_type_summary_df.fillna(0, inplace=True)
        # summarize all zone types
        zone_type_summary_df.loc[('All', ''), :] = zone_type_summary_df.sum(numeric_only=True, axis=0)
        zone_type_summary_df.reset_index(inplace=True)
        return zone_type_summary_df


    def count_zone_note(zonelevel_statistics_effective_df):
        """Function to count quantity of zones with each type of note:
        no_target, no_initiator, wwnn_zones, wwnp_duplicated_zones, etc"""

        for column in ['Wwnn', 'Wwnp_duplicated']:
            if not column in zonelevel_statistics_effective_df.columns:
                zonelevel_statistics_effective_df[column] = 0
        zonelevel_statistics_effective_df['zone_Wwnn_tag'] = \
            np.where(zonelevel_statistics_effective_df['Wwnn'] > 0, 'zone_Wwnn_tag', pd.NA)
        zonelevel_statistics_effective_df['zone_Wwnp_duplicated_tag'] = \
            np.where(zonelevel_statistics_effective_df['Wwnp_duplicated'] > 0, 'zone_Wwnp_duplicated_tag', pd.NA)

        # add column with tags (correct_zone, commented_zone, non_working_zone)
        for column in ['Target_Initiator_note', 'Target_model_note']:
            if not column in zonelevel_statistics_effective_df.columns:
                zonelevel_statistics_effective_df[column] = np.nan
        for column in ['absent', 'remote_na']:
            if not column in zonelevel_statistics_effective_df.columns:
                zonelevel_statistics_effective_df[column] = 0

        # add no_alias_zone tag
        for column in ['tdz_tag', 'zonemember_wwn', 'zonemember_domain_portindex']:
            if not column in  zonelevel_statistics_effective_df:
                zonelevel_statistics_effective_df[column] = 0
        mask_tdz = zonelevel_statistics_effective_df['tdz_tag'] != 0
        mask_zonemember_wwn = zonelevel_statistics_effective_df['zonemember_wwn'] != 0
        mask_zonemember_di = zonelevel_statistics_effective_df['zonemember_domain_portindex'] != 0
        zonelevel_statistics_effective_df['zone_no_alias_tag'] = \
            np.select([~mask_tdz & (mask_zonemember_wwn | mask_zonemember_di),
                        mask_tdz & mask_zonemember_di], ['zone_no_alias_tag', 'zone_no_alias_tag'],
                        default=pd.NA)

        # non_working zones
        non_working_zone_tags = ['no_initiator', 'no_target', 'no_target, no_initiator', 'no_target, several_initiators']
        mask_non_working_zone = zonelevel_statistics_effective_df['Target_Initiator_note'].isin(non_working_zone_tags)
        # commented zones
        mask_several_initiators = zonelevel_statistics_effective_df['Target_Initiator_note'] == 'several_initiators'
        storage_models_tags = ['different_storages', 'storage_library']
        mask_storage_models = zonelevel_statistics_effective_df['Target_model_note'].isin(storage_models_tags)
        mask_wwnn = zonelevel_statistics_effective_df['zone_Wwnn_tag'] == 'zone_Wwnn_tag'
        mask_wwnp_duplicated = zonelevel_statistics_effective_df['zone_Wwnp_duplicated_tag'] == 'zone_Wwnp_duplicated_tag'
        mask_absent_remote = (zonelevel_statistics_effective_df[['absent', 'remote_na']] > 0).any(axis=1)
        mask_no_alias = zonelevel_statistics_effective_df['zone_no_alias_tag'].notna()
        mask_commented_zone = mask_several_initiators | mask_storage_models | mask_wwnn | mask_wwnp_duplicated | mask_absent_remote | mask_no_alias
        # zones verification
        zonelevel_statistics_effective_df['zone_note_summary'] = np.select(
            [mask_non_working_zone, mask_commented_zone],
            ['non-working_zone', 'commented_zone'], default='correct_zone')

        zone_notes_summary_df = \
            dfop.count_frequency(zonelevel_statistics_effective_df, \
                count_columns=['zone_duplicated', 'zone_absorber', 'zone_paired', 'Pair_zone_note', 'Target_Initiator_note', 'Target_model_note', 
                                    'zone_Wwnn_tag', 'zone_Wwnp_duplicated_tag', 'zone_no_alias_tag', 'zone_note_summary'])
        return zone_notes_summary_df
    

    def count_unused_zone(zonelevel_statistics_df):
        """Function to add unused zones counter 
        (zones wich are not part of active (effective) configuaration)"""
         
        # remove duplicated zones
        zonelevel_statistics_duplicates_free_df = \
            zonelevel_statistics_df.drop_duplicates(subset=['Fabric_name', 'Fabric_label', 'zone']).copy()
        zone_unused_summary = dfop.count_frequency(zonelevel_statistics_duplicates_free_df, count_columns=['Effective_cfg_usage_note'])
        return zone_unused_summary


    def count_device_port_status(zoning_aggregated_df):
        """"Function to count device ports in zoning configuration for each ports state: 
        local, remote_imported, remote_na, remote_initializing, absent, remote_configured"""

        port_status_columns = ['Fabric_name', 'Fabric_label', 'alias_member', 'PortName', 'Fabric_device_status']
        mask_effective = zoning_aggregated_df['cfg_type'] =='effective'
        zoned_ports_status_df = zoning_aggregated_df.loc[mask_effective, port_status_columns].copy()
        # each PortWwnn (device port) is counted only once
        zoned_ports_status_df.drop_duplicates(subset=port_status_columns, inplace=True)
        zoned_ports_status_summary_df = dfop.count_frequency(zoned_ports_status_df, count_columns=['Fabric_device_status'])
        return zoned_ports_status_summary_df


    def count_zone_alias_port_statistics(zonelevel_statistics_effective_df):
        """Function to count mean, max, min values for alias qunatity and active ports quantity in zones for each Fabric"""

        # zonemember might be presented as alias, Wwnp or Domain Index
        alias_count_columns = ['zonemember_alias', 'zonemember_wwn', 'zonemember_domain_portindex']
        alias_count_columns = [column for column in alias_count_columns if column in zonelevel_statistics_effective_df.columns]
        # sum all "aliases" for each zone
        zonelevel_statistics_effective_df['Total_alias'] = \
            zonelevel_statistics_effective_df[alias_count_columns].sum(axis=1)
        # count statistics for each Fabric
        count_columns_dct = {'Total_alias': 'alias_per_zone', 'Total_zonemembers_active': 'active_ports_per_zone'}
        alias_vs_active_ports_per_zone_df = \
            dfop.find_mean_max_min(zonelevel_statistics_effective_df, count_columns = count_columns_dct)
        return alias_vs_active_ports_per_zone_df


    def count_alias_port_zone_statistics(alias_aggregated_df):
        """Function to count mean, max, min values for defined ports, active ports number and 
        number zones alias applied in in aliases for each Fabric in effective cfg"""

        mask_alias_effective = alias_aggregated_df['cfg_type'] == 'effective'
        alias_effective_df = alias_aggregated_df.loc[mask_alias_effective].copy()
        # defined and active ports number for each zonemember (alias) were calculated in advance
        # thus each alias should be counted only once
        alias_effective_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'zone_member'], inplace=True)
        count_columns_lst = ['ports_per_alias', 'active_ports_per_alias', 'zone_number_alias_used_in']
        count_columns_dct = {k:k for k in count_columns_lst}
        alias_ports_vs_zone_usage_df = dfop.find_mean_max_min(alias_effective_df, count_columns = count_columns_dct)
        return alias_ports_vs_zone_usage_df

    # split zonemember_statistics_df DataFrame
    zonelevel_statistics_effective_df, zonelevel_statistics_df, cfglevel_statistics_effective_df = \
            split_statistics(zonemember_statistics_df)
    # count values frequencies in splitted statistics DataFrames
    zone_type_summary_df = count_zone_type(cfglevel_statistics_effective_df)
    zone_notes_summary_df = count_zone_note(zonelevel_statistics_effective_df)
    zone_unused_summary = count_unused_zone(zonelevel_statistics_df)
    zoned_ports_status_summary_df = count_device_port_status(zoning_aggregated_df)
    # count active and unzoned device ports in Fabric
    zoned_vs_total_ports_summary_df = \
        active_vs_configured_ports(portshow_zoned_aggregated_df, configuration_type='cfg_effective')
    alias_vs_active_ports_per_zone_df = count_zone_alias_port_statistics(zonelevel_statistics_effective_df)
    alias_ports_vs_zone_usage_df = count_alias_port_zone_statistics(alias_aggregated_df)
    
    # merge all summary DataFrames into one
    df_lst = [zone_type_summary_df, zone_notes_summary_df, zone_unused_summary, zoned_ports_status_summary_df, 
                    zoned_vs_total_ports_summary_df, alias_vs_active_ports_per_zone_df, alias_ports_vs_zone_usage_df]
    active_cfg_statistics_df = merge_df(df_lst, fillna_index=4)

    return active_cfg_statistics_df

