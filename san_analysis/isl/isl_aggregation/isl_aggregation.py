"""Module to get aggreagated ISL DataFrame including islshow, trunkshow and fcredge"""


import numpy as np

import utilities.dataframe_operations as dfop

from .concat_ifl import concat_isl_ifl
from .isl_characteristics import (attenuation_calc, max_isl_speed,
                                  verify_isl_cfg_equality)
from .isl_port_level import port_level_join
from .isl_switch_level import (fabriclabel_join, switch_details_join,
                               switchname_join)


def isl_aggregated(fabric_labels_df, switch_params_aggregated_df, 
    isl_df, trunk_df, lsdb_df, fcredge_df, portshow_df, sfpshow_df, portcfgshow_df, switchshow_df, pattern_dct):
    """Function to create ISL aggregated DataFrame"""

    # remove unlabeled fabrics and slice DataFrame to drop unnecessary columns
    fabric_clean_df = fabric_clean(fabric_labels_df)
    # add switchnames to trunk and fcredge DataFrames
    isl_df, trunk_df, fcredge_df = switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df)
    # add IFL links to ISL-
    isl_df, fcredge_df = concat_isl_ifl(isl_df, trunk_df, fcredge_df, switchshow_df, switch_params_aggregated_df)
    # outer join of isl + ifl with trunk DataFrames 
    isl_aggregated_df = trunk_isl_join(isl_df, trunk_df)
    # number isl links
    isl_aggregated_df = number_isl(isl_aggregated_df)
    # add information for each isl port
    isl_aggregated_df, fcredge_df = port_level_join(isl_aggregated_df, fcredge_df, switchshow_df, portshow_df, 
                                                    portcfgshow_df, sfpshow_df, lsdb_df, pattern_dct)
    # adding switch information to isl aggregated DataFrame
    isl_aggregated_df = switch_details_join(switch_params_aggregated_df, isl_aggregated_df)
    # adding fabric labels to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df)
    # calculate maximum link speed
    isl_aggregated_df = max_isl_speed(isl_aggregated_df)
    # calculate link attenuation
    isl_aggregated_df = attenuation_calc(isl_aggregated_df)
    # remove unlabeled switches from isl aggregated Dataframe
    isl_aggregated_df, fcredge_df = remove_empty_fabrics(isl_aggregated_df, fcredge_df)
    # verify which port settings differs from both side of ISL 
    isl_aggregated_df = verify_isl_cfg_equality(isl_aggregated_df)
    # sort isl_aggregated
    isl_aggregated_df = sort_isl(isl_aggregated_df, switch_params_aggregated_df)
    return isl_aggregated_df, fcredge_df
    

def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Name', 'Enet_IP_Addr']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'SwitchName'}, inplace=True)
    return fabric_clean_df


def trunk_isl_join(isl_df, trunk_df):
    """Join Trunk and ISL DataFrames"""

    # List of columns DataFrames are joined on     
    join_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 'SwitchName',
                'switchWwn', 'switchRole', 'FabricID', 'FC_router', 'portIndex', 
                'Connected_portIndex', 'Connected_SwitchName',
                'Connected_switchWwn', 'Connected_switchDID']  

    fc_router = True
    if isl_df['FC_router'].isna().all():
        join_lst.remove('FC_router')
        trunk_df.drop(columns=['FC_router'], inplace=True)
        isl_df.drop(columns=['FC_router'], inplace=True)
        fc_router = False
    # merge updated ISL and TRUNK DataFrames
    isl_aggregated_df = trunk_df.merge(isl_df, how='outer', on=join_lst)
       
    if not fc_router:
        isl_aggregated_df['FC_router'] = np.nan
    return isl_aggregated_df


def number_isl(isl_aggregated_df):
    """Function to number ISL links"""

    # add ISL number in case of trunk presence and remove ifl tag
    isl_aggregated_df['ISL_number'].fillna(method='ffill', inplace=True)
    if isl_aggregated_df['ISL_number'].notna().any():
        mask_ifl = isl_aggregated_df['ISL_number'].str.contains('ifl', case=False, na=False)
        isl_aggregated_df.loc[mask_ifl, 'ISL_number'] = np.nan
    return isl_aggregated_df


def remove_empty_fabrics(isl_aggregated_df, fcredge_df):
    """
    Function to remove switches which are not part of research
    and sort required switches
    """

    # drop switches with empty fabric labels
    isl_aggregated_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # sort switches by switch names 
    isl_aggregated_df.sort_values(
        by=['switchType', 'chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], ascending=[False, True, True, True, True], inplace=True)
    # reset indexes
    isl_aggregated_df.reset_index(inplace=True, drop=True)
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
        fcredge_df.sort_values(by=['chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], \
                                      ascending=[True, True, True, True], inplace=True)        
    return isl_aggregated_df, fcredge_df


def sort_isl(isl_aggregated_df, switch_params_aggregated_df):
    """Function to sort ISLs based on isl_sort_columns"""

    # adding switchtype to sort switches by model
    isl_aggregated_df = dfop.dataframe_fillna(isl_aggregated_df, switch_params_aggregated_df, filled_lst=['switchType'], join_lst=['switchWwn'])

    isl_sort_columns = ['Fabric_name', 'Fabric_label',
                        'switchType', 'chassis_name', 'chassis_wwn',
                        'SwitchName', 'switchWwn',
                        'Trunking_GroupNumber', 'Master',
                        'ISL_number', 'IFL_number', 'portIndex']
    isl_sort_columns = [column for column in isl_sort_columns if column in isl_aggregated_df.columns]
    order_lst = [column != 'switchType' for column in isl_sort_columns]
    isl_aggregated_df.sort_values(by=isl_sort_columns, ascending=order_lst, inplace=True)
    return isl_aggregated_df



