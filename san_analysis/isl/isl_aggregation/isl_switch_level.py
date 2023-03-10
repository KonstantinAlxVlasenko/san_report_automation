"""Module to add switch details for each ISL port"""


import utilities.dataframe_operations as dfop


def switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df):
    """Function to add switchnames to ISL, Trunk and FCREdge DataFrames"""

    # slicing fabric_clean DataFrame 
    switch_name_df = fabric_clean_df.loc[:, ['switchWwn', 'SwitchName']].copy()
    switch_name_df.rename(
            columns={'switchWwn': 'Connected_switchWwn', 'SwitchName': 'Connected_SwitchName'}, inplace=True)
    # adding switchnames to trunk_df
    trunk_df = trunk_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # adding switchnames to isl_df
    isl_df = isl_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    return isl_df, trunk_df, fcredge_df


def switch_details_join(switch_params_aggregated_df, isl_sfp_connected_df):
    """Adding switch licenses, max speed, description"""

    # column names list to slice switch_params_aggregated DataFrame and join with isl_aggregated Dataframe
    switch_lst = ['SwitchName', 'switchWwn', 'switchType','licenses', 'switch_speedMax', 'HPE_modelName', 
                    'Base_Switch', 'Allow_XISL_Use', 'Base_switch_in_chassis']   
    # addition switch parameters information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_sfp_connected_df, switch_params_aggregated_df, switch_lst, 2)
    # convert switchType column to float for later sorting
    isl_aggregated_df = isl_aggregated_df.astype(dtype = {'switchType': 'float64'}, errors = 'ignore').copy()
    # check if Trunking lic present on both swithes in ISL link
    switch_trunking_dct = {'licenses' : 'Trunking_license', 'Connected_licenses' : 'Connected_Trunking_license'}
    for lic, trunking_lic in switch_trunking_dct.items():
        isl_aggregated_df[trunking_lic] = isl_aggregated_df.loc[isl_aggregated_df[lic].notnull(), lic].apply(lambda x: 'Trunking' in x)
        isl_aggregated_df[trunking_lic].replace(to_replace={True: 'Yes', False: 'No'}, inplace = True)
    return isl_aggregated_df


def fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df):
    """Adding Fabric labels and IP addresses to ISL aggregated and FCREdge DataFrame"""

    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe 
    fabric_labels_lst = ['Fabric_name', 'Fabric_label', 'SwitchName', 'switchWwn']
    # addition fabric labels information to isl_aggregated and fcredge DataFrames 
    isl_aggregated_df = isl_aggregated_df.merge(fabric_clean_df.loc[:, fabric_labels_lst], 
                                                how = 'left', on = fabric_labels_lst[2:])
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(fabric_clean_df.loc[:, fabric_labels_lst],
                                      how = 'left', on = fabric_labels_lst[2:])
    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe
    switch_ip_lst = ['SwitchName', 'switchWwn', 'Enet_IP_Addr']
    # addition IP adddreses information to isl_aggregated DataFrame
    isl_aggregated_df = dfop.dataframe_join(isl_aggregated_df, fabric_clean_df,  switch_ip_lst, 2)
    return isl_aggregated_df, fcredge_df