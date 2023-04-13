"""
Module to create tables 
- list of servers, storages, libraries
- list of hba fw and drivers
- list of servers, storages, libraries san connection 
"""


import utilities.dataframe_operations as dfop
import utilities.report_operations as report


def portcmd_report_main(portshow_aggregated_df, storage_connection_statistics_df, 
                            device_connection_statistics_df, data_names, report_headers_df, report_columns_usage_sr):
    """Function to create required report DataFrames out of aggregated DataFrame"""

    add_columns_lst = ['FW_Recommeneded', 'Driver_Recommeneded', 'FW_Supported', 'HW_Supported']
    portshow_aggregated_df = \
        portshow_aggregated_df.reindex(columns=[*portshow_aggregated_df.columns.tolist(), *add_columns_lst])
    # partition aggregated DataFrame to required tables
    servers_report_df, storage_report_df, library_report_df, hba_report_df, \
        storage_connection_df,  library_connection_df, server_connection_df = \
        report.generate_report_dataframe(portshow_aggregated_df, report_headers_df, report_columns_usage_sr, *data_names)

    # clean and sort DataFrames
    # device report
    servers_report_df = _clean_dataframe(servers_report_df, 'srv')
    hba_report_df = _clean_dataframe(hba_report_df, 'srv', duplicates = ['Идентификатор порта WWPN'])
    hba_report_df = device_name_duplicates_free_column(hba_report_df)
    storage_report_df = _clean_dataframe(storage_report_df, 'stor')
    storage_report_df = _multi_fabric(storage_report_df, report_columns_usage_sr)
    library_report_df = _clean_dataframe(library_report_df, 'lib')
    # device connection reports
    storage_connection_df = _clean_dataframe(storage_connection_df, 'stor', clean=True)
    storage_connection_df = device_name_duplicates_free_column(storage_connection_df)
    storage_connection_df = dfop.translate_values(storage_connection_df)
    library_connection_df = _clean_dataframe(library_connection_df, 'lib', clean=True)
    library_connection_df = device_name_duplicates_free_column(library_connection_df)
    library_connection_df = dfop.translate_values(library_connection_df)
    server_connection_df = _clean_dataframe(server_connection_df, 'srv', clean=True)
    server_connection_df = device_name_duplicates_free_column(server_connection_df)
    server_connection_df = dfop.translate_values(server_connection_df)
    
    storage_connection_statistics_report_df = connection_statistics_report(storage_connection_statistics_df, report_headers_df)
    device_connection_statistics_report_df = connection_statistics_report(device_connection_statistics_df, report_headers_df)
    return servers_report_df, storage_report_df, library_report_df, hba_report_df, \
            storage_connection_df,  library_connection_df, server_connection_df, \
                storage_connection_statistics_report_df, device_connection_statistics_report_df
    

def _clean_dataframe(df, mask_type, 
                        duplicates = ['Фабрика', 'Имя устройства', 'Имя группы псевдонимов', 
                                        'Класс устройства', 'Тип устройства'], 
                        clean = False):
    """
    Auxiliary function to sort, remove duplicates and 
    drop columns in cases they are not required in report DataFrame
    """

    # list of columns to check if they are empty
    columns_empty = ['Медленное устройство', 'Подключено через AG', 'Real device behind AG']
    # list of columns to check if all values are equal
    columns_unique = ['Режим коммутатора', 'LSAN', 'Connected_NPIV', 'FC4s', 'FCoE']
    # list of columns to sort DataFrame on
    columns_sort = [
        'Фабрика', 'Расположение', 'Имя устройства', 'Порт устройства', 
        'Подсеть', 'Имя коммутатора', 'Идентификатор порта WWPN'
        ]
    # create mask to filter required class only
    if mask_type == 'srv':
        mask = df['Класс устройства'].isin(['SRV', 'BLADE_SRV', 'SYNERGY_SRV', 'SRV_BLADE', 'SRV_SYNERGY'])
    elif mask_type == 'stor':
        mask = df['Класс устройства'] == 'STORAGE'
    elif mask_type == 'lib':
        mask = df['Класс устройства'] == 'LIB'
    elif mask_type == 'npiv':
        # to avoid warning fillna with "no"
        df['Connected_NPIV'].fillna('no', inplace=True)
        mask = df['Connected_NPIV'].str.contains('yes')
    # filter DataFrame base on hardware type 
    df = df.loc[mask].copy()
    # check if columns required to sort on are in the DataFrame
    columns_sort = [column for column in columns_sort if column in df.columns]
    df.sort_values(by = columns_sort, inplace = True)
    
    if duplicates:
        duplicates = [column for column in duplicates if column in df.columns]

    # DataFrames are cleaned in two ways
    # by drop duplicate values in certain columns
    if duplicates and not clean:
        df.drop_duplicates(subset = duplicates, inplace = True)
    # or by drop entire column
    if clean:
        # if all values in the column are the same
        for column in columns_unique:
            if column in df.columns and df[column].nunique() < 2:
                df.drop(columns = [column], inplace = True)
        # if all values are None in the column
        for column in columns_empty:
            # if all values are None
            if column in df.columns and df[column].isna().all():
                df.drop(columns=[column], inplace = True)
            # if all non None values are 'No'
            # elif column in df.columns and pd.Series(df[column] == 'No', pd.notna(df[column])).all():
            elif column in df.columns and (df[column].dropna() == 'No').all():
                df.drop(columns = [column], inplace = True)

    # drop column if each device connected to single fabric_name only
    possible_equal_column_pairs = [
                                    ('Количество портов устройства в подсети фабрики', 'Количество портов устройства в фабриках подсети'),
                                    ('Всего портов устройства', 'Количество портов устройства в подсетях фабрики')]
    df = dfop.drop_equal_columns(df, possible_equal_column_pairs)
    return df


def _multi_fabric(df, report_columns_usage_sr):
    """
    Function to check if device ports connected to different Fabrics.
    For example, main Fabric and replication Fabric.
    If yes then join Fabric cells values in one cell
    """

    df_columns = df.columns
    if not 'Имя устройства' in df_columns:
        return df

    if report_columns_usage_sr['fabric_name_usage'] and not df['Имя устройства'].is_unique:
        df['Фабрика'].fillna('nan', inplace=True)
        # if severeal aliases for one wwnp then combine all into one alias
        identical_values = {k: 'first' for k in df.columns[2:]}
        df = df.groupby(['Имя устройства'], as_index = False).agg({**{'Фабрика': ', '.join}, **identical_values})
        df = df.reindex(columns = df_columns)
        df = dfop.remove_duplicates_from_string(df, 'Фабрика')
    return df


def device_name_duplicates_free_column(df):
    """Function to create column with duplicates free device names"""

    duplicates_subset = ['Фабрика', 'Имя устройства', 'Тип устройства']
    duplicates_subset = [column for column in duplicates_subset if column in df.columns]
    df = dfop.remove_duplicates_from_column(df, 'Имя устройства', duplicates_subset, duplicates_free_column_name='Название устройства')
    df = dfop.drop_equal_columns(df, [('Имя устройства', 'Название устройства')])
    return df


def connection_statistics_report(connection_statistics_df, report_headers_df):
    """Function to create report table out of connection_statistics_df DataFrame"""

    # translate_dct = dct_from_columns('customer_report', max_title, 'Статистика_подключения_устройств_перевод_eng', 
    #                                 'Статистика_подключения_устройств_перевод_ru', init_file = 'san_automation_info.xlsx')

    connection_statistics_report_df = connection_statistics_df.copy()

    # drop FLOGI column if there is no virtual (NPIV) ports login
    if {'FLOGI', 'Group_type'}.issubset(connection_statistics_report_df.columns):
        # device_type group is always physical_virtual
        mask_not_device_type = connection_statistics_report_df['Group_type'] != 'device_type'
        if (connection_statistics_report_df.loc[mask_not_device_type, 'FLOGI'] == 'physical').all():
            connection_statistics_report_df.drop(columns='FLOGI', inplace=True)

    # translate notes
    columns = [column for column in connection_statistics_df.columns if 'note' in column and connection_statistics_df[column].notna().any()]
    columns.append('Fabric_name')

    connection_statistics_report_df = dfop.translate_dataframe(connection_statistics_report_df, report_headers_df, 
                                                            'Статистика_подключения_устройств_перевод', translated_columns=columns)
    # drop empty columns
    connection_statistics_report_df.dropna(axis=1, how='all', inplace=True)
    # drop zeroes for clean view
    connection_statistics_report_df = dfop.drop_zero(connection_statistics_report_df)
    return connection_statistics_report_df
