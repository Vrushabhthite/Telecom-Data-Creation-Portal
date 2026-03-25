def process_main2(main1_path, poc_path=None, poc_router_path=None, bsc_path=None, circle=None):

    import pandas as pd
    import numpy as np
    import os
    import re

    # ================= LOAD =================
    df_result = pd.read_excel(main1_path)

    # ================= REINDEX =================
    df_result = df_result.reindex(columns=[
        'REC_ID','OEM_NAME','NW_NAME','LINK_ID','LINK_NAME','SPAN_ID','SOURCE_NODE_NAME',
        'SOURCE_DXC_NAME','NODE_A_POPULAR_NAME','SINK_NODE_NAME','SINK_DXC_NAME','NODE_B_POPULAR_NAME',
        'Circle_A_End','SOURCE_CIR_NAME','SINK_CIRCLE_NAME','DXC_LINK_NAME','DXC_A_Popular_Name','DXC_B_Popular_Name',
        'FIBER_OWNER','STATUS','CREATED_AT','CREATED_BY','is_approve','APPROVED_BY','APPROVED_AT',
        'A_END_FROM_HP','Z_END_FROM_HP','SOURCE_CORE_NAME','BSC Name','Router_Name_A_End',
        'COMMON_PATH_LINK_GROUP_ID','PORT_A','PORT_B','TL_ID','SECTION_ID','A_END_VoxID','EXISTING',
        'Protection Type','OEM_NW','Router_Name_B_End','KeyA','KeyB','Complete_Path','optic_id',
        'NSSID_A_End','A END node name','A END port','Z END node name','Z END port',
        'INFINERA_DWDM','NOKIA_DWDM','REMARKS','WAN_LINK_ID','NSSID_B_End','Interface_A_end','Interface_B_end'
    ])

    # ================= NODE CONVERSION =================
    def convert_node(val):
        try:
            if '_' not in str(val) or '(' in str(val):
                return val
            parts = str(val).split('_')
            name = parts[2] if len(parts) >= 3 else ''
            model = str(val)[-7:]
            return f"{name}({model})" if name else ''
        except:
            return ''

    df_result['SOURCE_DXC_NAME'] = df_result['SOURCE_NODE_NAME'].apply(convert_node)
    df_result['NODE_A_POPULAR_NAME'] = df_result['SOURCE_NODE_NAME'].apply(convert_node)
    df_result['SINK_DXC_NAME'] = df_result['SINK_NODE_NAME'].apply(convert_node)
    df_result['NODE_B_POPULAR_NAME'] = df_result['SINK_NODE_NAME'].apply(convert_node)

    # ================= LINK =================
    df_result['LINK_ID'] = df_result['SOURCE_DXC_NAME'].astype(str) + '-' + df_result['SINK_DXC_NAME'].astype(str)
    df_result['LINK_NAME'] = df_result['LINK_ID']
    df_result['SPAN_ID'] = df_result['LINK_ID']
    df_result['DXC_LINK_NAME'] = df_result['LINK_ID']
    df_result['DXC_A_Popular_Name'] = df_result['LINK_ID']
    df_result['DXC_B_Popular_Name'] = df_result['LINK_ID']

    df_result['KeyA'] = df_result['SOURCE_NODE_NAME'].astype(str) + '-' + df_result['PORT_A'].astype(str)
    df_result['KeyB'] = df_result['SINK_NODE_NAME'].astype(str) + '-' + df_result['PORT_B'].astype(str)
    df_result['Complete_Path'] = df_result['KeyA'] + '-' + df_result['KeyB']

    df_result['SOURCE_CIR_NAME'] = df_result['Circle_A_End']
    df_result['SINK_CIRCLE_NAME'] = df_result['Circle_A_End']
    df_result['FIBER_OWNER'] = 'VIL'

    # ================= OEM LOGIC (UPDATED EXACT) =================
    df_result['OEM_NW'] = 'TX'

    df_result.loc[df_result['A_END_VoxID'] == 'NO_VOXID', 'OEM_NW'] = ''

    mask_ason = df_result['Protection Type'].astype(str).str.contains(
        'Unprotected-ASON|Protected-ASON', case=False, na=False
    )
    df_result.loc[mask_ason, 'OEM_NW'] = 'ASON'

    remarks_upper = df_result['REMARKS'].astype(str).str.upper().str.strip()
    nw_upper = df_result['NW_NAME'].astype(str).str.upper().str.strip()

    df_result.loc[remarks_upper.str.endswith('DARK FIBER'), 'OEM_NW'] = 'Dark_Fiber'
    df_result.loc[remarks_upper.str.contains('B2B') | nw_upper.str.contains('B2B'), 'OEM_NW'] = 'B2B'

    # ================= RENAME =================
    df_result.rename(columns={
        'Circle_A_End': 'SPAN_CIR_NAME',
        'Router_Name_A_End': 'SOURCE_ROUTER_NAME',
        'Router_Name_B_End': 'Secondary_Router',
        'NSSID_A_End': 'NSS_ID',
        'NSSID_B_End': 'Secondary_Router_nssid',
        'A_END_VoxID': 'VOX_ID',
        'BSC Name': 'SOURCE_BSC_NAME',
        'Protection Type': 'path_type',
        'A END node name': 'A_END_node_name',
        'A END port': 'A_END_port',
        'Z END node name': 'Z_END_node_name',
        'Z END port': 'Z_END_port'
    }, inplace=True)

    # ================= LINK GROUP =================
    def generate_link_group_id(row):
        if row['EXISTING'] == 'Working':
            return f"{row['VOX_ID']}-MAIN"
        elif row['EXISTING'] == 'Protection':
            return f"{row['VOX_ID']}-PROTECTED"
        return None

    df_result['COMMON_PATH_LINK_GROUP_ID'] = df_result.apply(generate_link_group_id, axis=1)

    result_df_1 = df_result

    # ================= POC =================
    if poc_path:
        df_POC = pd.read_excel(poc_path)

        result_df_1 = result_df_1.merge(
            df_POC[['nss_id','total_4g_sites','site_category','site_name','router','huawei_dwdm','optics_site_direction']],
            how='left',
            left_on='NSS_ID',
            right_on='nss_id'
        ).drop(columns=['nss_id'])

    # ================= POC ROUTER =================
    if poc_router_path:
        df_POC_ROUTER = pd.read_excel(poc_router_path)

        result_df_1 = result_df_1.merge(
            df_POC_ROUTER[['Host Name','4G']],
            how='left',
            left_on='SOURCE_ROUTER_NAME',
            right_on='Host Name'
        ).drop(columns=['Host Name'])

    # ================= RENAME AFTER MERGE =================
    result_df_1.rename(columns={
        'total_4g_sites': 'NSS_ID_SITES',
        'router': 'NSS_ID_WISE_ROUTERS',
        'huawei_dwdm': 'HUAWEI_DWDM',
        '4G': 'router_site_count',
        'WAN_LINK_ID': 'SOURCE_SUPER_WAN_NAME'
    }, inplace=True)

    # ================= CLEAN =================
    result_df_1['Complete_Path'] = result_df_1['Complete_Path'].replace('nan-nan-nan-nan', '')
    result_df_1['KeyA'] = result_df_1['KeyA'].replace('nan-nan', '')
    result_df_1['KeyB'] = result_df_1['KeyB'].replace('nan-nan', '')

    # ================= SECONDARY CATEGORY =================
    result_df_1['Secondary_Router_site_category'] = ''

    if poc_path:
        df_POC.columns = df_POC.columns.str.strip()
        mapping = df_POC.set_index('nss_id')['site_category'].to_dict()
        result_df_1['Secondary_Router_site_category'] = result_df_1['Secondary_Router_nssid'].map(mapping)

    # ================= BSC =================
    if bsc_path:
        df_BSC = pd.read_excel(bsc_path)
        df_BSC.columns = df_BSC.columns.str.strip()

        df_BSC_subset = df_BSC.iloc[:, 0:6]
        bsc_dict = dict(zip(df_BSC_subset.iloc[:, 0], df_BSC_subset.iloc[:, 5]))

        result_df_1['SOURCE_BSC_NAME'] = result_df_1['NSS_ID'].map(bsc_dict)

        mask_blank = result_df_1['SOURCE_BSC_NAME'].isna() | (result_df_1['SOURCE_BSC_NAME'] == '')
        result_df_1.loc[mask_blank, 'SOURCE_BSC_NAME'] = result_df_1.loc[mask_blank, 'Secondary_Router_nssid'].map(bsc_dict)

    # ================= FINAL REINDEX (IMPORTANT 🔥) =================
    
    result_df_1 = result_df_1.reindex(columns=[
    'REC_ID', 'OEM_NAME', 'NW_NAME', 'LINK_ID', 'LINK_NAME', 'SPAN_ID',
    'SOURCE_NODE_NAME', 'NSS_ID_A', 'LAT_A', 'LONG_A',
    'SOURCE_DXC_NAME', 'NODE_A_POPULAR_NAME',

    'SINK_NODE_NAME', 'NSS_ID_B', 'LAT_B', 'LONG_B',
    'SINK_DXC_NAME', 'NODE_B_POPULAR_NAME',

    'SPAN_CIR_NAME', 'SOURCE_CIR_NAME', 'SINK_CIRCLE_NAME',
    'DXC_LINK_NAME', 'DXC_A_Popular_Name', 'DXC_B_Popular_Name',

    'FIBER_OWNER', 'STATUS', 'CREATED_AT', 'UPDATED_AT',
    'CREATED_BY', 'is_approve', 'APPROVED_BY', 'APPROVED_AT',

    'A_END_FROM_HP', 'Z_END_FROM_HP', 'SOURCE_CORE_NAME',
    'SOURCE_BSC_NAME', 'SOURCE_ROUTER_NAME', 'SOURCE_SUPER_WAN_NAME',

    'COMMON_PATH_LINK_GROUP_ID', 'PORT_A', 'PORT_B', 'TL_ID',
    'SECTION_ID', 'VOX_ID', 'EXISTING', 'path_type', 'OEM_NW',

    'Secondary_Router', 'KeyA', 'KeyB', 'Complete_Path',
    'optic_id', 'NSS_ID',

    'NSS_ID_SITES', 'site_category', 'site_name',
    'router_site_count', 'NSS_ID_WISE_ROUTERS',

    'ECI_ACCESS', 'HUAWEI_ACCESS',
    'CIENA_DWDM', 'HUAWEI_DWDM', 'ZTE_DWDM',
    'INFINERA_DWDM', 'NOKIA_DWDM',

    'A_END_node_name', 'A_END_port',
    'Z_END_node_name', 'Z_END_port',

    'optics_site_direction', 'UPADATED_AT', 'REMARKS',

    'Secondary_Router_nssid', 'Secondary_Router_site_category',

    'Interface_A_end', 'Interface_B_end'
])
    result_df_1 = result_df_1.drop_duplicates()

    os.makedirs("media", exist_ok=True)
    output_path = os.path.join("media", f"{circle}_MAIN2_Output.csv")

    result_df_1.to_csv(output_path, index=False)

    print("Data Generated")
    print("Input rows", len(df_result))
    print("Output rows", len(result_df_1))

    return output_path