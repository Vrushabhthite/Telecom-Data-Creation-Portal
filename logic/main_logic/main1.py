def process_main1(
    eci_path=None,
    huawei_path=None,
    ciena_path=None,
    tejas_path=None,
    dwdm_huawei_path=None,
    dwdm_ciena_path=None,
    dwdm_zte_path=None,
    dwdm_nokia_path=None,
    wan_path=None,
    circle=None
):

    import pandas as pd
    import os
    import re

    print("🔹 Loading files...")

    df_eci = pd.read_excel(eci_path) if eci_path else pd.DataFrame()
    df_huawei = pd.read_excel(huawei_path) if huawei_path else pd.DataFrame()
    df_ciena = pd.read_excel(ciena_path) if ciena_path else pd.DataFrame()
    df_tejas = pd.read_excel(tejas_path) if tejas_path else pd.DataFrame()
    df_dwdm_huawei = pd.read_excel(dwdm_huawei_path) if dwdm_huawei_path else pd.DataFrame()
    df_dwdm_ciena = pd.read_excel(dwdm_ciena_path) if dwdm_ciena_path else pd.DataFrame()
    df_dwdm_zte = pd.read_excel(dwdm_zte_path) if dwdm_zte_path else pd.DataFrame()
    df_dwdm_nokia = pd.read_excel(dwdm_nokia_path) if dwdm_nokia_path else pd.DataFrame()
    df_wan = pd.read_excel(wan_path)

    print("✅ Files loaded")

    # -------------------- Column cleanup --------------------
    print("🔹 Stripping column names")

    for df in [
        df_eci, df_huawei, df_ciena, df_tejas,
        df_dwdm_huawei, df_dwdm_ciena, df_dwdm_zte, df_dwdm_nokia, df_wan
    ]:
        if not df.empty:
            df.columns = df.columns.str.strip()

    # -------------------- Filtering --------------------
    print("🔹 Filtering non-WAN data")

    if not df_eci.empty:
        df_eci = df_eci[df_eci['CUSTOMER_NAME'].notna()]

    if not df_huawei.empty:
        df_huawei = df_huawei[df_huawei['ServiceName'].notna()]

    if not df_ciena.empty:
        df_ciena = df_ciena[df_ciena[['SERVICENAME','CUSTOMER_NAME']].notna().any(axis=1)]

    if not df_tejas.empty:
        df_tejas = df_tejas[df_tejas[['SERVICENAME','CUSTOMER_NAME']].notna().any(axis=1)]

    if not df_dwdm_nokia.empty:
        df_dwdm_nokia = df_dwdm_nokia[df_dwdm_nokia['VOX ID'].notna()]

    if not df_dwdm_zte.empty:
        df_dwdm_zte = df_dwdm_zte[df_dwdm_zte['VOX ID'].notna()]

    if not df_dwdm_huawei.empty:
        df_dwdm_huawei = df_dwdm_huawei[
            df_dwdm_huawei[['VOX ID','Remarks','Phase Details']].notna().any(axis=1)
        ]

    if not df_dwdm_ciena.empty:
        df_dwdm_ciena = df_dwdm_ciena[
            df_dwdm_ciena[['Transport Client Name','customerName']].notna().any(axis=1)
        ]

    print(f"✅ WAN rows (BASE): {len(df_wan)}")

    # -------------------- Matching --------------------
    result = []
    match_tracker = {}

    print("🔹 Starting VOXID matching loop")

    for value in df_wan['A_END_VoxID'].unique():

        print("\n----------------------------------------")
        print(f"🔎 Processing VOX ID: {value}")

        match_tracker[value] = 0
        value_str = '' if pd.isna(value) else str(value)

        wan_rows = df_wan[df_wan['A_END_VoxID'] == value]

        for _, wan_row in wan_rows.iterrows():

            def add_matches(df, mask, oem, nw):
                nonlocal result
                matches = df[mask]
                match_tracker[value] += len(matches)
                print(f"  ➜ {oem} matches: {len(matches)}")

                for _, row in matches.iterrows():
                    r = row.copy()
                    r['A_END_VoxID'] = value
                    r['OEM_NAME'] = oem
                    r['NW_NAME'] = nw
                    result.append(r)

            if not df_eci.empty:
                add_matches(df_eci, df_eci['CUSTOMER_NAME'].astype(str).str.contains(value_str, na=False, case=False), 'ECI', 'ECI')

            if not df_huawei.empty:
                add_matches(df_huawei, df_huawei['ServiceName'].astype(str).str.contains(value_str, na=False, case=False), 'Huawei', 'Huawei')

            if not df_ciena.empty:
                add_matches(df_ciena,
                    df_ciena['SERVICENAME'].astype(str).str.contains(value_str, na=False, case=False) |
                    df_ciena['CUSTOMER_NAME'].astype(str).str.contains(value_str, na=False, case=False),
                    'Ciena','Ciena')

            if not df_tejas.empty:
                add_matches(df_tejas,
                    df_tejas['SERVICENAME'].astype(str).str.contains(value_str, na=False, case=False) |
                    df_tejas['CUSTOMER_NAME'].astype(str).str.contains(value_str, na=False, case=False),
                    'Tejas','Tejas')

            if not df_dwdm_nokia.empty:
                add_matches(df_dwdm_nokia, df_dwdm_nokia['VOX ID'].astype(str).str.contains(value_str, na=False), 'Nokia','Nokia Dwdm')

            if not df_dwdm_huawei.empty:
                add_matches(df_dwdm_huawei,
                    df_dwdm_huawei['VOX ID'].astype(str).str.contains(value_str, na=False) |
                    df_dwdm_huawei['Remarks'].astype(str).str.contains(value_str, na=False) |
                    df_dwdm_huawei['Phase Details'].astype(str).str.contains(value_str, na=False),
                    'Huawei','Huawei Dwdm')

            if not df_dwdm_ciena.empty:
                add_matches(df_dwdm_ciena,
                    df_dwdm_ciena['Transport Client Name'].astype(str).str.contains(value_str, na=False) |
                    df_dwdm_ciena['customerName'].astype(str).str.contains(value_str, na=False),
                    'Ciena','Ciena Dwdm')

            if not df_dwdm_zte.empty:
                add_matches(df_dwdm_zte, df_dwdm_zte['VOX ID'].astype(str).str.contains(value_str, na=False), 'ZTE','ZTE Dwdm')

    print("\n🔹 Matching completed")

    result_df = pd.DataFrame(result)

    if 'A_END_VoxID' not in result_df.columns:
        result_df['A_END_VoxID'] = pd.Series(dtype=str)

    df_combined = pd.merge(df_wan, result_df, how='left', on='A_END_VoxID')

    # -------------------- VOX REMARKS --------------------
    def build_remarks(vox, count):
        remarks = []

        if pd.isna(vox) or str(vox).strip() == '' or str(vox).upper() == 'NO_VOXID':
            remarks.append("VOX ID NOT FOUND")
        elif count == 0:
            remarks.append("VOX ID NOT MATCHED")

        vox_str = str(vox).upper()
        if 'DF' in vox_str:
            remarks.append("DARK FIBER")
        if 'BB' in vox_str:
            remarks.append("B2B")

        return '; '.join(remarks)

    df_combined['MATCH_COUNT'] = df_combined['A_END_VoxID'].map(match_tracker).fillna(0)

    print("🔹 Generating VOX remarks")
    df_combined['VOX_REMARKS'] = df_combined.apply(
        lambda r: build_remarks(r['A_END_VoxID'], r['MATCH_COUNT']),
        axis=1
    )

    # -------------------- PATH EXPANSION --------------------
    def extract_nodes_ports(path):
        try:
            s, z = path.split('#')
            sp = s.split('-')
            dp = z.split('-')
            return sp[0], '-'.join(sp[2:]), dp[0], '-'.join(dp[2:])
        except:
            return None, None, None, None

    def expand_rows(df):
        rows = []
        for _, row in df.iterrows():
            base = row.to_dict()
            added = False

            for label, ptype in [('Main Path','Working'), ('Prot Path','Protection')]:
                path = row.get(label)
                if pd.notna(path):
                    for seg in str(path).split('|'):
                        if '#' in seg:
                            sn, sp, dn, dp = extract_nodes_ports(seg)
                            if sn:
                                rows.append({
                                    **base,
                                    'EXISTING': ptype,
                                    'SOURCE_NODE_NAME': sn,
                                    'PORT_A': sp,
                                    'SINK_NODE_NAME': dn,
                                    'PORT_B': dp,
                                    'REMARKS': row.get('VOX_REMARKS','')
                                })
                                added = True

            if not added:
                rows.append({
                    **base,
                    'EXISTING': '',
                    'SOURCE_NODE_NAME': '',
                    'PORT_A': '',
                    'SINK_NODE_NAME': '',
                    'PORT_B': '',
                    'REMARKS': row.get('VOX_REMARKS','No path information')
                })

        return pd.DataFrame(rows)

    df_result = expand_rows(df_combined)

    # -------------------- NEW LOGIC: NSSID MATCH → FORCE B2B --------------------

    # Safe comparison
    nssid_a = df_result['NSSID_A_End'].astype(str).str.strip()
    nssid_b = df_result['NSSID_B_End'].astype(str).str.strip()

    is_blank = (nssid_a == '') | (nssid_b == '')

    is_not_available = (
        (nssid_a.str.upper() == 'NOT_AVAILABLE') |
        (nssid_b.str.upper() == 'NOT_AVAILABLE')
    )

    nssid_match_mask = (
        (nssid_a == nssid_b) &
        ~is_blank &
        ~is_not_available
    )

    nssid_different_mask = (
        (nssid_a != nssid_b) &
        ~is_blank &
        ~is_not_available
    )

    print(f"   ➜ Valid NSSID matches found: {nssid_match_mask.sum()}")

    # -------------------- Store Existing Values --------------------

    df_result['OLD_OEM_NAME'] = df_result['OEM_NAME']
    df_result['OLD_NW_NAME'] = df_result['NW_NAME']

    # -------------------- Apply B2B --------------------

    df_result.loc[nssid_match_mask, 'OEM_NAME'] = 'B2B'
    df_result.loc[nssid_match_mask, 'NW_NAME'] = 'B2B'

    # -------------------- NEW COLUMN: RemarkB2b --------------------

    df_result['RemarkB2b'] = ''

    # Match Case
    for idx in df_result[nssid_match_mask].index:
        old_oem = df_result.at[idx, 'OLD_OEM_NAME']
        old_nw = df_result.at[idx, 'OLD_NW_NAME']

        df_result.at[idx, 'RemarkB2b'] = \
            f"NSSID_A_End==NSSID_B_End=B2B (OEM: {old_oem}→B2B, NW: {old_nw}→B2B)"

    # Different Case
    df_result.loc[nssid_different_mask, 'RemarkB2b'] = 'NSSID not Match'

    # Blank / Not Available
    df_result.loc[is_blank | is_not_available, 'RemarkB2b'] = 'NSSID not Available'

    print("NSSID B2B logic applied successfully")


    # -------------------- FILTER AND WRITE B2B / DARK FIBER --------------------

    remarks_upper = df_result['REMARKS'].astype(str).str.upper().str.strip()
    nwname_upper = df_result['NW_NAME'].astype(str).str.upper().str.strip()

    # Masks
    b2b_mask = remarks_upper.str.contains('B2B', na=False) | nwname_upper.str.contains('B2B', na=False)
    dark_mask = remarks_upper.str.endswith('DARK FIBER', na=False)

    combined_mask = b2b_mask | dark_mask

    # Update OEM & NW
    df_result.loc[b2b_mask, ['OEM_NAME', 'NW_NAME']] = 'B2B'
    df_result.loc[dark_mask, ['OEM_NAME', 'NW_NAME']] = 'Dark Fiber'

    # EXISTING = Working
    df_result.loc[combined_mask, 'EXISTING'] = 'Working'

    # Protection Type = UNPROTECTED
    df_result.loc[combined_mask, 'Protection Type'] = 'UNPROTECTED'

    # -------------------- WAN Mapping --------------------

    df_result.loc[combined_mask, 'SOURCE_NODE_NAME'] = df_result.loc[combined_mask, 'Router_Name_A_End']
    df_result.loc[combined_mask, 'PORT_A'] = df_result.loc[combined_mask, 'ISIS_Interface_A_End']
    df_result.loc[combined_mask, 'SINK_NODE_NAME'] = df_result.loc[combined_mask, 'Router_Name_B_End']
    df_result.loc[combined_mask, 'PORT_B'] = df_result.loc[combined_mask, 'ISIS_Interface_B_End']

    df_result.loc[combined_mask, 'A END node name'] = df_result.loc[combined_mask, 'Router_Name_A_End']
    df_result.loc[combined_mask, 'A END port'] = df_result.loc[combined_mask, 'ISIS_Interface_A_End']
    df_result.loc[combined_mask, 'Z END node name'] = df_result.loc[combined_mask, 'Router_Name_B_End']
    df_result.loc[combined_mask, 'Z END port'] = df_result.loc[combined_mask, 'ISIS_Interface_B_End']


    # -------------------- SAVE --------------------

    output_path = os.path.join(f"{circle}_MAIN1_Output.xlsx")

    df_result.to_excel(output_path, index=False)

    print("FINAL OUTPUT SAVED")
    print("Input rows", len(df_combined))
    print("Output rows", len(df_result))

    return output_path