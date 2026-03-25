#Old Poc data creation logic

# def run_poc(circle, file1, file2, file3, file4, file5):

#     import pandas as pd
#     import numpy as np
#     import os
#     import re

#     print("Loading files...")
#     print("Circle:", circle)
#     print("Files:", file1, file2, file3, file4, file5)

#     # ============================================================
#     # STEP 1: RAN COUNT
#     # ============================================================
#     df1 = pd.read_excel(file1)

#     df1 = df1[df1['Circle'].astype(str).str.contains(circle, case=False, na=False)]

#     df1 = df1.reindex(columns=['Circle', 'NSS_ID', '4G', 'Host Name', 'NEDOMAIN', 'NETWORK_LAYER_NAME'])

#     df1 = df1.groupby(['Circle', 'NSS_ID'], as_index=False).agg({
#         'Host Name': lambda x: ','.join(sorted(set(x.dropna()))),
#         '4G': lambda x: x.fillna(0).sum(),
#         'NEDOMAIN': 'first',
#         'NETWORK_LAYER_NAME': 'first'
#     })

#     df1.rename(columns={'4G': 'Total 4G Sites'}, inplace=True)

#     print("STEP1 df1 rows:", len(df1))

#     # ============================================================
#     # STEP 2: DWDM
#     # ============================================================
#     df4 = pd.read_excel(file2, sheet_name="DWDM")

#     df4 = df4[df4['CIRCLE_CODE'].astype(str).str.contains(circle, case=False, na=False)]

#     df4 = df4.reindex(columns=["CIRCLE_CODE", "VENDOR", "NSS_ID", "ELEMENT_LABEL"])

#     agg_df = df4.groupby(["CIRCLE_CODE", "NSS_ID", "VENDOR"])["ELEMENT_LABEL"] \
#         .apply(lambda x: ', '.join(x)).reset_index()

#     pivot_df = agg_df.pivot(index=["CIRCLE_CODE", "NSS_ID"], columns="VENDOR", values="ELEMENT_LABEL").reset_index()

#     DWDM_df4 = pivot_df.fillna("")
#     DWDM_df4.columns = ['CIRCLE_CODE', 'NSS_ID'] + [f"{col}_DWDM" for col in pivot_df.columns[2:]]
#     DWDM_df4.rename(columns={'CIRCLE_CODE': 'Circle'}, inplace=True)

#     print("STEP2 DWDM rows:", len(DWDM_df4))

#     # ============================================================
#     # STEP 3: ACCESS
#     # ============================================================
#     df5 = pd.read_excel(file2, sheet_name="OPT")
    

#     df5 = df5[df5['CIRCLE_CODE'].astype(str).str.contains(circle, case=False, na=False)]

#     df5 = df5.reindex(columns=["CIRCLE_CODE", "VENDOR", "NSS_ID", "ELEMENT_LABEL"])

#     agg_df = df5.groupby(["CIRCLE_CODE", "NSS_ID", "VENDOR"])["ELEMENT_LABEL"] \
#         .apply(lambda x: ', '.join(x)).reset_index()

#     pivot_df = agg_df.pivot(index=["CIRCLE_CODE", "NSS_ID"], columns="VENDOR", values="ELEMENT_LABEL").reset_index()

#     ACCESS_df5 = pivot_df.fillna("")
#     ACCESS_df5.columns = ['CIRCLE_CODE', 'NSS_ID'] + [f"{col}_ACCESS" for col in pivot_df.columns[2:]]
#     ACCESS_df5.rename(columns={'CIRCLE_CODE': 'Circle'}, inplace=True)

#     print("STEP3 ACCESS rows:", len(ACCESS_df5))

#     # ============================================================
#     # STEP 4: MW TREE
#     # ============================================================
#     df_mw = pd.read_excel(file3)

#     df_mw.columns = df_mw.iloc[0]
#     df_mw = df_mw.drop(0).reset_index(drop=True)
#     df_mw.columns = df_mw.columns.str.strip()

#     df_mw = df_mw[df_mw['CIRCLE'].astype(str).str.contains(circle, case=False, na=False)]

#     df_mw = df_mw.reindex(columns=[
#         'CIRCLE', 'SUBNETWORK_GNE_NSSID', 'OEM',
#         "UNIQUE_DEPENDENT_RAN_SITES_COUNT",
#         "GNE_LOCATED_OPTICS_NODE_NAME", "NE1_ROUTER"
#     ])

#     df_mw['UNIQUE_DEPENDENT_RAN_SITES_COUNT'] = pd.to_numeric(
#         df_mw['UNIQUE_DEPENDENT_RAN_SITES_COUNT'], errors='coerce'
#     )

#     df_mw = df_mw.groupby(['CIRCLE', 'SUBNETWORK_GNE_NSSID'], as_index=False).agg({
#         'OEM': 'first',
#         'UNIQUE_DEPENDENT_RAN_SITES_COUNT': 'sum',
#         "GNE_LOCATED_OPTICS_NODE_NAME": 'first',
#         'NE1_ROUTER': 'first'
#     })

#     df_mw.rename(columns={
#         'UNIQUE_DEPENDENT_RAN_SITES_COUNT': 'Total 4G Sites',
#         'CIRCLE': 'Circle',
#         'SUBNETWORK_GNE_NSSID': 'NSS_ID',
#         "NE1_ROUTER": "ROUTER",
#         "GNE_LOCATED_OPTICS_NODE_NAME": "OPTICS_NODE_NAME"
#     }, inplace=True)

#     print("STEP4 MW rows:", len(df_mw))

#     # ============================================================
#     # STEP 6: optical_df
#     # ============================================================
#     optical_df = pd.concat([ACCESS_df5, DWDM_df4], ignore_index=True)
#     optical_df = optical_df.fillna('')
#     optical_df = optical_df.drop_duplicates(subset='NSS_ID', keep='first')

#     d2_final_unique = df_mw.drop_duplicates(subset='NSS_ID')

#     optical_df['Total 4G Sites'] = optical_df['NSS_ID'].map(
#         d2_final_unique.set_index('NSS_ID')['Total 4G Sites']
#     )

#     print("STEP6 optical rows:", len(optical_df))

#     # ============================================================
#     # STEP 7: MERGE
#     # ============================================================
#     Final_df = df1.merge(
#         optical_df,
#         on='NSS_ID',
#         how='left',
#         suffixes=('_df1', '_optical')
#     )

#     missing_optical = optical_df[~optical_df['NSS_ID'].isin(df1['NSS_ID'])]

#     Final_df = pd.concat([Final_df, missing_optical], ignore_index=True)

#     Final_df['Circle'] = Final_df['Circle_df1'].combine_first(
#         Final_df.get('Circle', Final_df.get('Circle_optical'))
#     )

#     Final_df['Total 4G Sites'] = Final_df['Total 4G Sites_df1'].combine_first(
#         Final_df.get('Total 4G Sites', Final_df.get('Total 4G Sites_optical'))
#     )

#     Final_df['Total 4G Sites'] = (
#         Final_df['Total 4G Sites']
#         .replace(r'^\s*$', 0, regex=True)
#         .fillna(0)
#     )

#     print("STEP7 merged rows:", len(Final_df))

#     # ============================================================
#     # STEP 8: Escalation
#     # ============================================================
#     df6 = pd.read_excel(file4)
#     df_2_final_unique = df6.drop_duplicates(subset='NSS_ID')

#     Final_df['MTx/Pre-Agg/POP'] = Final_df['NSS_ID'].map(
#         df_2_final_unique.set_index('NSS_ID')['Node_Name']
#     )

#     # ============================================================
#     # STEP 9: Site Info
#     # ============================================================
#     df7 = pd.read_excel(file5, sheet_name="NE Type")
#     df_3_final_unique = df7.drop_duplicates(subset='Site NSSID')

#     Final_df['Site Name'] = Final_df['NSS_ID'].map(
#         df_3_final_unique.set_index('Site NSSID')['Site Name']
#     )

#     Final_df['Optics Site Direction '] = Final_df['NSS_ID'].map(
#         df_3_final_unique.set_index('Site NSSID')['Number of Direction']
#     )

#     # ============================================================
#     # STEP 10: POC CATEGORY
#     # ============================================================
#     Final_df['Total 4G Sites_optical'] = pd.to_numeric(
#         Final_df['Total 4G Sites_optical'], errors='coerce'
#     )

#     conditions = [
#         Final_df['MTx/Pre-Agg/POP'].notna() & (Final_df['MTx/Pre-Agg/POP'].astype(str).str.strip() != ""),
#         Final_df['Total 4G Sites'] > 199,
#         Final_df['Total 4G Sites'].between(50, 199),
#         Final_df['Total 4G Sites'].between(15, 49),
#         Final_df['Total 4G Sites'].between(5, 14),
#         Final_df['Total 4G Sites'].between(0, 4)
#     ]

#     choices = ['POC0', 'POC0', 'POC1', 'POC2', 'POC3', 'POC4']
#     Final_df['Site_Category'] = np.select(conditions, choices, default="")

#     # ============================================================
#     # CLEANUP
#     # ============================================================
#     Final_df.drop(columns=['Circle_df1', 'Total 4G Sites_df1'], inplace=True, errors='ignore')

#     Final_df.rename(columns={
#         'Circle_optical': 'Circle',
#         'Host Name': 'Router',
#     }, inplace=True)

#     Final_df.columns = [re.sub(r'\W+', '_', col.strip().lower()) for col in Final_df.columns]

#     if 'total_4g_sites_optical' in Final_df.columns:
#         Final_df.drop(columns=['total_4g_sites_optical'], inplace=True)

#     # ============================================================
#     # SAVE
#     # ============================================================
#     os.makedirs("media", exist_ok=True)
#     output_path = os.path.join("media", f"{circle}_POC_Output.xlsx")

#     Final_df.to_excel(output_path, index=False)

#     print("✅ POC Data Generated")
#     print("Final rows:", len(Final_df))

#     print("Input rows", len(df1))
#     print("Output rows", len(Final_df))

#     return output_path




#Poc data creation 3rd logic
def run_poc(circle, ran_file, scope_file, mw_tree_file, mtx_file, site_info_file):
    import pandas as pd
    import numpy as np
    import os
    import re

    print("Loading files...")
    print("Circle:", circle)

    # ============================================================
    # STEP 1: RAN COUNT
    # ============================================================
    df1 = pd.read_excel(ran_file)

    df1 = df1[df1['Circle'].astype(str).str.contains(circle, case=False, na=False)]

    df1 = df1.reindex(columns=['Circle', 'NSS_ID', '4G', 'Host Name', 'NEDOMAIN', 'NETWORK_LAYER_NAME'])

    df1 = df1.groupby(['Circle', 'NSS_ID'], as_index=False).agg({
        'Host Name': lambda x: ','.join(sorted(set(x.dropna()))),
        '4G': lambda x: x.fillna(0).sum(),
        'NEDOMAIN': 'first',
        'NETWORK_LAYER_NAME': 'first'
    })

    df1.rename(columns={'4G': 'Total 4G Sites'}, inplace=True)

    print("STEP1 rows:", len(df1))

    # ============================================================
    # STEP 2: DWDM (FROM SCOPE FILE)
    # ============================================================
    df4 = pd.read_excel(scope_file, sheet_name="DWDM")

    df4 = df4[df4['CIRCLE_CODE'].astype(str).str.contains(circle, case=False, na=False)]

    df4 = df4.reindex(columns=["CIRCLE_CODE", "VENDOR", "NSS_ID", "ELEMENT_LABEL"])

    agg_df = df4.groupby(["CIRCLE_CODE", "NSS_ID", "VENDOR"])["ELEMENT_LABEL"] \
        .apply(lambda x: ', '.join(x.astype(str))).reset_index()

    pivot_df = agg_df.pivot(index=["CIRCLE_CODE", "NSS_ID"], columns="VENDOR", values="ELEMENT_LABEL").reset_index()
    pivot_df = pivot_df.fillna("")

    vendor_columns = pivot_df.columns[2:]
    pivot_df.columns = ['CIRCLE_CODE', 'NSS_ID'] + [f"{col}_DWDM" for col in vendor_columns]

    vendor_df = df4.groupby(["CIRCLE_CODE", "NSS_ID"])["VENDOR"] \
        .apply(lambda x: ', '.join(sorted(x.unique()))).reset_index()

    DWDM_df4 = pivot_df.merge(vendor_df, on=["CIRCLE_CODE", "NSS_ID"], how="left")
    DWDM_df4.rename(columns={'CIRCLE_CODE': 'Circle'}, inplace=True)

    print("STEP2 DWDM rows:", len(DWDM_df4))

    # ============================================================
    # STEP 3: ACCESS (FROM SCOPE FILE)
    # ============================================================
    df5 = pd.read_excel(scope_file, sheet_name="OPT")

    df5 = df5[df5['CIRCLE_CODE'].astype(str).str.contains(circle, case=False, na=False)]

    df5 = df5.reindex(columns=["CIRCLE_CODE", "VENDOR", "NSS_ID", "ELEMENT_LABEL"])

    agg_df = df5.groupby(["CIRCLE_CODE", "NSS_ID", "VENDOR"])["ELEMENT_LABEL"] \
        .apply(lambda x: ', '.join(x)).reset_index()

    pivot_df = agg_df.pivot(index=["CIRCLE_CODE", "NSS_ID"], columns="VENDOR", values="ELEMENT_LABEL").reset_index()

    vendor_list_df = df5.groupby(["CIRCLE_CODE", "NSS_ID"])["VENDOR"] \
        .apply(lambda x: ', '.join(sorted(x.unique()))).reset_index()

    ACCESS_df5 = pivot_df.merge(vendor_list_df, on=["CIRCLE_CODE", "NSS_ID"], how="left")
    ACCESS_df5 = ACCESS_df5.fillna("")

    vendor_columns = pivot_df.columns[2:]

    ACCESS_df5.columns = (
        ['CIRCLE_CODE', 'NSS_ID'] +
        [f"{col}_ACCESS" for col in vendor_columns] +
        ['VENDOR']
    )

    ACCESS_df5.rename(columns={'CIRCLE_CODE': 'Circle'}, inplace=True)

    print("STEP3 ACCESS rows:", len(ACCESS_df5))

    # ============================================================
    # STEP 4: MERGE
    # ============================================================
    dwdm_cols = ['Circle', 'NSS_ID'] + [col for col in DWDM_df4.columns if col.endswith('_DWDM')]
    access_cols = ['Circle', 'NSS_ID'] + [col for col in ACCESS_df5.columns if col.endswith('_ACCESS')]

    df1 = df1.merge(DWDM_df4[dwdm_cols], on=['Circle', 'NSS_ID'], how='left')
    df1 = df1.merge(ACCESS_df5[access_cols], on=['Circle', 'NSS_ID'], how='left')

    df1 = df1.fillna("")

    print("STEP4 merged rows:", len(df1))

    # ============================================================
    # STEP 8: Escalation (MTX FILE)
    # ============================================================
    df6 = pd.read_excel(mtx_file)
    df6_unique = df6.drop_duplicates(subset='NSS_ID')

    df1['MTx/Pre-Agg/POP'] = df1['NSS_ID'].map(
        df6_unique.set_index('NSS_ID')['Node_Name']
    )

    # ============================================================
    # STEP 9: SITE INFO
    # ============================================================
    df7 = pd.read_excel(site_info_file, sheet_name="NE Type")
    df7_unique = df7.drop_duplicates(subset='Site NSSID')

    df1['Site Name'] = df1['NSS_ID'].map(
        df7_unique.set_index('Site NSSID')['Site Name']
    )

    df1['Optics Site Direction'] = df1['NSS_ID'].map(
        df7_unique.set_index('Site NSSID')['Number of Direction']
    )

    # ============================================================
    # STEP 10: POC CATEGORY
    # ============================================================
    df1['Total 4G Sites'] = pd.to_numeric(df1['Total 4G Sites'], errors='coerce').fillna(0)

    conditions = [
        df1['MTx/Pre-Agg/POP'].notna() & (df1['MTx/Pre-Agg/POP'].astype(str).str.strip() != ""),
        df1['Total 4G Sites'] > 199,
        df1['Total 4G Sites'].between(50, 199),
        df1['Total 4G Sites'].between(15, 49),
        df1['Total 4G Sites'].between(5, 14),
        df1['Total 4G Sites'].between(0, 4)
    ]

    choices = ['POC0', 'POC0', 'POC1', 'POC2', 'POC3', 'POC4']
    df1['Site_Category'] = np.select(conditions, choices, default="")

    # ============================================================
    # CLEANUP
    # ============================================================
    df1.rename(columns={'Host Name': 'Router'}, inplace=True)

    df1.columns = [re.sub(r'\W+', '_', col.strip().lower()) for col in df1.columns]

    # ============================================================
    # SAVE
    # ============================================================
 
    output_path = os.path.join(f"{circle}_POC_Data_Output.xlsx")

    df1.to_excel(output_path, index=False)

    print("✅ POC Data Generated")
    print("Final rows:", len(df1))

    return output_path