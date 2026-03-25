# import numpy as np
# import pandas as pd
# import re
# import os


# def run(service_path, tunnel_path, single_path , circle=None):

#     # ===============================
#     # 1️⃣ Read Trail File
#     # ===============================
#     df = pd.read_excel(service_path)
#     df = df.iloc[6:].reset_index(drop=True)

#     df.columns = df.iloc[0]
#     df = df.drop(0).reset_index(drop=True)
#     df.columns = df.columns.str.strip()

#     df = df.reindex(columns=[
#         "ASON-WDM Trail", "Name", "VOX ID",
#         "Remarks", "Phase Details",
#         "Protection Status", "Source", "Sink"
#     ])

#     df['INDEX_ID'] = np.nan

#     # ===============================
#     # 2️⃣ Read ASON Trail Route File
#     # ===============================
#     file1 = pd.read_excel(tunnel_path)
#     file1 = file1.iloc[6:].reset_index(drop=True)

#     file1.columns = file1.iloc[0]
#     file1 = file1.drop(0).reset_index(drop=True)
#     file1.columns = file1.columns.str.strip()

#     index_map = dict(zip(file1['Name'], file1['Index']))
#     class_map = dict(zip(file1['Name'], file1['Class']))

#     mask = df['ASON-WDM Trail'] == 'Yes'
#     df.loc[mask, 'INDEX_ID'] = df.loc[mask, 'Name'].map(index_map)
#     df.loc[mask, 'Protection Status'] = df.loc[mask, 'Name'].map(class_map)

#     # ===============================
#     # 3️⃣ Read Single Route Specific File
#     # ===============================
#     Single_file = pd.read_excel(single_path)
#     Single_file = Single_file.iloc[6:].reset_index(drop=True)

#     Single_file.columns = Single_file.iloc[0]
#     Single_file = Single_file.drop(0).reset_index(drop=True)
#     Single_file.columns = Single_file.columns.str.strip()

#     df = df.merge(
#         Single_file[['Name', 'Working Route', 'Protection Route', 'Rate']],
#         on='Name',
#         how='left'
#     )

#     df['Protection Status'] = df['Protection Status'].replace({
#         "Diamond": "Protected-ASON",
#         "Copper": "Unprotected-ASON",
#         "Silver": "Unprotected-ASON"
#     })

#     df['Rate'] = df['Rate'].replace({
#         "ODU0": "1.25 G",
#         "ODU1": "2.5 G",
#         "ODU2": "10 G",
#         "ODU3": "40 G",
#         "ODU4": "100 G",
#         "ODUcn": "200 G"
#     })

#     df.rename(columns={
#         'Protection Status': 'Protection Type',
#         'Working Route': 'Main Path',
#         'Protection Route': 'Prot Path'
#     }, inplace=True)

#     # ===============================
#     # Extract Cleaned Paths
#     # ===============================
#     df['Cleaned_Main'] = df['Main Path'].astype(str).str.extract(
#         r'Positive\s*([\s\S]*?)\s*Negative', expand=False)

#     df['Cleaned_Prot'] = df['Prot Path'].astype(str).str.extract(
#         r'Positive\s*([\s\S]*?)\s*Negative', expand=False)

#     # ===============================
#     # Extract A End
#     # ===============================
#     df['Temp'] = df['Source'].str.split(r'-(?=shelf)', regex=True)
#     second = df['Temp'].str[-1]
#     mask = second.isna()
#     df.loc[mask, 'Temp'] = df.loc[mask, 'Source'].str.split('-')

#     df['First_part'] = df['Temp'].str[0]
#     df['A END port'] = df['Temp'].str[-1]

#     df['split_first_part'] = df['First_part'].str.split('-')
#     second = df['split_first_part'].str[-1]
#     length_second = second.fillna("").astype(str).str.len()

#     mask = second.notna() & ((length_second == 29) | (length_second == 20))
#     df['A END node name'] = np.where(mask, second, df['First_part'])

#     # ===============================
#     # Extract Z End
#     # ===============================
#     df['Temp'] = df['Sink'].str.split(r'-(?=shelf)', regex=True)
#     second = df['Temp'].str[-1]
#     mask = second.isna()
#     df.loc[mask, 'Temp'] = df.loc[mask, 'Sink'].str.split('-')

#     df['First_part'] = df['Temp'].str[0]
#     df['Z END port'] = df['Temp'].str[-1]

#     df['split_first_part'] = df['First_part'].str.split('-')
#     second = df['split_first_part'].str[-1]
#     length_second = second.fillna("").astype(str).str.len()

#     mask = second.notna() & ((length_second == 29) | (length_second == 20))
#     df['Z END node name'] = np.where(mask, second, df['First_part'])

#     # ===============================
#     # Final Cleanup
#     # ===============================
#     df.drop(['Main Path', 'Prot Path'], axis=1, inplace=True)

#     df.rename(columns={
#         'Cleaned_Main': 'Main Path',
#         'Cleaned_Prot': 'Prot Path'
#     }, inplace=True)

#     df.drop(['ASON-WDM Trail', 'Temp', 'Source', 'Sink'], axis=1, inplace=True)

#     df = df[[
#         "INDEX_ID", "Name", "VOX ID", "Remarks", "Phase Details",
#         "Protection Type",
#         "A END node name", "A END port",
#         "Z END node name", "Z END port",
#         "Main Path", "Prot Path", "Rate"
#     ]]

#     # ===============================
#     # Export
#     # ===============================
#     output_filename = f"{circle}_dwdm_huawei_output.xlsx" if circle else "dwdm_huawei_output.xlsx"
#     output_path = os.path.join(output_filename)

#     df.to_excel(output_path, index=False)


#     return output_path


def run(service_path, tunnel_path, single_path, circle=None):
    import numpy as np
    import pandas as pd
    import os

    print("🔹 Loading files...")

    # ===============================
    # Helper: Auto Read (CSV or Excel)
    # ===============================
    def read_file(path):
        if path.endswith(".csv"):
            return pd.read_csv(path, skiprows=9, low_memory=False)
        else:
            df = pd.read_excel(path)
            df = df.iloc[6:].reset_index(drop=True)
            df.columns = df.iloc[0]
            df = df.drop(0).reset_index(drop=True)
            return df

    # ===============================
    # 1️⃣ SERVICE FILE
    # ===============================
    df = read_file(service_path)
    df.columns = df.columns.str.strip()

    df = df.reindex(columns=[
        "ASON-WDM Trail","Name","VOX ID","Remarks",
        "Phase Details","Protection Status","Source","Sink"
    ])

    df['INDEX_ID'] = np.nan

    # ===============================
    # 2️⃣ TUNNEL FILE
    # ===============================
    file1 = read_file(tunnel_path)
    file1.columns = file1.columns.str.strip()
    file1 = file1.drop_duplicates(subset=['Name'])

    index_map = dict(zip(file1['Name'], file1['Index']))
    class_map = dict(zip(file1['Name'], file1['Class']))

    mask = df['ASON-WDM Trail'] == 'Yes'
    df.loc[mask, 'INDEX_ID'] = df.loc[mask, 'Name'].map(index_map)
    df.loc[mask, 'Protection Status'] = df.loc[mask, 'Name'].map(class_map)

    # ===============================
    # 3️⃣ SINGLE FILE
    # ===============================
    df_single = read_file(single_path)
    df_single.columns = df_single.columns.str.strip()
    df_single = df_single.drop_duplicates(subset=['Name'])

    df = df.merge(
        df_single[['Name','Working Route','Protection Route','Rate']],
        on='Name',
        how='left'
    )

    # ===============================
    # Replace Values
    # ===============================
    df['Protection Status'] = df['Protection Status'].replace({
        "Diamond": "Protected-ASON",
        "Copper": "Unprotected-ASON",
        "Silver": "Unprotected-ASON"
    })

    df['Rate'] = df['Rate'].replace({
        "ODU0": "1.25 G",
        "ODU1": "2.5 G",
        "ODU2": "10 G",
        "ODU3": "40 G",
        "ODU4": "100 G",
        "ODUcn": "200 G"
    })

    # ===============================
    # Rename
    # ===============================
    df = df.rename(columns={
        'Protection Status': 'Protection Type',
        'Working Route': 'Main Path',
        'Protection Route': 'Prot Path'
    })

    df = df[
        ["INDEX_ID","ASON-WDM Trail","Name","VOX ID","Remarks",
         "Phase Details","Protection Type","Main Path","Prot Path",
         "Rate","Source","Sink"]
    ]

    # ===============================
    # Extract Paths
    # ===============================
    df['Cleaned_Main'] = df['Main Path'].astype(str).str.extract(
        r'Positive\s*([\s\S]*?)\s*Negative', expand=False
    )

    df['Cleaned_Prot'] = df['Prot Path'].astype(str).str.extract(
        r'Positive\s*([\s\S]*?)\s*Negative', expand=False
    )

    # ===============================
    # A END
    # ===============================
    df['Temp'] = df['Source'].str.split(r'-(?=shelf)', regex=True)
    mask = df['Temp'].str[-1].isna()
    df.loc[mask, 'Temp'] = df.loc[mask, 'Source'].str.split('-')

    df['First_part'] = df['Temp'].str[0]
    df['A END port'] = df['Temp'].str[-1]

    second = df['First_part'].str.split('-').str[-1]
    length = second.fillna("").astype(str).str.len()

    df['A END node name'] = np.where(
        second.notna() & ((length == 29) | (length == 20)),
        second,
        df['First_part']
    )

    # ===============================
    # Z END
    # ===============================
    df['Temp'] = df['Sink'].str.split(r'-(?=shelf)', regex=True)
    mask = df['Temp'].str[-1].isna()
    df.loc[mask, 'Temp'] = df.loc[mask, 'Sink'].str.split('-')

    df['First_part'] = df['Temp'].str[0]
    df['Z END port'] = df['Temp'].str[-1]

    second = df['First_part'].str.split('-').str[-1]
    length = second.fillna("").astype(str).str.len()

    df['Z END node name'] = np.where(
        second.notna() & ((length == 29) | (length == 20)),
        second,
        df['First_part']
    )

    # ===============================
    # Final Cleanup
    # ===============================
    df.drop(['Main Path','Prot Path'], axis=1, inplace=True)

    df = df.rename(columns={
        'Cleaned_Main': 'Main Path',
        'Cleaned_Prot': 'Prot Path'
    })

    df.drop(['ASON-WDM Trail','Temp','Source','Sink'], axis=1, inplace=True)

    df = df[
        ["INDEX_ID","Name","VOX ID","Remarks","Phase Details","Protection Type",
         "A END node name","A END port","Z END node name","Z END port",
         "Main Path","Prot Path","Rate"]
    ]

    # ===============================
    # SAVE
    # ===============================
    os.makedirs("media", exist_ok=True)
    output_path = os.path.join("media", f"{circle}_dwdm_output.xlsx")

    df.to_excel(output_path, index=False)

    print("✅ File Generated Successfully")
  

    print("FINAL OUTPUT SAVED")
    print("Input rows", len(df))
    print("Output rows", len(df))

    return output_path