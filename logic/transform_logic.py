import pandas as pd
import os

def run_transform(input_path, circle, transform_type):

    print("📂 Input file:", input_path)

    # ================= READ FILE =================
    try:
        if input_path.endswith(".csv"):
            df = pd.read_csv(input_path, encoding='ISO-8859-1', low_memory=False)

        elif input_path.endswith(".xlsx") or input_path.endswith(".xls"):
            df = pd.read_excel(input_path, engine="openpyxl")

        else:
            raise ValueError("Unsupported file format")

    except Exception as e:
        print("File read error:", str(e))
        raise ValueError("Invalid or corrupted file")

    # ================= SELECT LOGIC =================
    if transform_type == "exclation":
        result = exclation_matrix(df)

    elif transform_type == "bsc":
        result = super_bsc(df)

    elif transform_type == "router":
        result = super_router(df)

    elif transform_type == "wan":
        result = super_wan(df)

    elif transform_type == "vooxid":
        result = vooxid(df)

    else:
        raise ValueError("Invalid transform type")

    # ================= SAVE FILE =================
    output_path = f"{circle}_{transform_type}.csv"   # ❌ removed media folder

    result.to_csv(output_path, index=False)

    return output_path

# =========================================================
# ================= EXCLATION MATRIX ======================
# =========================================================

def exclation_matrix(df):

    output_df = (
        df.groupby([
            'SOURCE_CIR_NAME','VOX_ID','SOURCE_ROUTER_NAME',
            'SOURCE_SUPER_WAN_NAME','NW_NAME','EXISTING'
        ])['LINK_ID']
        .apply(lambda x: '_'.join(x.astype(str)))
        .reset_index()
    )

    output_df = output_df.rename(columns={
        'VOX_ID': 'VOXID',
        'SOURCE_ROUTER_NAME': 'ROUTER_NAME',
        'SOURCE_SUPER_WAN_NAME': 'SUPER_WAN_NAME',
        'EXISTING': 'PATH_TYPE',
        'LINK_ID': 'LINK_ID_PATH_DETAILS',
        'SOURCE_CIR_NAME': 'Owner_Circle'
    })

    output_df['NDS_IP_WAN_Link_Name'] = output_df['VOXID']

    output_df['Incident_Type'] = output_df['PATH_TYPE'].map({
        'Working': '10G WAN Link Down',
        'Protection': '10G WAN Link on Threat'
    })

    output_df = output_df.drop(['PATH_TYPE', 'LINK_ID_PATH_DETAILS'], axis=1)

    # ================= COLUMN ORDER (SAME) =================
    output_df = output_df.reindex(columns=[
        'esc_ID','zone_name','VOXID','ROUTER_NAME','SUPER_WAN_NAME','NW_NAME',
        'NDS_IP_WAN_Link_Name','Router_A','Router_B','Signalling_Type','Incident_Type','Owner_Circle',
        'PERSON1','MOB1','EMAIL1','PERSON2','MOB2','EMAIL2','PERSON3','MOB3','EMAIL3',
        'PERSON4','MOB4','EMAIL4','ADDITIONAL_EMAIL','UPDATED_BY','UPDATED_TIME',
        'L2PERSON1','L2PERSON2','L2PERSON3','L2PERSON4','L2MOB1','L2MOB2','L2MOB3','L2MOB4',
        'L2EMAIL1','L2EMAIL2','L2EMAIL3','L2EMAIL4',
        'L3PERSON1','L3PERSON2','L3PERSON3','L3PERSON4','L3MOB1','L3MOB2','L3MOB3','L3MOB4',
        'L3EMAIL1','L3EMAIL2','L3EMAIL3','L3EMAIL4',
        'CREATED_AT','UPDATED_AT','CREATED_BY'
    ])

    # ================= STATIC VALUES =================
    output_df['Router_A'] = output_df['VOXID']
    output_df['Router_B'] = output_df['VOXID']
    output_df['Signalling_Type'] = 'WAN LINK'
    output_df['zone_name'] = 'West'
    output_df['esc_ID'] = range(1, len(output_df) + 1)

    # L1
    output_df['PERSON1'] = 'Sandeep Chaturvedi'
    output_df['MOB1'] = '8411008375'
    output_df['EMAIL1'] = 'sandeep.chaturvedi2@vodafoneidea.com'

    output_df['PERSON2'] = 'Sandeep Singh'
    output_df['MOB2'] = '9823003350'
    output_df['EMAIL2'] = 'sandeep.singh@vodafoneidea.com'

    output_df['PERSON3'] = 'Manoj Markose'
    output_df['MOB3'] = '9819818923'
    output_df['EMAIL3'] = 'Manoj.Markose@vodafoneidea.com'

    output_df['PERSON4'] = 'Jitendra Swarnkar'
    output_df['MOB4'] = '8411007890'
    output_df['EMAIL4'] = 'jitendra.swarnkar@vodafoneidea.com'

    output_df['ADDITIONAL_EMAIL'] = 'snoctx.shifthead3@vodafoneidea.com'

    # L2
    output_df['L2PERSON1'] = output_df['PERSON1']
    output_df['L2PERSON2'] = output_df['PERSON2']
    output_df['L2PERSON3'] = output_df['PERSON3']
    output_df['L2PERSON4'] = output_df['PERSON4']

    output_df['L2MOB1'] = output_df['MOB1']
    output_df['L2MOB2'] = output_df['MOB2']
    output_df['L2MOB3'] = output_df['MOB3']
    output_df['L2MOB4'] = output_df['MOB4']

    output_df['L2EMAIL1'] = output_df['EMAIL1']
    output_df['L2EMAIL2'] = output_df['EMAIL2']
    output_df['L2EMAIL3'] = output_df['EMAIL3']
    output_df['L2EMAIL4'] = output_df['EMAIL4']

    # L3
    output_df['L3PERSON1'] = output_df['PERSON1']
    output_df['L3PERSON2'] = output_df['PERSON2']
    output_df['L3PERSON3'] = output_df['PERSON3']
    output_df['L3PERSON4'] = output_df['PERSON4']

    output_df['L3MOB1'] = output_df['MOB1']
    output_df['L3MOB2'] = output_df['MOB2']
    output_df['L3MOB3'] = output_df['MOB3']
    output_df['L3MOB4'] = output_df['MOB4']

    output_df['L3EMAIL1'] = output_df['EMAIL1']
    output_df['L3EMAIL2'] = output_df['EMAIL2']
    output_df['L3EMAIL3'] = output_df['EMAIL3']
    output_df['L3EMAIL4'] = output_df['EMAIL4']
    output_df['CIR_NAME']=  output_df['Owner_Circle']
    
    print("exceration  Data Generated Successfully")
    print("Input rows:", len(df))
    print("Output rows:", len( output_df))

    return output_df

# ================= SUPER BSC =================

def super_bsc(df):

    import pandas as pd

    # ================= COLUMN CLEAN =================
    df.columns = df.columns.str.strip()

    print("🔹 Original rows:", len(df))

    # ================= SPLIT + EXPLODE (EXACT SAME) =================
    df['SOURCE_BSC_NAME'] = (
        df['SOURCE_BSC_NAME']
        .astype(str)
        .str.split(',')
    )

    df = df.explode('SOURCE_BSC_NAME')

    df['SOURCE_BSC_NAME'] = df['SOURCE_BSC_NAME'].str.strip()

    print("🔹 Rows after explode:", len(df))

    # ================= GROUP =================
    result = df.groupby('SOURCE_BSC_NAME').agg(
        VOX_IDs=('VOX_ID', lambda x: ','.join(sorted(set(x.dropna().astype(str))))),
        ROUTER_NAME=('SOURCE_ROUTER_NAME', lambda x: ','.join(sorted(set(x.dropna().astype(str))))),
        WAN_NAME=('SOURCE_SUPER_WAN_NAME', lambda x: ','.join(sorted(set(x.dropna().astype(str))))),
        count=('VOX_ID', 'nunique'),
        NW_NAME=('NW_NAME', 'first'),
        CIR_NAME=('SPAN_CIR_NAME', 'first'),
        BSC_NAME=('SOURCE_BSC_NAME', 'first'),
        LINK_IDs=('COMMON_PATH_LINK_GROUP_ID', lambda x: ','.join(sorted(set(x.dropna().astype(str)))))
    ).reset_index(drop=True)

    # ================= ADD FIELD =================
    result['WAN_LINK_ID'] = result['WAN_NAME']

    # ================= COLUMN ORDER =================
    result = result.reindex(columns=[
        'id', 'BSC_NAME', 'ROUTER_NAME', 'WAN_NAME', 'NW_NAME',
        'VOX_IDs', 'count', 'LINK_IDs', 'WAN_LINK_ID', 'CIR_NAME'
    ])

    # ================= REMOVE INVALID ROWS (EXACT SAME) =================
    result = result.drop(result[result['BSC_NAME'].isna()].index)
    result = result.drop(result[result['BSC_NAME'].astype(str).str.lower() == 'nan'].index)

    # ================= ID COLUMN =================
    result['id'] = ''

    # ================= LOG =================
    print("Input rows:", len(df))
    print("Output rows:", len(result))
    print("Super BSC Output generated successfully")

    return result



def super_router(df):

    import pandas as pd

    print("🔹 Input rows:", len(df))

    # -------------------------------
    # STEP 1: Normalize routers
    # -------------------------------
    df_source = df.copy()
    df_source['ROUTER_NAME'] = df_source['SOURCE_ROUTER_NAME']

    df_secondary = df.copy()
    df_secondary['ROUTER_NAME'] = df_secondary['Secondary_Router']

    # Combine both
    df_router = pd.concat([df_source, df_secondary], ignore_index=True)

    # Remove blank routers
    df_router['ROUTER_NAME'] = df_router['ROUTER_NAME'].astype(str).str.strip()
    df_router = df_router[
        (df_router['ROUTER_NAME'] != '') &
        (df_router['ROUTER_NAME'].str.upper() != 'NAN')
    ]
    
    print("🔹 Router normalized rows:", len(df_router))

    # -------------------------------
    # STEP 2: Group by ROUTER_NAME
    # -------------------------------
    result = df_router.groupby('ROUTER_NAME').agg(

        VOX_IDs=(
            'VOX_ID',
            lambda x: ','.join(sorted(set(x.dropna().astype(str))))
        ),

        SUPER_WAN_NAME=(
            'SOURCE_SUPER_WAN_NAME',
            lambda x: ','.join(sorted(set(x.dropna().astype(str))))
        ),

        count=(
            'VOX_ID',
            'nunique'
        ),

        NW_NAME=('NW_NAME', 'first'),

        BSC_NAME=(
            'SOURCE_BSC_NAME',
            lambda x: ','.join(sorted(set(x.dropna().astype(str))))
        ),
        
        CIR_NAME=('SPAN_CIR_NAME', 'first'),

        LINK_IDs=(
            'COMMON_PATH_LINK_GROUP_ID',
            lambda x: ','.join(sorted(set(x.dropna().astype(str))))
        )

    ).reset_index()

    # -------------------------------
    # STEP 3: Add required columns
    # -------------------------------
    # result['id'] = range(1, len(result) + 1)
    result['common_count'] = ''
    result['created_at'] = ''
    result['updated_at'] = ''
    result['site_category'] = ''
    result['NSS_ID'] = ''
    result['LAT-A'] = ''
    result['LONG-A'] = ''
    result['GENERIC_NAME'] = ''

    # -------------------------------
    # STEP 4: Reorder columns
    # -------------------------------
    result = result.reindex(columns=[
        'id', 'ROUTER_NAME', 'SUPER_WAN_NAME', 'BSC_NAME', 'NW_NAME',
        'VOX_IDs', 'count', 'common_count', 'LINK_IDs',
        'site_category', 'NSS_ID', 'LAT-A', 'LONG-A', 'GENERIC_NAME',
        'created_at', 'updated_at', 'CIR_NAME'
    ])

    # -------------------------------
    # FINAL LOGS
    # -------------------------------
    print("🔹 Unique Routers:", result['ROUTER_NAME'].nunique())
    print("Input rows:", len(df))
    print("Output rows:", len(result))
    print("Super Router Output generated successfully")

    return result



# ================= SUPER WAN =================
def super_wan(df):

    import pandas as pd
    import numpy as np

    print("🔹 Input rows:", len(df))

    # Normalize SUPER WAN NAME
    df['SOURCE_SUPER_WAN_NAME'] = (
        df['SOURCE_SUPER_WAN_NAME']
        .astype(str)
        .str.strip()
    )

    # ----------------------------------------------------
    # 🔑 CREATE UNIQUE GROUP KEY (CRITICAL FIX)
    # ----------------------------------------------------
    mask_invalid_wan = (
        df['SOURCE_SUPER_WAN_NAME'].isna() |
        (df['SOURCE_SUPER_WAN_NAME'] == '') |
        (df['SOURCE_SUPER_WAN_NAME'].str.upper() == 'NO_WANID')
    )

    df['WAN_GROUP_KEY'] = df['SOURCE_SUPER_WAN_NAME']

    df.loc[mask_invalid_wan, 'WAN_GROUP_KEY'] = (
        'NO_WANID_' + df.loc[mask_invalid_wan].index.astype(str)
    )

    # ----------------------------------------------------
    # GROUP & AGGREGATE (NO DATA LOSS)
    # ----------------------------------------------------
    result = df.groupby('WAN_GROUP_KEY', as_index=False).agg(

        SUPER_WAN_NAME=('SOURCE_SUPER_WAN_NAME', 'first'),

        VOX_IDs=(
            'VOX_ID',
            lambda x: ','.join(sorted(set(
                str(i) for i in x if pd.notna(i) and str(i).strip() != ''
            )))
        ),
        
        CIR_NAME=('SPAN_CIR_NAME', 'first'),

        NW_NAME=('NW_NAME', 'first'),

        count=('VOX_ID', 'nunique'),

        LINK_IDs=(
            'LINK_ID',
            lambda x: ','.join(sorted(set(
                str(i) for i in x if pd.notna(i) and str(i).strip() != ''
            )))
        )

    )

    # Add ID
    result['id'] = range(1, len(result) + 1)

    # Reorder columns
    result = result[[
        'id',
        'SUPER_WAN_NAME',
        'NW_NAME',
        'VOX_IDs',
        'count',
        'LINK_IDs',
        'CIR_NAME'
    ]]

    # -------------------------------
    # FINAL LOGS
    # -------------------------------
    print("Super Wan Data Generated")
    print("✅ File generated successfully")
    print("Input rows :", len(df))
    print("Output rows:", len(result))

    return result
    


#================= VOXID =================



def vooxid(df):

    import numpy as np

    # ================= NORMALIZE =================
    df['VOX_ID'] = df['VOX_ID'].astype(str).str.strip()
    df['SOURCE_SUPER_WAN_NAME'] = df['SOURCE_SUPER_WAN_NAME'].astype(str).str.strip()

    # Safe columns
    df['PATH'] = df.get('path_type', '').fillna('')
    df['PATH_TYPE'] = df.get('EXISTING', '').fillna('')

    # ================= HANDLE INVALID VOX_ID =================
    mask_invalid_vox = (
        df['VOX_ID'].isna() |
        (df['VOX_ID'] == '') |
        (df['VOX_ID'].str.upper() == 'NO_VOXID')
    )

    df['VOX_GROUP_KEY'] = df['VOX_ID']

    df.loc[mask_invalid_vox, 'VOX_GROUP_KEY'] = (
        'NO_VOXID_' + df.loc[mask_invalid_vox].index.astype(str)
    )

    # ================= GROUPING =================
    result = (
        df
        .groupby(['VOX_GROUP_KEY', 'PATH_TYPE'], as_index=False)
        .agg(
            VOX_ID=('VOX_ID', 'first'),

            WAN_LINK_ID=(
                'SOURCE_SUPER_WAN_NAME',
                lambda x: ','.join(
                    sorted(set(str(i) for i in x if str(i).lower() not in ['nan', '']))
                )
            ),

            CIR_NAME=('SPAN_CIR_NAME', 'first'),

            LINK_ID_PATH_DETAILS=(
                'LINK_ID',
                lambda x: ','.join(
                    sorted(set(str(i) for i in x if str(i).lower() not in ['nan', '']))
                )
            ),

            PATH=('PATH', 'first'),
        )
    )

    # ================= STATIC FIELDS =================
    result['NDS_LINK_NAME'] = result['VOX_ID']
    result['ent_link'] = 'VIL'
    result['id'] = range(1, len(result) + 1)
    result['PATH_ID'] = range(1, len(result) + 1)
    result['STATUS'] = '1'
    result['wan_type'] = 'POC'
    result['CREATED_AT'] = ''
    result['UPDATED_AT'] = ''
    result['PATH_DETAIL'] = ''

    # ================= FINAL COLUMN ORDER =================
    final_columns = [
        'id', 'ent_link', 'VOX_ID', 'WAN_LINK_ID', 'NDS_LINK_NAME',
        'PATH_ID', 'LINK_ID_PATH_DETAILS', 'PATH_TYPE',
        'PATH_DETAIL', 'STATUS', 'wan_type', 'CREATED_AT',
        'UPDATED_AT', 'PATH', 'CIR_NAME'
    ]

    result = result[final_columns]

    # Optional: blank id (as per your script)
    result['id'] = ''

    print("VoX_ID Data Generated Successfully")
    print("Input rows:", len(df))
    print("Output rows:", len(result))
    print(result)
    return result