import pandas as pd
import numpy as np
import os


def run(service_path, tunnel_path, circle):

    # ===============================
    # Read Uploaded Files
    # ===============================
    df1 = pd.read_excel(service_path, sheet_name="Sheet3", header=0)
    df2 = pd.read_excel(tunnel_path, sheet_name="Sheet3", header=0)

    # Ensure CIRCLE_CODE is string and uppercase
    df1['CIRCLE_CODE'] = df1['CIRCLE_CODE'].astype(str).str.upper()
    df2['CIRCLE_CODE'] = df2['CIRCLE_CODE'].astype(str).str.upper()

    # Dynamic Circle Filter
    df1 = df1[df1['CIRCLE_CODE'] == circle.upper()]
    df2 = df2[df2['CIRCLE_CODE'] == circle.upper()]

    # ===============================
    # Select Required Columns
    # ===============================
    df1_cols = df1[[
        "CIRCLE_CODE", "VENDOR", "SERVICEID", "SERVICENAME",
        "MAIN_TUNNEL", "A_SIDE_NODE", "A_SIDE_PORT",
        "Z_SIDE_NODE", "Z_SIDE_PORT"
    ]]

    df2_cols = df2[[
        "MAIN_TUNNEL_ID", "MAIN_TUNNEL_NAME",
        "MAIN_TUNNEL_PATH", "PROTECTION_TUNNEL_ID",
        "PROTECTION_TUNNEL_PATH"
    ]]

    # ===============================
    # Explode MAIN_TUNNEL
    # ===============================
    df1_cols["MAIN_TUNNEL"] = df1_cols["MAIN_TUNNEL"].astype(str)
    df1_cols["MAIN_TUNNEL"] = df1_cols["MAIN_TUNNEL"].str.split(",")

    df_exploded = df1_cols.explode("MAIN_TUNNEL").reset_index(drop=True)

    df1_for_merge = df_exploded.copy()
    df1_for_merge["MAIN_TUNNEL_tmp"] = (
        df1_for_merge["MAIN_TUNNEL"]
        .where(df1_for_merge["MAIN_TUNNEL"].notna(), "__NO_MATCH__")
        .astype(str)
    )

    df2_cols["MAIN_TUNNEL_ID"] = (
        df2_cols["MAIN_TUNNEL_ID"]
        .fillna(0)
        .astype(int)
        .astype(str)
    )

    # ===============================
    # Merge
    # ===============================
    merged_df = pd.merge(
        df1_for_merge,
        df2_cols,
        left_on="MAIN_TUNNEL_tmp",
        right_on="MAIN_TUNNEL_ID",
        how="left"
    )

    # Protection Logic
    merged_df.loc[merged_df["MAIN_TUNNEL_PATH"].notna(), "Protection Type"] = \
        np.where(
            merged_df.loc[merged_df["MAIN_TUNNEL_PATH"].notna(), "PROTECTION_TUNNEL_PATH"].isna(),
            "Working",
            "Protection"
        )

    merged_df = merged_df.drop(columns=["MAIN_TUNNEL_ID"])

    # ===============================
    # Swap A/Z Side (Your Original Logic)
    # ===============================
    merged_df.rename(columns={
        "A_SIDE_NODE": "temp",
        "A_SIDE_PORT": "temp1",
        "Z_SIDE_NODE": "Ztemp",
        "Z_SIDE_PORT": "Ztemp1"
    }, inplace=True)

    merged_df.rename(columns={
        "temp": "Z_SIDE_NODE",
        "temp1": "Z_SIDE_PORT",
        "Ztemp": "A_SIDE_NODE",
        "Ztemp1": "A_SIDE_PORT"
    }, inplace=True)

    # ===============================
    # Final Column Arrangement
    # ===============================
    merged_df = merged_df[[
        "CIRCLE_CODE", "VENDOR", "SERVICEID", "SERVICENAME",
        "A_SIDE_NODE", "A_SIDE_PORT",
        "Z_SIDE_NODE", "Z_SIDE_PORT",
        "Protection Type",
        "MAIN_TUNNEL", "MAIN_TUNNEL_NAME",
        "MAIN_TUNNEL_PATH",
        "PROTECTION_TUNNEL_ID",
        "PROTECTION_TUNNEL_PATH"
    ]]

    merged_df.columns = [
        "CIRCLE_CODE", "VENDOR", "Service Oid", "ServiceName",
        "A END node name", "A END port",
        "Z END node name", "Z END port",
        "Protection Type",
        "MainTunnel Oid", "Tunnel Name",
        "Main Path",
        "ProtectionTunnel Oid", "Prot Path"
    ]

    merged_df = merged_df[[
        "CIRCLE_CODE", "VENDOR", "Service Oid", "ServiceName",
        "A END node name", "A END port",
        "Z END node name", "Z END port",
        "Tunnel Name", "Protection Type",
        "MainTunnel Oid", "Main Path",
        "ProtectionTunnel Oid", "Prot Path"
    ]]

    merged_df[["Main Path", "Prot Path"]] = (
        merged_df[["Main Path", "Prot Path"]]
        .astype(str)
        .apply(lambda col:
               col.str.replace("~", "#", regex=False)
                  .str.replace(" | ", "|", regex=False))
    )

    # ===============================
    # Export
    # ===============================
    output_filename = f"{circle}_access_huawei_output.xlsx"
    output_path = os.path.join(output_filename)

    merged_df.to_excel(output_path, index=False)

    print("Huawei Access File Generated")

    return output_path