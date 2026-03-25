import pandas as pd
import os


def run(service_path, tunnel_path, circle):

    # ===============================
    # Load SERVICE report
    # ===============================
    df1 = pd.read_excel(service_path, sheet_name="Sheet2")

    # Strip hidden spaces in header
    df1 = df1.iloc[1:].reset_index(drop=True)
    df1.columns = df1.iloc[0]
    df1 = df1.drop(0).reset_index(drop=True)
    df1.columns = df1.columns.str.strip()

    # Required columns
    main_input = df1.reindex(columns=[
        "ETH_VPN_ID",
        "CUSTOMER_NAME",
        "A_SIDE_NODE",
        "A_END_PORT",
        "Z_SIDE_NODE",
        "Z_END_PORT",
        "TUNNEL"
    ])

    # ===============================
    # Explode Tunnel Column
    # ===============================
    main_input.loc[:, "TUNNEL"] = main_input["TUNNEL"].astype(str).str.split(",")
    df_exploded = main_input.explode("TUNNEL").reset_index(drop=True)

    df_exploded["TUNNEL"] = (
        df_exploded["TUNNEL"].astype(str)
        .str.replace(r"[\'\"\[\]]", "", regex=True)
        .str.split(":", n=1, expand=True)[1]
        .fillna(df_exploded["TUNNEL"])
    )

    df_exploded["TUNNEL"] = df_exploded["TUNNEL"].replace("nan", "")

    # ===============================
    # Load TUNNEL report
    # ===============================
    df2 = pd.read_excel(tunnel_path)

    df2.columns = df2.columns.str.strip()

    df2_input = df2.reindex(columns=[
        "TUNNEL_ID",
        "PROTECTION_ROLE",
        "MAIN_PATH",
        "PROTECTION_PATH"
    ])

    df2_input['TUNNEL_ID'] = df2_input['TUNNEL_ID'].astype(str).str.split(':').str[1]

    # ===============================
    # Merge
    # ===============================
    merged_df = pd.merge(
        df_exploded,
        df2_input,
        left_on="TUNNEL",
        right_on="TUNNEL_ID",
        how="left"
    )

    merged_df = merged_df.drop(columns=["TUNNEL_ID"])

    # ===============================
    # Rename columns
    # ===============================
    merged_df.rename(columns={
        "ETH_VPN_ID": "Service Oid",
        "CUSTOMER_NAME": "CUSTOMER_NAME",
        "A_SIDE_NODE": "A END node name",
        "A_END_PORT": "A END port",
        "Z_SIDE_NODE": "Z END node name",
        "Z_END_PORT": "Z END port",
        "TUNNEL": "Tunnel id",
        "PROTECTION_ROLE": "Protection Type",
        "MAIN_PATH": "Main Path",
        "PROTECTION_PATH": "Prot Path"
    }, inplace=True)

    # ===============================
    # Export
    # ===============================
    output_filename = f"{circle}_access_eci_output.xlsx"
    output_path = os.path.join(output_filename)

    merged_df.to_excel(output_path, index=False)

    print("ECI File generated successfully")

    return output_path