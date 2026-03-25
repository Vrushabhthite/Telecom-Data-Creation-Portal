import pandas as pd
import re
import os


def run(service_path, tunnel_path, circle):

    # ===============================
    # Load SERVICE report
    # ===============================
    df = pd.read_excel(
        service_path,
        sheet_name="Sheet3",
        usecols=[
            "CIRCLE_CODE", "VENDOR", "SERVICEID", "SERVICENAME", "CUSTOMER_NAME",
            "TUNNEL", "A_END_NODE", "A_END_PORT", "Z_END_NODE", "Z_END_PORT"
        ]
    )

    # Ensure CIRCLE_CODE is string and uppercase (safe filtering)
    df['CIRCLE_CODE'] = df['CIRCLE_CODE'].astype(str).str.upper()
    df = df[df['CIRCLE_CODE'] == circle.upper()]

    # ===============================
    # Extract fres:<number> safely
    # ===============================
    df["TUNNEL"] = df["TUNNEL"].apply(
        lambda x: re.findall(r"fres:-?\d+", str(x)) if pd.notna(x) else []
    )

    # ===============================
    # Explode multiple tunnels
    # ===============================
    df_output = df.explode("TUNNEL").reset_index(drop=True)

    # Remove empty rows
    df_output = df_output[
        df_output["TUNNEL"].notna() & (df_output["TUNNEL"] != "")
    ]

    # ===============================
    # Load TUNNEL report
    # ===============================
    df2 = pd.read_excel(
        tunnel_path,
        sheet_name="Sheet2",
        usecols=[
            "CIRCLE_CODE", "TUNNEL_ID", "TUNNEL_NAME", "PROTECTION_ROLE",
            "MAIN_TUNNEL_PATH", "PROTECTION_TUNNEL_PATH"
        ]
    )

    df2['CIRCLE_CODE'] = df2['CIRCLE_CODE'].astype(str).str.upper()
    df2 = df2[df2['CIRCLE_CODE'] == circle.upper()]

    # Ensure same datatype
    df2["TUNNEL_ID"] = df2["TUNNEL_ID"].astype(str)
    df_output["TUNNEL"] = df_output["TUNNEL"].astype(str)

    # ===============================
    # Merge
    # ===============================
    df_final = df_output.merge(
        df2,
        how="left",
        left_on="TUNNEL",
        right_on="TUNNEL_ID"
    )

    df_final.drop(columns=["TUNNEL_ID"], inplace=True)

    cols = ["MAIN_TUNNEL_PATH", "PROTECTION_TUNNEL_PATH"]

    df_final[cols] = df_final[cols].apply(
        lambda x: x.astype(str).str.replace(",", "|", regex=False)
    )

    # Rename columns
    df_final.rename(columns={
        "SERVICEID": "Service Oid",
        "A_END_NODE": "A END node name",
        "A_END_PORT": "A END port",
        "Z_END_NODE": "Z END node name",
        "Z_END_PORT": "Z END port",
        "TUNNEL": "Tunnel id",
        "PROTECTION_ROLE": "Protection Type",
        "MAIN_TUNNEL_PATH": "Main Path",
        "PROTECTION_TUNNEL_PATH": "Prot Path"
    }, inplace=True)

    # ===============================
    # Export
    # ===============================
    output_filename = f"{circle}_access_ciena_output.xlsx"
    output_path = os.path.join(output_filename)

    df_final.to_excel(output_path, index=False)
    print("Access Input rows",len(df))
    print("Access Input rows",len(df2))
    print("Output rows",len(df_final))

    print("✅ File generated successfully")

    return output_path