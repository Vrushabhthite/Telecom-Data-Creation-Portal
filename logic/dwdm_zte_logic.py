import pandas as pd
import os


def run(client_excel_path, circle):

    # ===============================
    # Step 1: Read Excel
    # ===============================
    df = pd.read_excel(
        client_excel_path,
        sheet_name="Sheet2",
        usecols=[
            "PARENTED_OSS", "ENTITYID", "SNCNAME", "USERLABEL",
            "ADEVICE", "APTP", "ZDEVICE", "ZPTP",
            "ZCTP", "ENTITYTYPE"
        ]
    )

    circle = circle.upper()
    circle_prefix = circle[:2]  # KER → KE

    # ===============================
    # Step 2: Normalize Columns
    # ===============================
    df["ADEVICE"] = df["ADEVICE"].astype(str).str.upper()
    df["USERLABEL"] = df["USERLABEL"].astype(str).str.upper()

    # ===============================
    # Step 3: Filter ADEVICE
    # Example:
    # KER → KE
    # MAH → MA
    # GUJ → GU
    # ===============================
    df_ADevice = df[df["ADEVICE"].str.startswith(circle_prefix, na=False)]

    print("Rows after ADEVICE filter:", df_ADevice.shape[0])

    # ===============================
    # Step 4: Filter USERLABEL
    # ===============================
    df_UserLabel = df_ADevice[
    df_ADevice["USERLABEL"].str.startswith(circle_prefix[0], na=False)
   ]

    print("Rows after USERLABEL filter:", df_UserLabel.shape[0])

    # ===============================
    # Step 5: Replace Protection Type
    # ===============================
    df_UserLabel["ENTITYTYPE"] = df_UserLabel["ENTITYTYPE"].replace(
        "SNC", "Protected-ASON"
    )

    # ===============================
    # Step 6: Remove Duplicates
    # ===============================
    df_UserLabel_unique = df_UserLabel.drop_duplicates(subset=["USERLABEL"])

    # ===============================
    # Step 7: Rename Columns
    # ===============================
    df_UserLabel_unique = df_UserLabel_unique.rename(columns={
        "USERLABEL": "VOX ID",
        "ADEVICE": "A END node name",
        "APTP": "A END port",
        "ZDEVICE": "Z END node name",
        "ZPTP": "Z END port",
        "ZCTP": "Rate",
        "ENTITYTYPE": "Protection Type"
    })

    # ===============================
    # Step 8: Export
    # ===============================
    output_filename = f"{circle}_dwdm_zte_output.xlsx"
    output_path = os.path.join(output_filename)

    df_UserLabel_unique.to_excel(output_path, index=False)

    print("✅ DWDM ZTE File Generated")
    print("Output rows",len(df_UserLabel_unique))

    return output_path