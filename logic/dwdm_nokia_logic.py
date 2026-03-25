import pandas as pd
import os


def run(client_csv_path, circuit_excel_path, circle):

    # ===============================
    # Step 1: Read CLIENT CSV File
    # ===============================
    df = pd.read_csv(
        client_csv_path,
        usecols=[
            "ENTITYID", "SNCNAME", "USERLABEL",
            "ADEVICE", "APTP", "ZDEVICE", "ZPTP",
            "ZRATE", "CONNECTION_ALIAS"
        ]
    )

    # ===============================
    # Step 2: Filter ADEVICE (Circle Based)
    # Example: KER → KE
    # ===============================
    circle_prefix = circle[:2].upper()   # KER → KE
    df_ADevice = df[df['ADEVICE'].str.contains(circle_prefix, na=False)]

    # ===============================
    # Step 3: Filter USERLABEL
    # ===============================
    df_UserLabel = df_ADevice[
        df_ADevice['USERLABEL'].str.contains(circle_prefix[0], na=False)
    ]

    # ===============================
    # Step 4: Add Protection Type
    # ===============================
    df_UserLabel['Protection Type'] = "Protected-ASON"

    # ===============================
    # Step 5: Remove duplicate USERLABEL
    # ===============================
    df_UserLabel_unique = df_UserLabel.drop_duplicates(subset=['USERLABEL'])

    # ===============================
    # Step 6: Read CIRCUIT Excel File
    # ===============================
    df2 = pd.read_excel(
        circuit_excel_path,
        sheet_name="Sheet2",
        usecols=[
            "CLIENT_USERLABEL", "OTS_USERLABEL",
            "OTS_ADEVICE", "OTS_APTP",
            "OTS_ZDEVICE", "OTS_ZPTP"
        ]
    )

    # Take first unique CLIENT_USERLABEL
    if not df2.empty:
        first_client = df2['CLIENT_USERLABEL'].iloc[0]
        df_filtered = df2[df2['CLIENT_USERLABEL'] == first_client].copy()
        df_filtered = df_filtered.drop_duplicates(subset='OTS_USERLABEL')

        df_filtered['PATH_SEGMENT'] = (
            df_filtered['OTS_ADEVICE'] + '-' + df_filtered['OTS_APTP'] +
            '#' +
            df_filtered['OTS_ZDEVICE'] + '-' + df_filtered['OTS_ZPTP']
        )
    else:
        df_filtered = pd.DataFrame()

    # ===============================
    # Step 7: Rename Columns
    # ===============================
    df_UserLabel_unique = df_UserLabel_unique.rename(columns={
        "CONNECTION_ALIAS": "VOX ID",
        "ADEVICE": "A END node name",
        "APTP": "A END port",
        "ZDEVICE": "Z END node name",
        "ZPTP": "Z END port",
        "ZRATE": "Rate"
    })

    # ===============================
    # Final Output
    # ===============================
    output_filename = f"{circle}_dwdm_nokia_output.xlsx"
    output_path = os.path.join(output_filename)

    df_UserLabel_unique.to_excel(output_path, index=False)

    print("DWDM Nokia File Generated")

    return output_path