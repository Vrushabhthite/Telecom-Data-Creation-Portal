# # wandb.py

# import pandas as pd
# import os


# def process_wan_db(file_path,Circle):

#     print("🔹 Loading WAN_DB...")

#     WAN_df1 = pd.read_excel(file_path, sheet_name="WAN_DB")
#     # print("WAN_DB Columns:", WAN_df1.columns.tolist())
#     WAN_df1 = WAN_df1[WAN_df1['Circle'] == Circle]
#     WAN_df1['Remarks for Update'] = ''

#     print("🔹 Loading WAN Physical...")

#     WAN_df = pd.read_excel(file_path, sheet_name="Parent_Child_WAN_DB")
   
#     WAN_df['Router_Lag_Name'] = (
#         WAN_df['Router_Name'].astype(str).str.strip() +
#         WAN_df['Lag_Name'].astype(str).str.strip()
#     )

#     WAN_df['VOXID_A_End'] = WAN_df['VOXID_A_End'].astype(str).str.strip()
#     WAN_df['VOXID_B_End'] = WAN_df['VOXID_B_End'].astype(str).str.strip()

#     expanded_rows = []

#     for idx, row in WAN_df1.iterrows():

#         row_copy = row.copy()
#         row_added = False

#         a_conca = str(row['A End Conca']).strip()
#         b_conca = str(row['B End Conca']).strip()

#         match_a = WAN_df[WAN_df['Router_Lag_Name'] == a_conca]
#         match_b = WAN_df[WAN_df['Router_Lag_Name'] == b_conca]

#         if not match_a.empty or not match_b.empty:

#             if not match_a.empty:
#                 a_row = match_a.iloc[0]

#                 new_intf = a_row['Child_Interface']
#                 if pd.notna(new_intf) and str(new_intf).strip() != '':
#                     if row_copy.get('ISIS_Interface_A_End') != new_intf:
#                         row_copy['ISIS_Interface_A_End'] = new_intf
#                         row_copy['Remarks for Update'] += 'A-End Interface Replaced | '

#                 vox = a_row['VOXID_A_End']
#                 if pd.notna(vox) and str(vox).strip() not in ['', 'NO_VOXID']:
#                     if row_copy.get('VOXID_A_End') != vox:
#                         row_copy['VOXID_A_End'] = vox
#                         row_copy['Remarks for Update'] += 'A-End VOXID Updated | '

#             if not match_b.empty:
#                 b_row = match_b.iloc[0]

#                 new_intf = b_row['Child_Interface']
#                 if pd.notna(new_intf) and str(new_intf).strip() != '':
#                     if row_copy.get('ISIS_Interface_B_End') != new_intf:
#                         row_copy['ISIS_Interface_B_End'] = new_intf
#                         row_copy['Remarks for Update'] += 'B-End Interface Replaced | '

#                 vox = b_row['VOXID_B_End']
#                 if pd.notna(vox) and str(vox).strip() not in ['', 'NO_VOXID']:
#                     if row_copy.get('VOXID_B_End') != vox:
#                         row_copy['VOXID_B_End'] = vox
#                         row_copy['Remarks for Update'] += 'B-End VOXID Updated | '

#             expanded_rows.append(row_copy)
#             row_added = True

#         if not row_added:
#             expanded_rows.append(row_copy)

#     WAN_df_final = pd.DataFrame(expanded_rows)

#     # ================= FINAL CHANGES =================
#     WAN_df_final.rename(columns={"VOXID_A_End": "A_END_VoxID"}, inplace=True)

#     WAN_df_final["A_END_VoxID"] = (
#         WAN_df_final["A_END_VoxID"]
#         .replace(["#N/A", "nan", "NaN"], "")
#         .fillna("")
#         .replace("", "NO_VOXID")
#     )

#     if "WAN_Link_ID.1" in WAN_df_final.columns:
#         WAN_df_final["WAN_Link_ID.1"] = (
#             WAN_df_final["WAN_Link_ID.1"]
#             .replace(["#N/A", "nan", "NaN"], "")
#             .fillna("")
#             .replace("", "NO_WANID")
#         )

#     # ================= SAVE =================

#     os.makedirs("media", exist_ok=True)
#     output_path = os.path.join("media", f"{Circle}_WAN_DB_Output.xlsx")

#     WAN_df_final.to_excel(output_path, index=False)
#     print("WanDB data genrated")
#     return output_path