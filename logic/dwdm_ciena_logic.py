import pandas as pd
import ast
import re
import os


def run(service_path, circle=None):
    print("File Uploading")
    # ===============================
    # Read Uploaded File
    # ===============================
    df = pd.read_excel(service_path)
    
    # Filter required records
    df = df[
        (df['Service Class'] == 'Transport Client') &
        (df['Rate'].isin(['100GE', '10GE']))
    ]

    df = df.reindex(columns=[
        "Serial Number",
        "Transport Client Name",
        "customerName",
        "Source",
        "Sink",
        "Resiliency",
        "Path Details"
    ])

    # ===============================
    # Split Source → A END
    # ===============================
    df['Source_clean'] = df['Source'].str.replace(r"[\[\]']", "", regex=True)
    split_df = df['Source_clean'].str.split('::', n=1, expand=True)

    if split_df.shape[1] == 2:
        df['A END node name'] = split_df[0]
        df['A END port'] = split_df[1]
    else:
        df['A END node name'] = df['Source_clean']
        df['A END port'] = ""

    df = df.drop(['Source_clean', 'Source'], axis=1)

    # ===============================
    # Split Sink → Z END
    # ===============================
    df['Sink_clean'] = df['Sink'].str.replace(r"[\[\]']", "", regex=True)
    split_df = df['Sink_clean'].str.split('::', n=1, expand=True)

    if split_df.shape[1] == 2:
        df['Z END node name'] = split_df[0]
        df['Z END port'] = split_df[1]
    else:
        df['Z END node name'] = df['Sink_clean']
        df['Z END port'] = ""

    df = df.drop(['Sink_clean', 'Sink'], axis=1)

    # ===============================
    # Clean Path Details
    # ===============================
    def clean_path_details(path_str):
        if not isinstance(path_str, str):
            return path_str
        pos = path_str.find(", 'Rx Direction'")
        if pos == -1:
            return path_str
        return path_str[:pos] + "]"

    df['Path Details'] = df['Path Details'].apply(clean_path_details)

    # ===============================
    # Create Main / Prot Path
    # ===============================
    df['Main Path'] = None
    df['Prot Path'] = None

    result_rows = []
    for sn, group in df.groupby('Serial Number'):
        if len(group) == 1:
            row = group.iloc[0].copy()
            row['Main Path'] = row['Path Details']
            row['Prot Path'] = ''
            row = row.drop(labels='Path Details')
            result_rows.append(row)
        else:
            first_row = group.iloc[0].copy()
            second_row = group.iloc[1].copy()
            first_row['Main Path'] = first_row['Path Details']
            first_row['Prot Path'] = second_row['Path Details']
            first_row = first_row.drop(labels='Path Details')
            result_rows.append(first_row)

    df = pd.DataFrame(result_rows)

    # ===============================
    # Convert Path Lists
    # ===============================
    def safe_eval_list(x):
        if isinstance(x, list):
            return x
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except Exception:
                return []
        return []

    def convert_list_to_path_string(lst):
        if not isinstance(lst, list) or len(lst) == 0:
            return ""
        if lst[0] == 'Tx Direction':
            lst = lst[1:]
        result = []
        for item in lst:
            if '_' in item:
                prefix, suffix = item.rsplit('_', 1)
                result.append(f"{prefix}-{suffix}")
            else:
                result.append(item)
        return '|'.join(result)

    df['Main Path'] = df['Main Path'].apply(safe_eval_list).apply(convert_list_to_path_string)
    df['Prot Path'] = df['Prot Path'].apply(safe_eval_list).apply(convert_list_to_path_string)

    # ===============================
    # Clean Alternating Separators
    # ===============================
    def clean_and_alternating_separator(path_str):
        if not isinstance(path_str, str) or path_str == "":
            return ""

        parts = re.split(r'[|#]', path_str)

        cleaned_parts = []
        for part in parts:
            part = re.sub(r'_1_', '-', part)
            part = re.sub(r'_1(\b|-)', r'-1\1', part)
            cleaned_parts.append(part)

        output = cleaned_parts[0]
        separators = ['#', '|']
        for i, segment in enumerate(cleaned_parts[1:]):
            sep = separators[i % 2]
            output += sep + segment

        return output

    df['Main Path'] = df['Main Path'].apply(clean_and_alternating_separator)
    df['Prot Path'] = df['Prot Path'].apply(clean_and_alternating_separator)

    # ===============================
    # Protection Type Mapping
    # ===============================
    df.rename(columns={'Resiliency': 'Protection Type'}, inplace=True)

    df['Protection Type'] = (
        df['Protection Type'].astype(str).str.strip().str.lower()
        .map({
            'protected': 'Protected-ASON',
            'unprotected': 'Unprotected-ASON'
        })
    )

    # Clean Ports
    df['A END port'] = df['A END port'].apply(lambda x: str(x).split(',')[0])
    df['A END port'] = df['A END port'].apply(lambda x: str(x).rsplit('_', 1)[-1])

    df['Z END port'] = df['Z END port'].apply(lambda x: str(x).split(',')[0])
    df['Z END port'] = df['Z END port'].apply(lambda x: str(x).rsplit('_', 1)[-1])

    # ===============================
    # Export
    # ===============================
    output_filename = f"{circle}_dwdm_ciena_output.xlsx" if circle else "dwdm_ciena_output.xlsx"
    output_path = os.path.join(output_filename)

    df.to_excel(output_path, index=False)

    print("DWDM Ciena File Generated")

    print("Output Rows",len(df))

    return output_path