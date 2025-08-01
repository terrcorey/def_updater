import xlwings as xw
import sys
import os
import numpy as np
import pandas as pd
from update_def_labels import read_def_file
from tqdm import tqdm
import openpyxl
from openpyxl.utils import get_column_letter

def format_excel(output_excel_path):
    try:
        # Open workbook with openpyxl for autofit
        wb = openpyxl.load_workbook(output_excel_path)
        sheet_names = wb.sheetnames
        for sheet_name in sheet_names:
            ws = wb[sheet_name]
            for col in range(2, ws.max_column + 1):  # Columns B onward
                max_length = 0
                col_letter = get_column_letter(col)
                for cell in ws[col_letter]:
                    if cell.value is not None:
                        cell_length = len(str(cell.value))
                        if cell_length > max_length:
                            max_length = cell_length
                ws.column_dimensions[col_letter].width = max_length + 2  # Add some padding
        wb.save(output_excel_path)
        wb.close()

        # Open workbook with xlwings for suppressing number-as-text warnings
        app = xw.App(visible=False)
        wb_xlw = app.books.open(output_excel_path)
        for ws in wb_xlw.sheets:
            used_range = ws.used_range
            for cell in used_range:
                if cell.api.Errors(4).Value:  # 4 is xlNumberAsText
                    cell.api.Errors(4).Ignore = True
        wb_xlw.save()
        wb_xlw.close()
        app.quit()
    except Exception as e:
        print(f"Error formatting workbook: {e}")

def create_excel_file():
    excel_file_path = os.path.join(".", "label_editor.xlsx")
    # Check if the Excel file exists
    if os.path.exists(excel_file_path):
        confirm = input(f"{excel_file_path} already exists. Are you sure you want to regenerate a new file? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Operation cancelled.")
            sys.exit(0)
        os.remove(excel_file_path)


def read_states(states_file_path):
    # Read the states file into a DataFrame
    df = pd.read_csv(states_file_path, sep=r"\s+", header=None, nrows=1)
    return df

def write_to_excel(df, mol, filename):
    excel_file_path = os.path.join(".", "label_editor.xlsx")
    sheet_name = f"{mol}__{filename}"
    sheet_name = sheet_name[:31]
    try:
        with pd.ExcelWriter(excel_file_path, mode='a' if os.path.exists(excel_file_path) else 'w', engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
            ws = writer.sheets[sheet_name]
            ws.insert_rows(1)
            ws.cell(row=1, column=1, value=f"Molecule: {mol}, File: {filename}")
    except Exception as e:
        print(f"Error saving DataFrame to Excel: {e}") 


def main():
    input_path = os.path.join(".", "input")
    mol_dirs = [d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))]
    for mol in tqdm(mol_dirs, desc = "Printing labels onto Excel..."):
        def_labels = [
            {
            "Quantum label": "StateID",
            "Format quantum label": "I12 %12d",
            "Description quantum label": "Unique integer identifier for the energy level"
            },
            {
            "Quantum label": "E",
            "Format quantum label": "F12.6 %12.6f",
            "Description quantum label": "State energy in cm-1"
            },
            {
            "Quantum label": "gtot",
            "Format quantum label": "I6 %6d",
            "Description quantum label": "Total energy level degeneracy"
            },
            {
            "Quantum label": "J",
            "Format quantum label": "",
            "Description quantum label": "Total rotational quantum number, excluding nuclear spin"
            }
        ]
        # Process each molecule directory
        mol_path = os.path.join(input_path, mol)
        for f in os.listdir(mol_path):
            def_file_path = os.path.join(mol_path, f)
            if def_file_path.endswith(".def"):
                # Read the labels from the definition file
                def_dict = read_def_file(def_file_path)
                def_labels = def_labels + def_dict['Quantum labels']

                states_file_path = os.path.join(mol_path, f.replace(".def", ".states"))
                if os.path.exists(states_file_path):
                    states_df = read_states(states_file_path)
                else:
                    print(f"States file not found for {def_file_path}. Skipping.")
                    continue
                # Stack def_labels and states_df
                labels = [l['Quantum label'] for l in def_labels]
                fmt = [l['Format quantum label'] for l in def_labels]
                description = [l['Description quantum label'] for l in def_labels]
                header_df = pd.DataFrame([labels, fmt, description])
                full_df = pd.concat([header_df, states_df], ignore_index=True)
                J_state = str(full_df.iloc[3, 3])
                try:
                    J_state = int(J_state)
                    full_df.iloc[1, 3] = f"I7 %7d"
                except ValueError:
                    full_df.iloc[1, 3] = f"F7.1 %7.1f"
                write_to_excel(full_df, mol, f)
    

if __name__ == "__main__":
    # Define the path to the Excel file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    create_excel_file()
    main()
    format_excel(os.path.join(".", "label_editor.xlsx"))
