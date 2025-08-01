import xlwings
import os
import numpy as np
import pandas as pd
from update_def_labels import read_def_file


def create_excel_file():
    excel_file_path = os.path.join(".", "label_editor.xlsx")

    # Check if the Excel file exists
    if os.path.exists(excel_file_path):
        os.remove(excel_file_path)
    # Create a new Excel workbook
    wb = xlwings.Book()
    wb.save(excel_file_path)
    wb.close()

def read_states(states_file_path):
    # Read the states file into a DataFrame
    df = pd.read_csv(states_file_path, sep="\t", header=None, names=["State", "Label"])
    return df

def write_to_excel(df, mol, filename):
    excel_file_path = os.path.join(".", "label_editor.xlsx")
    wb = xlwings.Book(excel_file_path)
    sheet_name = f"{mol}__{filename}"
    # Remove sheet if it already exists to avoid duplication
    if sheet_name in [s.name for s in wb.sheets]:
        wb.sheets[sheet_name].delete()
    sheet = wb.sheets.add(name=sheet_name)

    # Write the molecule name and definition file name
    sheet.range("A1").value = f"Molecule: {mol}"
    sheet.range("A2").value = f"Definition File: {filename}"

    # Write the DataFrame to the Excel sheet starting from row 4
    sheet.range("A4").options(index=False, header=True).value = df

    # Save and close the workbook
    wb.save()
    wb.close()

def main():
    input_path = os.path.join(".", "input")
    mol_dirs = [d for d in os.listdir(input_path) if os.path.isdir(os.path.join(input_path, d))]
    for mol in mol_dirs:
        # Process each molecule directory
        mol_path = os.path.join(input_path, mol)
        for f in os.listdir(mol_path):
            def_file_path = os.path.join(mol_path, f)
            if def_file_path.endswith(".def"):
                # Read the labels from the definition file
                def_dict = read_def_file(def_file_path)
                def_labels = def_dict['dataset']['states']['states_file_fields']
                states_file_path = os.path.join(mol_path, f.replace(".def", ".states"))
                if os.path.exists(states_file_path):
                    states_df = read_states(states_file_path)
                    print(states_df)
                else:
                    print(f"States file not found for {def_file_path}. Skipping.")
                    continue
                # Stack def_labels and states_df
                labels_df = pd.DataFrame(def_labels, columns=["Label"])
                stacked_df = pd.concat([labels_df, states_df], axis=1)
                write_to_excel(stacked_df, mol, f)

if __name__ == "__main__":
    # Define the path to the Excel file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    create_excel_file()
    main()

