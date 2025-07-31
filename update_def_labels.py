import numpy as np
import os
import sys
from tqdm import tqdm
import json
from datetime import datetime
import inspect
import pandas as pd
import re
import requests
import bz2
from io import BytesIO

def download_states_first_line():
    states_dir = os.path.join(".", "input")
    for mol in os.listdir(states_dir):
        if os.path.isdir(os.path.join(states_dir, mol)):
            for f in os.listdir(os.path.join(states_dir, mol)):
                if f.endswith(".def"):
                    states_file_path = os.path.join(states_dir, mol, f).replace(".def", ".states")
                    if not os.path.exists(states_file_path):
                        iso_slug, ds_name = f.split(".")[0].split("__")
                        url = f"https://exomol.com/db/{mol}/{iso_slug}/{ds_name}/{f.replace('.def', '.states.bz2')}".replace("+", "_p")
                        response = requests.get(url)
                        if response.status_code == 200:
                            # Decompress only until the first line is read
                            decompressor = bz2.BZ2Decompressor()
                            buffer = BytesIO(response.content)
                            line = b""
                            while True:
                                chunk = buffer.read(1024)
                                if not chunk:
                                    break
                                data = decompressor.decompress(chunk)
                                line += data
                                if b"\n" in line:
                                    line = line.split(b"\n", 1)[0]
                                    break
                            # Save the first line to the states file
                            with open(states_file_path, "wb") as f:
                                f.write(line + b"\n")
                        else:
                            error_log(f"Dataset: {f.replace('.def', '')} || Failed to download states file from {url}", "Error")
                            


def check_J_format(states_file_path):
    # Reads only the first row to check the J column format
    df = pd.read_csv(states_file_path, sep=r"\s+", header=None, nrows=1)
    # Assume J is in the 4th column (index 3)
    j_col = df.iloc[:, 3]
    # If any value is not integer, use float format
    if any(j_col.apply(lambda x: not float(x).is_integer())):
        return "%7.1f", "F7.1"
    return "%7d", "I7"

# json loaders
def load_correction_dict():
    path = os.path.join(os.getcwd(), "other_materials", "lib", "correction_dict.json")
    with open(path, "r") as f:
        return json.load(f)
    
def load_labels_data():
    path = os.path.join(os.getcwd(), "states_labels.json")
    with open(path, "r") as f:
        labels_data = json.load(f)
    return labels_data

def load_standard_labels():
    path = os.path.join(os.getcwd(), "other_materials", "lib", "standard_label_structure.json")
    with open(path, "r") as f:
        standard_labels = json.load(f)
    return standard_labels

def make_def_json():
    """Creates a JSON file with the standard labels structure."""
    path = os.path.join(os.getcwd(), "other_materials", "scripts", "convert_newnew.py")
    os.system(f"python3 {path}")

# Error handling and logging
def exit_script():
    log_file_path = os.path.join(os.getcwd(), "log.txt")
    with open(log_file_path, 'r') as log_file:
        log_content = log_file.readlines()
    w_counter = 0
    e_counter = 0
    c_counter = 0
    for l in log_content:
        if "[Warn]" in l:
            w_counter += 1
        elif "[Error]" in l:
            e_counter += 1
        elif "[Critical]" in l:
            c_counter += 1
    print(f"Script execution completed with {w_counter} warnings, {e_counter} errors, and {c_counter} critical errors.")
    print("Please check the log file for details.")
    print("Exiting script...")
    error_log("Script ended at " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Log")
    sys.exit(0)

def error_log(message, prefix="Error"):
    """Logs an error message to a file, including the full call stack line numbers (formatted for readability)."""
    log_file_path = os.path.join(os.getcwd(), "log.txt")
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    # Build call stack info, each frame on a new line, indented
    if prefix == "Log":
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"[{prefix}] at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {message}\n")
    else:
        stack = inspect.stack()
        stack_lines = []
        for frame in reversed(stack[1:]):
            stack_lines.append(f"    {frame.function}@{frame.lineno}")
        stack_info = "\n" + "\n".join(stack_lines)
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"[{prefix}] at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{stack_info}\n    {message}\n")

# Input reading and processing
def read_def_file(def_file_path):
    """Reads a definition file and returns a dictionary mapping descriptions to values."""
    def_dict = {}

    with open(def_file_path, 'r') as def_file:
        lines = def_file.readlines()

    for l in lines:
        if "#" not in l:
            continue
        parts = [entry.strip() for entry in l.split("#", 1)]
        if len(parts) != 2:
            continue
        value, desc = parts
        if value in correction_dict:
            value = correction_dict[value]
        if def_dict.get(desc):
            temp = def_dict[desc]
            if isinstance(temp, list):
                temp.append(value)
            else:
                def_dict[desc] = [temp, value]
        else:
            def_dict[desc] = value

    quantum_labels = []
    auxiliary_labels = []

    keys = list(def_dict.keys())
    for key in keys:
        if "Quantum label" in key:
            if key.endswith(": bound/quasi-bound"):
                key = key.replace(": bound/quasi-bound", "")
            assert key[-2:].strip().isdigit(), f"Key '{key}' does not end with a digit."
            try:
                idx = int(key[-2:].strip())
                label = {
                    "Quantum label": def_dict[key],
                    "Format quantum label": def_dict.get(f"Format quantum label {idx}", ""),
                    "Description quantum label": def_dict.get(f"Description quantum label {idx}", "")
                }
                quantum_labels.append(label)
                def_dict.pop(key)
                def_dict.pop(f"Format quantum label {idx}", None)
                def_dict.pop(f"Description quantum label {idx}", None)
            except ValueError as e:
                error_log(f"Dataset: {filename} || Error processing quantum label '{key, def_dict[key]}': {e}")
                continue
        elif "Auxiliary label" in key:
            assert key[-1].isdigit(), f"Key '{key}' does not end with a digit."
            try:
                idx = int(key[-1])
                label = {
                    "Auxiliary label": def_dict[key],
                    "Format auxiliary label": def_dict.get(f"Format auxiliary label {idx}", ""),
                    "Description auxiliary label": def_dict.get(f"Description auxiliary label {idx}", "")
                }
                auxiliary_labels.append(label)
                def_dict.pop(key)
                def_dict.pop(f"Format auxiliary label", None)
                def_dict.pop(f"Description auxiliary label", None)
            except ValueError as e:
                error_log(f"Dataset: {filename} || Error processing auxiliary label '{key, def_dict[key]}': {e}")
                continue

    idx_list = def_dict["Irreducible representation ID"] 
    irreps_list = def_dict["Irreducible representation label"]
    nuc_deg_list = def_dict["Nuclear spin degeneracy"]
    
    assert len(idx_list) == len(irreps_list) == len(nuc_deg_list), "Irreducible representation lists are not of the same length."
    irreps = []
    for i in idx_list:
        i = int(i) - 1
        dict = {
            "Irreducible representation label": irreps_list[i],
            "Nuclear spin degeneracy": nuc_deg_list[i]
        }
        irreps.append(dict)
    def_dict["irreps"] = irreps
    def_dict.pop("Irreducible representation ID", None)
    def_dict.pop("Irreducible representation label", None)
    def_dict.pop("Nuclear spin degeneracy", None)

    label_list = def_dict.get("Label for a particular broadener", None)
    fname_list = def_dict.get("Filename of particular broadener", None)
    max_J_list = def_dict.get("Maximum J for which pressure broadening parameters provided", None)
    lor_hw_list = def_dict.get('Value of Lorentzian half-width for J" > Jmax', None)
    temp_exp_list = def_dict.get('Value of temperature exponent for lines with J" > Jmax', None)
    set_no_list = def_dict.get("Number of defined quantum number sets", None)

    code_list = def_dict.get("A code that defines this set of quantum numbers", None)
    line_no_list = def_dict.get("No. of lines in the broad that contain this code", None)
    qn_no_list = def_dict.get("No. of quantum numbers defined", None)

    qns_list = def_dict.get("Defined quantum number", None)

    if label_list and fname_list and max_J_list and lor_hw_list and temp_exp_list and set_no_list:
        assert len(label_list) == len(fname_list) == len(max_J_list) == len(lor_hw_list) == len(temp_exp_list) == len(set_no_list), "Broadener lists are not of the same length."
        broadeners = []
        for i in range(len(label_list)):
            dict = {
                "Label for a particular broadener": label_list[i],
                "Filename for a particular broadener": fname_list[i],
                "Maximum J for which pressure broadening parameters provided": max_J_list[i],
                'Value of Lorentzian half-width for J" > Jmax': lor_hw_list[i],
                'Value of temperature exponent for lines with J" > Jmax': temp_exp_list[i],
                "Number of defined quantum number sets": set_no_list[i],
                "Quantum number sets": []
            }
            if code_list and line_no_list and qn_no_list:
                length = np.sum(np.array([int(x) for x in set_no_list]))
                assert len(code_list) == len(line_no_list) == len(qn_no_list) == length, "Broadener code lists are not of the same length."
                # Calculate the offset for this broadener
                set_offset = sum(int(set_no_list[m]) for m in range(i))
                for j in range(int(set_no_list[i])):
                    idx = set_offset + j
                    qn_dict = {
                        "A code that defines this set of quantum numbers": code_list[idx],
                        "No. of lines in the broad that contain this code": line_no_list[idx],
                        "No. of quantum numbers defined": qn_no_list[idx],
                        "Defined quantum number": []
                    }
                    if qns_list:
                        length = np.sum(np.array([int(x) for x in qn_no_list]))
                        assert len(qns_list) == length, "Broadener quantum number lists are not of the same length."
                        # Calculate the correct offset for qns_list
                        qn_offset = sum(int(qn_no_list[m]) for m in range(idx))
                        for k in range(int(qn_no_list[idx])):
                            qn_dict["Defined quantum number"].append(qns_list[qn_offset + k])
                    dict["Quantum number sets"].append(qn_dict)
            broadeners.append(dict)

        def_dict["Broadening parameters"] = broadeners
        def_dict.pop("Label for a particular broadener", None)
        def_dict.pop("Filename of particular broadener", None)
        def_dict.pop("Maximum J for which pressure broadening parameters provided", None)
        def_dict.pop("Value of Lorentzian half-width for J\" > Jmax", None)
        def_dict.pop("Value of temperature exponent for lines with J\" > Jmax", None)
        def_dict.pop("Number of defined quantum number sets", None)
        def_dict.pop("A code that defines this set of quantum numbers", None)
        def_dict.pop("No. of lines in the broad that contain this code", None)
        def_dict.pop("No. of quantum numbers defined", None)
        def_dict.pop("Defined quantum number", None)

    def_dict["Quantum labels"] = quantum_labels
    def_dict["Auxiliary labels"] = auxiliary_labels

    return def_dict

# Output formatting
def line_formatter(value, desc):
    """Formats a line for the new definition file."""
    value = str(value).strip()
    desc = str(desc).strip()
    l = "EXOMOL.def                                                                      # ID"
    try:
        tot_chars = int(len(l.split("#")[0]))
        num_chars = int(len(value))
        padding = tot_chars - num_chars
        assert padding >= 0, "Padding cannot be negative."
    except Exception as e:
        error_log(f"Dataset: {filename} || {e} for {desc}", "Warn")
        padding = 0
    line = value + " " * padding + "# " + desc + "\n"
    return line

def label_formatter(labels_dict):
    """Formats the quantum labels for the definition file."""
    result = ""
    try:
        for i, dict in enumerate(labels_dict):
            keys = list(dict.keys())
            for key in keys:
                result += line_formatter(dict[key], f"{key} {i+1}")
    except Exception as e:
        error_log(f"Dataset: {filename} || {e}", "Critical")
        result = ""
    return result

def broadener_formatter(broad_dict):
    """Formats the broadening parameters for the definition file."""
    result = ""
    try:
        for dict in broad_dict:
            keys = list(dict.keys())
            for key in keys:
                if isinstance(dict[key], list):
                    for value in dict[key]:
                        sub_keys = list(value.keys())
                        for sub_key in sub_keys:
                            if isinstance(value[sub_key], list):
                                for sub_value in value[sub_key]:
                                    result += line_formatter(sub_value, f"{sub_key}")
                            else:
                                result += line_formatter(value[sub_key], f"{sub_key}")
                else:
                    result += line_formatter(dict[key], f"{key}")
    except Exception as e:
        error_log(f"Dataset: {filename} || {e}", "Critical")
        result = ""
    return result

# Update def_dict according to states_labels.json
def def_dict_update(def_dict, labels_list):
    """Updates the definition dictionary with new labels."""
    if labels_list[3] == "F":
        error_log(f"Dataset: {filename} || Fourth label should be F instead of J.", "Warn")
        pass
    labels = labels_list[4:]

    keys = list(def_dict.keys())

    for key in keys:
        if "Lifetime availability (1=yes, 0=no)" in key:
            if "tau" in labels:
                labels.remove("tau")    
                def_dict[key] = 1
            else:
                def_dict[key] = 0

        elif "Lande g-factor availability (1=yes, 0=no)" in key:
            if "gfactor" in labels:
                labels.remove("gfactor")    
                def_dict[key] = 1
            else:
                def_dict[key] = 0

        elif "Uncertainty availability (1=yes, 0=no)" in key:
            if "unc" in labels:
                labels.remove("unc")    
                def_dict[key] = 1
            else:
                def_dict[key] = 0

    aux_labels = [label for label in labels if "Auxiliary" in label]
    if aux_labels:
        if len(aux_labels) != len(def_dict.get("Auxiliary labels", [])):
            def_dict["Auxiliary labels"] = []
            for aux_label in aux_labels:
                label = aux_label.split(":")[1]
                if label == "SourceType":
                    dict = {
                        "Auxiliary label": label,
                        "Format auxiliary label": "A2 %2s",
                        "Description auxiliary label": "Ma=MARVEL,Ca=Calculated,EH=Effective Hamiltonian,IE=Isotopologue extrapolation"
                    }
                    def_dict["Auxiliary labels"].append(dict)
                elif label == "Ecal":
                    dict = {
                        "Auxiliary label": label,
                        "Format auxiliary label": "F12.6 %12.6f",
                        "Description auxiliary label": "Calculated energy in cm-1"
                    }
                    def_dict["Auxiliary labels"].append(dict)
    new_labels = []
    prefixes = []
    for label in labels:
        if label not in aux_labels and ":" in label:
            prefixes.append(label.split(":")[0])
            new_labels.append(label.split(":")[1])
        elif label not in aux_labels:
            new_labels.append(label)

    quanta_cases = np.unique(np.array(prefixes))

    if int(def_dict["No. of quanta cases"]) != len(quanta_cases):
        error_log(f"Dataset: {filename} || Number of quanta cases ({len(quanta_cases)}) does not match number of entries in def file ({def_dict['No. of quanta cases']}).", "Warn")

    old_labels = def_dict.get("Quantum labels", None)
    if not old_labels:
        error_log(f"Dataset: {filename} || No quantum labels found in definition file.", "Critical")
        exit_script()
    for i, label in enumerate(new_labels):
        found = False
        if label == "J":
            mol = re.sub(r'\((\d+)([A-Za-z]*)\)', r'(\2)', def_dict["IsoFormula"])
            mol = mol.replace("(", "").replace(")", "")
            fname = def_dict["Iso-slug"] + "__" + def_dict["Isotopologue dataset name"] + ".states"

            states_file_path = os.path.join(".", "input", mol, fname)
            J_cfmt, J_ffmt = check_J_format(states_file_path)
            fmt = " ".join([J_ffmt, J_cfmt])
            new_labels[i] = {
                "Quantum label": label,
                "Format quantum label": fmt,
                "Description quantum label": "Total rotational quantum number"
            }
            found = True
            break
        for dict in old_labels:
            if dict["Quantum label"] == label:
                new_labels[i] = dict
                found = True
                break
        if not found:
            for dict in standard_labels:
                if dict["Quantum label"] == label:
                    new_labels[i] = dict
                    found = True
                    break
            if not found:
                error_log(f"Dataset: {filename} || Quantum label '{label}' not found in old labels. Please input them manually.")

def update_def(def_file_path, def_dict):
    """Creates a new definition file with updated labels."""
    os.makedirs(os.path.join(os.getcwd(), "output"), exist_ok=True)
    mol = os.path.basename(os.path.dirname(def_file_path))
    output_dir = os.path.join(os.getcwd(), "output", mol)
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, os.path.basename(def_file_path))
    
    keys = list(def_dict.keys())

    with open(def_file_path, 'r') as def_file:
        with open(output_file_path, 'w') as output_file:
            lines = def_file.readlines()
            for l in lines:
                if "#" not in l:
                    continue
                parts = [entry.strip() for entry in l.split("#", 1)]
                if len(parts) != 2:
                    continue
                value, desc = parts
                if desc in keys and not isinstance(def_dict.get(desc), list):
                    value = def_dict.get(desc)
                    output_file.write(line_formatter(value, desc))
                    def_dict.pop(desc, None)
                elif desc in keys and isinstance(def_dict.get(desc), list):
                    for value in def_dict[desc]:
                        output_file.write(line_formatter(value, desc))
                    def_dict.pop(desc, None)         

                if desc == "No. of quanta defined":
                    output_file.write(label_formatter(def_dict.get("Quantum labels", [])))
                    output_file.write(label_formatter(def_dict.get("Auxiliary labels", [])))
                    def_dict.pop("Quantum labels", None)
                    def_dict.pop("Auxiliary labels", None)
                elif desc == "Number of irreducible representations":
                    output_file.write(label_formatter(def_dict.get("irreps", [])))
                    def_dict.pop("irreps", None)
                elif desc == "Default value of temperature exponent for all lines":
                    output_file.write(broadener_formatter(def_dict.get("Broadening parameters", [])))
                    def_dict.pop("Broadening parameters", None)

            # Write any remaining entries in def_dict to the output file
            for desc, value in def_dict.items():
                if isinstance(value, list):
                    for v in value:
                        output_file.write(line_formatter(v, desc))
                else:
                    output_file.write(line_formatter(value, desc))

def main():
    global date
    date = datetime.now().strftime("%Y%m%dT%H%M%S")
    error_log("Script started.", "Log")
    def_folder_path = os.path.join(os.getcwd(), "input")
    if not os.path.exists(def_folder_path):
        os.makedirs(def_folder_path)
        print("Input folder has been created. Please place your .def files in the 'input' folder.")
        exit_script()
    def_paths = []
    labels_data = load_labels_data()
    for dir in os.listdir(def_folder_path):
        dir_path = os.path.join(def_folder_path, dir)
        if os.path.isdir(dir_path):
            for file in os.listdir(dir_path):
                if file.endswith(".def"):
                    def_paths.append(os.path.abspath(os.path.join(dir_path, file)))

    if not labels_data:
        print(f"No updated labels file found in the folder. Please ensure 'states_labels.json' is correctly named and present.")
        exit_script()

    if not def_paths:
        print(f"No definition files found in the folder: {def_folder_path}")
        exit_script()
    
    skipped_files = []
    for def_file_path in tqdm(def_paths, desc="Processing definition files"):
        global filename
        filename = os.path.basename(def_file_path)
        filename = filename.replace(".def", "")
        if labels_data is not None:
            match = next((item for item in labels_data if item.get('ds_name') == filename), None)
        else:
            match = None
        if match and None not in match['labels']:
            def_dict = read_def_file(def_file_path)
            labels_list = match['labels']
            def_dict_update(def_dict, labels_list)
            update_def(def_file_path, def_dict)
        else:
            skipped_files.append(filename)
            continue


    if skipped_files:
        print(f"Skipped {len(skipped_files)} files due to missing or incomplete labels:")
        for filename in skipped_files:
            print(f" - {filename}")

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    correction_dict = load_correction_dict()
    standard_labels = load_standard_labels()
    log_file_path = os.path.join(os.path.dirname(__file__), "log.txt")
    if os.path.exists(log_file_path):
        os.remove(log_file_path)
    download_states_first_line()
    main()
    make_def_json()
    exit_script()
    