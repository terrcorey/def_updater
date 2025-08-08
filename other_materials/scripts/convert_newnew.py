import json
import os
import itertools
import traceback
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from update_def_labels import error_log, check_J_format
from tqdm import tqdm

def find_doi(ds_name):
    path = os.path.join(".", "states_labels.json")
    with open(path, "r") as f:
        states_labels = json.load(f)
        for entry in states_labels:
            if entry["ds_name"] == ds_name:
                return entry.get("doi", "")


def def_to_json(def_file_path, json_file_path):
    fname = os.path.basename(def_file_path).replace(".def", ".states")
    mol = os.path.basename(os.path.dirname(def_file_path))
    states_file_path = os.path.join(states_directory, mol, fname)
    J_cfmt, J_ffmt = check_J_format(states_file_path)
    json_dict = {
        "isotopologue": {},
        "atoms":{},
        "irreducible_representations": {},
        "dataset": {
            "states": {
                "states_file_fields": [
                {
                    "name": "ID",
                    "desc": "Unique integer identifier for the energy level",
                    "ffmt": "I12",
                    "cfmt": "%12d"
                },
                {
                    "name": "E",
                    "desc": "State energy in cm-1",
                    "ffmt": "F12.6",
                    "cfmt": "%12.6f"
                },
                {
                    "name": "gtot",
                    "desc": "Total energy level degeneracy",
                    "ffmt": "I6",
                    "cfmt": "%6d"
                },
                {
                    "name": "J",
                    "desc": "Total rotational quantum number, excluding nuclear spin",
                    "ffmt": J_ffmt,
                    "cfmt": J_cfmt
                },
            ]
            },
            "transitions": {}
        },
        "partition_function": {},
        "broad": {
            "default_Lorentzian_half-width": None,
            "default_temperature_exponent": None,
        }
    }

    # Open a CSV file for writing
    with open(def_file_path, 'r') as def_file:
        lines = def_file.readlines()
        line_iter = iter(lines)
        quantum_case_label = None
        broadener_label = None
        for line in line_iter:
            if '# IsoFormula' in line:
                json_dict["isotopologue"]["iso_formula"] = line.split('#')[0].strip()
            elif '# Iso-slug' in line:
                json_dict["isotopologue"]["iso_slug"] = line.split('#')[0].strip()
            elif '# Inchi key of molecule' in line:
                json_dict["isotopologue"]["inchikey"] = line.split('#')[0].strip()
            elif '# Number of atoms' in line:
                json_dict["atoms"]["number_of_atoms"] = int(line.split('#')[0].strip())
            elif '# Isotope number' in line:
                isotope_number = int(line.split('#')[0].strip())
                element_symbol = next(line_iter, None)
                if element_symbol is not None:
                    element_symbol = element_symbol.split('#')[0].strip()
                if "element" not in json_dict["atoms"]:
                    json_dict["atoms"]["element"] = {}
                json_dict["atoms"]["element"][element_symbol] = isotope_number
            elif '# Isotopologue mass (Da) and (kg)' in line:
                mass_values = line.split('#')[0].strip().split()
                json_dict["isotopologue"]["mass_in_Da"] = float(mass_values[0])
            elif '# Symmetry group' in line:
                json_dict["isotopologue"]["point_group"] = line.split('#')[0].strip()
            elif '# Isotopologue dataset name' in line:
                dataset_name = line.split('#')[0].strip()
                json_dict["dataset"]["name"] = dataset_name

                # Get the dataset name from the file name, without the extension
                filename = os.path.basename(def_file_path).replace(".def", "")
                # print(filename.replace("-", ""))
                doi = find_doi(filename)
                # print(f"Filename: {filename}")
                # print(f"DOI: {doi}")
                json_dict["dataset"]["doi"] = doi
            elif '# Version number with format YYYYMMDD' in line:
                json_dict["dataset"]["version"] = int(line.split('#')[0].strip())
            elif '# Maximum temperature of linelist' in line:
                json_dict["dataset"]["max_temperature"] = float(line.split('#')[0].strip())
            elif '# No. of states in .states file' in line:
                value = line.split('#')[0].strip()
                if value in ['NaN', '']:
                    json_dict["dataset"]["states"]["number_of_states"] = None
                else:
                    json_dict["dataset"]["states"]["number_of_states"] = int(float(value))
            elif '# Total number of transitions' in line:
                value = line.split('#')[0].strip()
                if value in ['NaN', '']:
                    json_dict["dataset"]["transitions"]["number_of_transitions"] = None
                else:
                    json_dict["dataset"]["transitions"]["number_of_transitions"] = int(value)
            elif '# No. of transition files' in line:
                json_dict["dataset"]["transitions"]["number_of_transition_files"] = int(line.split('#')[0].strip())
            elif '# Maximum wavenumber (in cm-1)' in line:
                json_dict["dataset"]["transitions"]["max_wavenumber"] = float(line.split('#')[0].strip())
            elif '# Maximum temperature of partition function' in line:
                mpft_temp = line.split('#')[0].strip()
                if mpft_temp in ['NaN', '']:
                    json_dict["partition_function"]["max_partition_function_temperature"] = None
                else:
                    json_dict["partition_function"]["max_partition_function_temperature"] = float(mpft_temp)
            elif '# Step size of temperature' in line:
                ssot_temp = line.split('#')[0].strip()
                if ssot_temp in ['NaN', '']:
                    json_dict["partition_function"]["partition_function_step_size"] = None
                else:
                    json_dict["partition_function"]["partition_function_step_size"] = float(ssot_temp)
            elif '# Default value of temperature exponent for all lines' in line:
                dvotefal_temp = line.split('#')[0].strip()
                if dvotefal_temp in ['NaN', '']:
                    json_dict["dataset"]["n_L_default"] = None
                else:
                    json_dict["dataset"]["n_L_default"] = float(dvotefal_temp)
            elif '# Uncertainty availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["states"]["uncertainties_available"] = bool(int(line.split('#')[0].strip()))
            elif '# No. of pressure broadeners available' in line:
                json_dict["dataset"]["num_pressure_broadeners"] = int(line.split('#')[0].strip())
            elif '# Dipole availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["dipole_available"] = bool(int(line.split('#')[0].strip()))
            elif '# No. of cross section files available' in line:
                json_dict["dataset"]["nxsec_files"] = int(line.split('#')[0].strip())
            elif '# No. of k-coefficient files available' in line:
                json_dict["dataset"]["nkcoeff_files"] = int(line.split('#')[0].strip())
            elif '# Lifetime availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["states"]["lifetime_available"] = bool(int(line.split('#')[0].strip()))
            elif '# Lande g-factor availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["states"]["lande_g_available"] = bool(int(line.split('#')[0].strip()))
            elif '# Hyperfine resolved dataset (1=yes, 0=no)' in line:
                if int(line.split('#')[0].strip()) == 1:
                    json_dict["dataset"]["states"]["hyperfine_resolved_dataset"] = True
                    json_dict["dataset"]["states"]["states_file_fields"][3]['name'] = "F"
                else:
                    json_dict["dataset"]["states"]["hyperfine_resolved_dataset"] = False
            elif '# Cooling function availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["cooling_function_available"] = bool(int(line.split('#')[0].strip()))
            elif '# Specific heat availability (1=yes, 0=no)' in line:
                json_dict["dataset"]["specific_heat_available"] = bool(int(line.split('#')[0].strip()))
            elif '# Continuum (1=yes, 0=no)' in line:
                json_dict["dataset"]["continuum"] = bool(int(line.split('#')[0].strip()))
            elif '# Predis (1=yes, 0=no)' in line:
                json_dict["dataset"]["predis"] = bool(int(line.split('#')[0].strip()))
            elif '# Number of irreducible representations' in line:
                num_irreps = int(line.split('#')[0].strip())
                for _ in range(num_irreps): 
                    irrep_label = next(line_iter).split('#')[0].strip()
                    gnuc = int(next(line_iter).split('#')[0].strip())
                    json_dict["irreducible_representations"][irrep_label] = gnuc

            # elif '# Irreducible representations and their degeneracies' in line:
            #     for _ in range(4):
            #         irrep_line = next(line_iter, None)
            #         if irrep_line is not None:
            #             irrep_label, irrep_gnuc = irrep_line.split('#')[0].strip().split()
            #             json_dict["irreps"][irrep_label] = int(irrep_gnuc)

            # elif '# Quantum numbers' in line:
            #     while True:
            #         qn_line = next(line_iter, None)
            #         if qn_line is None or qn_line.strip() == '':
            #             break
            #         qn_name, qn_ffmt, qn_cfmt, qn_desc = qn_line.split('#')[0].strip().split(maxsplit=3)
            #         json_dict["dataset"]["states"]["states_file_fields"].append({
            #             "name": qn_name,
            #             "ffmt": qn_ffmt,
            #             "cfmt": qn_cfmt,
            #             "desc": qn_desc
            #         })
            if '# Quantum case label' in line:
                quantum_case_label = line.split('#')[0].strip()
                # print("Quantum case label:", quantum_case_label)
            elif '# Quantum label' in line:
                quantum_label = line.split('#')[0].strip()
                # print(quantum_label)
                format_quantum_label = next(line_iter, None)
                if format_quantum_label is not None:
                    if '# Format quantum' in format_quantum_label:
                        format_quantum_label = format_quantum_label.split('#')[0].strip()
                        format_quantum_parts = format_quantum_label.split(' ')
                        if len(format_quantum_parts) == 1:
                            ffmt = format_quantum_parts[0]
                            cfmt = format_quantum_parts[0]
                        else:
                            ffmt, cfmt = format_quantum_parts[0], ' '.join(format_quantum_parts[1:])
                    else:
                        ffmt = ''
                        cfmt = ''
                        # If the line does not contain "# Format quantum", it's a description line,
                        # so we need to go back one line
                        line_iter = itertools.chain([format_quantum_label], line_iter)
                else:
                    ffmt = ''
                    cfmt = ''

                description_quantum_label = next(line_iter, None)
                if description_quantum_label is not None:
                    description_quantum_label = description_quantum_label.split('#')[0].strip()
                # Safely concatenate quantum_case_label and quantum_label
                name_field = (
                    (quantum_case_label if quantum_case_label is not None else "") +
                    (":" if quantum_case_label is not None and quantum_label is not None else "") +
                    (quantum_label if quantum_label is not None else "")
                )
                json_dict["dataset"]["states"]["states_file_fields"].append({
                    "name": name_field,
                    "ffmt": ffmt,
                    "cfmt": cfmt,
                    "desc": description_quantum_label
                })
                # print("Appending to states dictionary:", quantum_case_label + ":" + quantum_label)
            elif '# Auxiliary title' in line:
                aux_title = line.split('#')[0].strip()
                fmt = next(line_iter, None)
                cfmt = None
                ffmt = None
                if fmt != None:
                    ffmt, cfmt = fmt.split('#')[0].strip().split(" ")
                desc = next(line_iter, None)
                if desc != None:
                    desc = desc.split('#')[0].strip()
                json_dict["dataset"]["states"]["states_file_fields"].append({
                    "name": "Auxiliary:" + aux_title,
                    "ffmt": ffmt,
                    "cfmt": cfmt,
                    "desc": desc
                })
            elif '# Default value of Lorentzian half-width for all lines (in cm-1/bar)' in line:
                json_dict["broad"]["default_Lorentzian_half-width"] = float(line.split('#')[0].strip())
            elif '# Default value of temperature exponent for all lines' in line:
                json_dict["broad"]["default_temperature_exponent"] = float(line.split('#')[0].strip())
            elif '# Label for a particular broadener' in line:
                broadener_label = line.split('#')[0].strip()
                json_dict["broad"][broadener_label] = {}
            elif '# Higher energy with complete set of transitions (in cm-1)' in line:
                max_energy = line.split('#')[0].strip()
                if max_energy == 'NA' or max_energy == '' or max_energy == 'NaN':
                    json_dict["dataset"]["states"]["max_energy"] = None
                else:
                    json_dict["dataset"]["states"]["max_energy"] = float(line.split('#')[0].strip())

            elif '# Description' in line:
                json_dict["dataset"]["states"]["uncertainty_description"] = line.split('#')[0].strip()

            elif '# No. of quanta defined' in line:
                json_dict["dataset"]["states"]["num_quanta"] = int(line.split('#')[0].strip())
            elif '# Filename of particular broadener' in line:
                if broadener_label is not None:
                    json_dict["broad"][broadener_label]["filename"] = line.split('#')[0].strip()
            elif '# Maximum J for which pressure broadening parameters provided' in line:
                if broadener_label is not None:
                    json_dict["broad"][broadener_label]["max_J"] = int(line.split('#')[0].strip())

            elif '# Value of Lorentzian half-width for J" > Jmax' in line:
                if broadener_label is not None:
                    json_dict["broad"][broadener_label]["Lorentzian_half_width"] = float(line.split('#')[0].strip())

            elif '# Value of temperature exponent for lines with J" > Jmax' in line:
                if broadener_label is not None:
                    json_dict["broad"][broadener_label]["temperature_exponent"] = float(line.split('#')[0].strip())

            elif '# Number of defined quantum number sets' in line:
                if broadener_label is not None:
                    json_dict["broad"][broadener_label]["num_quantum_number_sets"] = int(line.split('#')[0].strip())
                    json_dict["broad"][broadener_label]["quantum_number_sets"] = []

                    for _ in range(json_dict["broad"][broadener_label]["num_quantum_number_sets"]):
                        quantum_number_set = {}
                        code_line = next(line_iter, None)
                        if code_line is not None:
                            quantum_number_set["code"] = code_line.split('#')[0].strip()
                        else:
                            quantum_number_set["code"] = None

                        num_lines_line = next(line_iter, None)
                        if num_lines_line is not None:
                            quantum_number_set["num_lines"] = int(num_lines_line.split('#')[0].strip())
                        else:
                            quantum_number_set["num_lines"] = None

                        num_quantum_numbers_line = next(line_iter, None)
                        if num_quantum_numbers_line is not None:
                            quantum_number_set["num_quantum_numbers"] = int(num_quantum_numbers_line.split('#')[0].strip())
                        else:
                            quantum_number_set["num_quantum_numbers"] = 0

                        quantum_numbers = []
                        for _ in range(quantum_number_set["num_quantum_numbers"]):
                            qn_line = next(line_iter, None)
                            if qn_line is not None:
                                quantum_numbers.append(qn_line.split('#')[0].strip())
                            else:
                                quantum_numbers.append(None)
                        quantum_number_set["quantum_numbers"] = quantum_numbers

                        json_dict["broad"][broadener_label]["quantum_number_sets"].append(quantum_number_set)

        # # Add the InChI retrieval code here
        # inchikey = json_dict["isotopologue"].get("inchikey")
        # if inchikey:
        #     url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/{inchikey}/property/InChI/TXT"
        #     response = requests.get(url)
        #     if response.status_code == 200:
        #         inchi = response.text.strip().replace("InChI=", "")
        #         json_dict["isotopologue"]["inchi"] = inchi

        # Ensure the order of keys in "states" as specified
        states_keys_order = [
            "number_of_states",
            "max_energy",
            "hyperfine_resolved_dataset",
            "uncertainty_description",
            "uncertainties_available",
            "lifetime_available",
            "lande_g_available",
            "num_quanta",
            "states_file_fields"
        ]
        json_dict["dataset"]["states"] = {k: json_dict["dataset"]["states"].get(k) for k in states_keys_order}

        dataset_keys_order = [
            "name",
            "version",
            "doi",
            "max_temperature",
            "n_L_default",
            "num_pressure_broadeners",
            "nxsec_files",
            "nkcoeff_files",
            "dipole_available",
            "cooling_function_available",
            "specific_heat_available",
            "continuum",
            "predis",
            "states",
            "transitions"
        ]

        json_dict["dataset"] = {k: json_dict["dataset"].get(k) for k in dataset_keys_order}
        # print(json_dict)

        insert_index = 4
        if json_dict["dataset"]["states"].get("uncertainties_available", False):
            json_dict["dataset"]["states"]["states_file_fields"].insert(insert_index, {
                "name": "unc",
                "desc": "Energy uncertainty in cm-1",
                "ffmt": "F12.6",
                "cfmt": "%12.6f"
            })
            insert_index += 1

        if json_dict["dataset"]["states"].get("lifetime_available", False):
            json_dict["dataset"]["states"]["states_file_fields"].insert(insert_index, {
                "name": "tau",
                "desc": "Lifetime in s",
                "ffmt": "ES12.4",
                "cfmt": "%12.4e"
            })
            insert_index += 1

        if json_dict["dataset"]["states"].get("lande_g_available", False):
            json_dict["dataset"]["states"]["states_file_fields"].insert(insert_index, {
                "name": "gfactor",
                "desc": "Lande g-factor",
                "ffmt": "F10.6",
                "cfmt": "%10.6f"
            })
            insert_index += 1
            

        with open(json_file_path, 'w') as json_file:
            json.dump(json_dict, json_file, indent=4, ensure_ascii=False)


# Specify the directory where the downloaded files are located
directory = os.path.join(".", "output")
states_directory = os.path.join(".", "input")
# Specify the directory where the converted JSON files will be saved
output_directory = os.path.join(".", "output")

# Create the output directory if it doesn't exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

if __name__ == "__main__":
    successful_conversion = 0
    os.chdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
    
    # def_to_json(os.path.join(directory, "CS", "12C-32S__JnK.def"), os.path.join(output_directory, "CS", "12C-32S__JnK.def.json"))

    # Gather all .def files to process
    def_files = []
    for dir in os.listdir(directory):
        dir_path = os.path.join(directory, dir)
        if os.path.isdir(dir_path):
            for file_name in os.listdir(dir_path):
                if file_name.endswith('.def'):
                    def_files.append((dir, file_name))

    # Process files with tqdm progress bar
    for dir, file_name in tqdm(def_files, desc="Converting .def files"):
        def_file_path = os.path.join(directory, dir, file_name)
        json_file_name = file_name.replace('.def', '.def.json')
        json_file_path = os.path.join(output_directory, dir, json_file_name)

        try:
            def_to_json(def_file_path, json_file_path)
            successful_conversion += 1
        except Exception as e:
            error_log(f"Error converting file {file_name}: {str(e)}\n{traceback.format_exc()}")

    total_files = len(def_files)
    print("Successfully converted", successful_conversion, "files out of", total_files, ".")
