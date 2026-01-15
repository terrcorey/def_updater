import os
import pandas as pd
import json

def get_all_paths():
    result = []
    for r, d, f in os.walk("."):
        for file in f:
            if file.endswith(".json"):
                json_path = os.path.join(r, file)
                result.append(json_path)
    return result

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data

def extract_keys(data_dict, keys):
    return {key: data_dict.get(key, None) for key in keys}

def extract_wanted_info(data):
    iso_info = data.get("isotopologue", {})
    keys_to_extract = [
        "iso_formula",
        "iso_slug",
        "inchi",
        "inchikey",
        "mass_in_Da"
    ]
    iso_dict = extract_keys(iso_info, keys_to_extract)

    dataset_info = data.get("dataset", {})
    keys_to_extract = [
        "name",
        "version",
        "doi",
        "max_temperature",
        "num_pressure_broadeners",
        "nxsec_files",
        "nkcoeff_files",
        "dipole_available",
        "cooling_function_available",
        "specific_heat_available",
        "continuum"
    ]
    dataset_dict = extract_keys(dataset_info, keys_to_extract)

    hyperfine_info = {
        "hyperfine_resolved_dataset":dataset_info.get("states", {}).get("hyperfine_resolved_dataset", {})
        }
    
    combined_dict = {**iso_dict, **dataset_dict, **hyperfine_info}
    return combined_dict 

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    os.chdir("../../output/")
    all_paths = get_all_paths()
    lines = []
    for path in all_paths:
        data = read_json(path)
        dict = extract_wanted_info(data)
        lines.append(dict)

    df = pd.DataFrame(lines)
    df.to_csv("../other_materials/lib/def_summary.csv", index=False)