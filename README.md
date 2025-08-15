# def_updater
For ExoMol database files. 
This script updates the labels in definition files (.def) for various datasets based on a JSON file containing updated label information.

```
<def_updater>/
├── input/                        # Input folder containing .def files
│   ├── <dataset1>/               # Folder for dataset1
│   │   └── <dataset1>.def        # Definition file for dataset1
│   └── <dataset2>/               # Folder for dataset2
│       └── <dataset2>.def        # Definition file for dataset2
├── states_labels.json            # JSON file with updated label information for all datasets
├── update_def_labels.py          # Script for generating the output .def and .def.json files
├── label_editor_generator.py     # Script for generating label editors
├── output/                       # Output folder for updated files
│   ├── <dataset1>/               # Folder for updated dataset1 files
│   │   ├── <dataset1>.def        # Updated definition file for dataset1
│   │   └── <dataset1>.def.json   # JSON version of the updated definition file for dataset1
│   └── <dataset2>/               # Folder for updated dataset2 files
│       ├── <dataset2>.def        # Updated definition file for dataset2
│       └── <dataset2>.def.json   # JSON version of the updated definition file for dataset2
└── log.txt                       # Log file with error messages and script execution details
```
- Each <datasetX> is a subfolder containing a .def file for that dataset.
- states_labels.json contains the label information for all datasets.
