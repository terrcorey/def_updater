# def_updater
```
For ExoMol database files. 
This script updates the labels in definition files (.def) for various datasets based on a JSON file containing updated label information.

<def_update_testing>/
├── update_def_labels.py        This script
├── states_labels.json          JSON file containing updated label information for all datasets
├── input/                      Input folder containing .def files
│   ├── <dataset1>/
│   │   └── <dataset1>.def      Definition file for dataset1
│   └── <dataset2>/
│       └── <dataset2>.def      Definition file for dataset2
├── output/                     Output folder where updated .def files will be saved
│   ├── <dataset1>/
│   │   └── <dataset1>.def      Updated definition file for dataset1
│   └── <dataset2>/
│       └── <dataset2>.def      Updated definition file for dataset2
└── log.txt                     Log file containing error messages and script execution details
```
- Each <datasetX> is a subfolder containing a .def file for that dataset.
- states_labels.json contains the label information for all datasets.
