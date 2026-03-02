import os
from tqdm import tqdm
import numpy as np

def main():
    case_labels = []
    for r, d, f in os.walk("."):
        for file in f:
            if file.endswith(".def"):
                filepath = os.path.join(r, file)
                with open(filepath, "r") as infile:
                    lines = infile.readlines()

                for line in lines:
                    if "Quantum case label" in line:
                        case_labels.append(line.split("#")[0].strip())
    
    return np.unique(case_labels)
            

if __name__ == "__main__":
    os.chdir(os.path.dirname(__file__))
    os.chdir("../../output/")
    labels = main()

    for label in labels:
        print(label)