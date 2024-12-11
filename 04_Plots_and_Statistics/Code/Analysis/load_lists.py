import os
import glob
from adblockparser import AdblockRules

# Path to the list folder
LIST_FOLDER = os.path.join(os.getcwd(),"..", "Filterlist", "selected_lists")

def get_rules_by_country(country):
    filename = country + ".txt"
    with open(os.path.join(LIST_FOLDER, filename), encoding='utf-8') as f:
        raw_rules = f.read().splitlines()
    return AdblockRules(raw_rules)


if __name__ == '__main__':
    folder = glob.glob(os.path.join(LIST_FOLDER, "*.txt"))
    print(folder)