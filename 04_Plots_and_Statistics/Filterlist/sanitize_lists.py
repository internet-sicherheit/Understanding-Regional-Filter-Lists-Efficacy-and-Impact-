import requests
import os
import pandas as pd
import sys

# File that contains the filter list URLs.
LIST_INPUT_FILE = "list_links.txt"
# Folder to store the filter justdomain_lists.
LIST_FOLDER = "new_selected_lists"
# Folder to store the filter justdomain_lists.
LIST_SANITIZED_FOLDER = "sanitized_lists"
# Name of the baseline list (e.e., standard EasyList)
BASELINE_LIST_NAME = "USA"


def write_list_to_disk(list_content, filename, folder):
    """
    Writes the filter list to disk
    :param list_content:  The list (string)
    :param filename: Name of the filter list
    :param folder: The output folder
    """
    filename = filename.replace(" ", "_").replace("'", "").replace("/", "")
    full_filename = os.path.join(os.getcwd(), folder, str(filename + ".txt"))
    open(full_filename, 'w', encoding="utf-8").write(list_content)


def download_urls():
    df = pd.read_csv(LIST_INPUT_FILE, sep=';')

    for index, row in df.iterrows():
        country = row['country']
        url = row['link']

        list_content = requests.get(url, allow_redirects=True).content.decode('utf-8')
        write_list_to_disk(list_content, country, LIST_FOLDER)


def read_filter_lists():
    """
    Read the filter justdomain_lists from disc.
    """
    all_lists = dict()
    baseline_list = set()
    # all_rules = set()

    # Define the input dir
    input_dir = os.path.join(os.getcwd(), LIST_FOLDER)

    # Iterate over all files (filter justdomain_lists) in the input directory
    for filename in os.listdir(input_dir):
        filter_list = set()
        # Read each filter list
        with open(os.path.join(input_dir, filename), 'rb') as file:
            # Read each filter rule
            for line in file.readlines():
                line = line.decode("UTF-8")
                # Skip comments
                if line.startswith('!'):
                    continue
                filter_list.add(line.rstrip())

        # Find the baseline file
        if filename.replace('.txt', '') == BASELINE_LIST_NAME:
            baseline_list = filter_list
        else:
            all_lists[filename.replace('.txt', '')] = filter_list

    return all_lists, baseline_list


def sanitize_lists(other_lists, baseline_list):
    all_sanitized_lists = dict()
    # Iterate over all justdomain_lists abd compute the difference of the local list and the baseline list
    for name, the_list in other_lists.items():
        # Compute the difference and print the new and old size of the set
        sanitized_list = the_list.difference(baseline_list)
        all_sanitized_lists[name] = sanitized_list
        print(name, "\tlen original list:", len(the_list), "\tlen sanitized list:", len(sanitized_list))

    return all_sanitized_lists


def write_sanitized_list_to_disk(sanitized_lists):
    for name, a_list in sanitized_lists.items():
        # convert set to string
        rule_list = ""
        for rule in a_list:
            rule_list = rule_list + rule + "\n"

        # Write list to disk
        write_list_to_disk(rule_list, name + "_sanitized", LIST_SANITIZED_FOLDER)


if __name__ == '__main__':
    # download_urls()
    the_other_lists, the_baseline_list = read_filter_lists()
    print("Number of rules in the baseline list:", len(the_baseline_list))
    the_sanitized_lists = sanitize_lists(the_other_lists, the_baseline_list)
    write_sanitized_list_to_disk(the_sanitized_lists)
    write_sanitized_list_to_disk({"USA": the_baseline_list})
