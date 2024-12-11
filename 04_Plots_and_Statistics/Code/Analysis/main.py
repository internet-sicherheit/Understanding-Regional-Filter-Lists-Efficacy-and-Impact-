import os
import glob
import pandas as pd
from tqdm import tqdm
from google.cloud import bigquery
from adblockparser import AdblockRules

# Path to the list folder
LIST_FOLDER = os.path.join(os.getcwd(),"..", "Filterlist", "selected_lists")
ALL_LIST = dict()
BASIC_FILTERLIST = list()

def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "..", "resources", "google_gcp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df

def remove_basic_filterlist():
    with open(os.path.join(LIST_FOLDER, 'USA.txt'), encoding='utf-8') as f:
        for line in f.readlines():
            line = str(line)

            if line.startswith('!'):
                continue

            BASIC_FILTERLIST.append(line.rstrip())

def get_rules_per_list():
    search = '*.txt'
    files = glob.glob(os.path.join(LIST_FOLDER, search))

    for file in files:
        raw_rules = list()
        with open(file, encoding='utf-8') as f:
            for line in f.readlines():
                line = str(line)

                if line.startswith('!'):
                    continue

                raw_rules.append(line.rstrip())

        cleaned_rules = raw_rules
        if os.path.basename(file).replace('.txt', '') != 'USA':
            cleaned_rules = set(raw_rules).difference(set(BASIC_FILTERLIST))

        ALL_LIST[os.path.basename(file).replace('.txt', '')] = list(cleaned_rules)

def init_adblocker(ruleset):
    return AdblockRules(ruleset)

def is_known_blocker(url, rules):
    """
    Tests if the given URL is present on the initialized block list.

    :param url: The URL to test
    :param rules: The parser object
    :return: TRUE if present on the list
    """
    return rules.should_block(url)

if __name__ == '__main__':
    # Init basic filterlist
    remove_basic_filterlist()
    get_rules_per_list()

    for k, v in ALL_LIST.items():
        print(k, len(v))

    #adblock_rules = get_rules_by_country('USA')

    #is_known_blocker("", adblock_rules)