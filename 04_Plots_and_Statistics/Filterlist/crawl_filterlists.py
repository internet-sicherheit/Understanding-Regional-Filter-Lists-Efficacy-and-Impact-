import requests
import os
import csv
import urllib3
import glob
import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup


# File that contains the filter list URLs.
LIST_INPUT_FILE = "list_links.txt"
# Folder to store the filter justdomain_lists.
LIST_FOLDER = "selected_lists"

def write_list_to_disk(list_content, filename):
    """
    Writes the filter list to disk
    :param list_content:  The list (string)
    :param filename: Name of the filter list
    """
    filename = filename.replace(" ", "_").replace("'", "").replace("/", "")
    full_filename = os.path.join(os.getcwd(), LIST_FOLDER, str(filename + ".txt"))
    open(full_filename, 'w', encoding="utf-8").write(list_content)

def getUrls():
    df = pd.read_csv(LIST_INPUT_FILE, sep=';')

    for index, row in df.iterrows():
        country = row['country']
        url = row['link']

        list_content = requests.get(url, allow_redirects=True).content.decode('utf-8')
        write_list_to_disk(list_content, country)


if __name__ == '__main__':
    getUrls()