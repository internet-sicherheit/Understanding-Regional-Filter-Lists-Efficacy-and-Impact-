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
# Website that holds the filter justdomain_lists
LIST_INPUT_URL = "https://tw3.gitlab.io/tw3filterlists/"
# Threshold of rules in a list
RULE_THRESHOLD = 1000

TOO_SHORT_LIST = 0
TOO_OLD_LIST = 0
NO_ADBLOCK_FORMAT = 0
CLOUD_NOT_LOAD = 0
NO_AGE_INFORMATION = 0
INTERESTING_LIST = 0


def get_list_urls():
    """
    Fetches a list of URLs to blocking justdomain_lists.
    :return: a list of URLS that might hold a block list
    """
    list_urls = []
    print("Reading URLS...")
    filter_list_html = requests.get(LIST_INPUT_URL, allow_redirects=True).content
    soup = BeautifulSoup(filter_list_html, "html.parser")

    filter_list_rows = soup.find("table", {"id": "table-filters"}).find("tbody").find_all("tr")

    for row in filter_list_rows:
        name_tag = row.find_all("td")[0].find("a")
        if name_tag is None:
            continue

        list_name = name_tag.encode_contents().decode('utf-8')
        for form in row.find_all("button", {"class": "btn btn-action s-rounded bg-dark tooltip tooltip-bottom"}):
            list_url = form.get('data-tooltip')
            if list_url is not None and list_url.startswith('http'): # and list_url.endswith('txt'):
                list_urls.append((list_name, list_url))

    n_filter_list = len(list_urls)
    print("Found %d justdomain_lists" % n_filter_list)
    return list_urls, n_filter_list


def is_list_of_interest(the_list, list_name=None):
    """
    Tests if the filter justdomain_lists meets the requirements to be included in our analysis.
    :param the_list: Teh filter list (string).
    :return: True if the given list meets all requirements; False otherwise.
    """
    global TOO_SHORT_LIST
    global TOO_OLD_LIST
    global NO_ADBLOCK_FORMAT
    global CLOUD_NOT_LOAD
    global NO_AGE_INFORMATION

    is_interesting_list = True

    if the_list.startswith("[Adblock Plus"):
        if the_list.count('\n') < RULE_THRESHOLD:
            TOO_SHORT_LIST += 1
            is_interesting_list = False

    # if the_list.startswith("[Adblock Plus"):
        for row in the_list.split('\n'):
            if row.startswith("!"):
                if row.startswith("! Last modified:") or row.startswith("! Version:") \
                        or row.startswith("! Last updated:") \
                        or row.startswith("! Last update") or row.startswith("! Last change") \
                        or row.startswith("! Updated") or row.startswith("! Last Modified:"):

                    if "2023" not in row:
                        TOO_OLD_LIST += 1
                        is_interesting_list = False
                else:
                    # No information on age
                    NO_AGE_INFORMATION += 1
                    # is_interesting_list &= True

            elif row.startswith("[Adblock Plus"):
                # Skip the first row
                continue
            else:
                # Stop if the starting comment block, which contains the metadata, ends.
                # print(list_name, "\n", the_list[0:125], "\n------------------------")
                break
                # is_interesting_list &= False
    else:
        if the_list.count('\n') > 5:
            NO_ADBLOCK_FORMAT += 1
        else:
            CLOUD_NOT_LOAD += 1
        is_interesting_list = False

    return is_interesting_list


def validate_lists(list_urls):
    global CLOUD_NOT_LOAD
    global INTERESTING_LIST

    for list_name, list_url in list_urls:
        try:
            list_content = requests.get(list_url, allow_redirects=True).content.decode('utf-8')
            if is_list_of_interest(list_content, list_name):
                INTERESTING_LIST += 1
                write_list_to_disk(list_content, list_name)
            # else:
            #     print("not a block list", list_url)
        except requests.exceptions.SSLError:
            CLOUD_NOT_LOAD += 1
        except requests.exceptions.ConnectionError:
            CLOUD_NOT_LOAD += 1
        except urllib3.exceptions.MaxRetryError:
            CLOUD_NOT_LOAD += 1
        except UnicodeDecodeError as dec_err:
            CLOUD_NOT_LOAD += 1


def write_list_to_disk(list_content, filename):
    """
    Writes the filter list to disk
    :param list_content:  The list (string)
    :param filename: Name of the filter list
    """
    filename = filename.replace(" ", "_").replace("'", "").replace("/", "")
    full_filename = os.path.join(os.getcwd(), LIST_FOLDER, str(filename + ".txt"))
    open(full_filename, 'w', encoding="utf-8").write(list_content)


def download_lists():
    """
    Download the newest version of the identified filter justdomain_lists
    """
    # Read CSV file that contains the links to the filter justdomain_lists

    with open(os.path.join(os.getcwd(), LIST_INPUT_FILE), newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=';')

        # This skips the first row (header) of the CSV file.
        next(reader)

        # Download each list
        for row in reader:
            print(row)
            filename = os.path.join(os.getcwd(), LIST_FOLDER, str(row[1]+".txt"))
            r = requests.get(row[2], allow_redirects=True)
            open(filename, 'wb').write(r.content)

def loadlist(path):
    """
    :param path:
    :return: list with blocking rules
    """
    with open(path, 'r', encoding="utf-8") as file:
        return {line.strip() for line in file if not line.startswith('!') and line.strip()}

def cmp(list1, list2, easylist='USA.txt'):
    """
    :param list1: easylist from the given country
    :param list2: standard easylist
    :return: unique rules in list1, difference from standard easylist
    """
    basic_easylist = loadlist(os.path.join(os.getcwd(), LIST_FOLDER, easylist))
    # remove basic easylist
    list1_specific = list1.difference(basic_easylist)
    list2_specific = list2.difference(basic_easylist)

    return list1_specific.difference(list2_specific)

def compare_lists(easylist='USA.txt'):
    """
    :param easylist: standard easylist
    """
    basic_easylist = loadlist(os.path.join(os.getcwd(), LIST_FOLDER, easylist))

    # Search term for txt files, exlude USA file as standard easylist
    search = "*.txt"
    files = glob.glob(os.path.join(os.getcwd(), LIST_FOLDER, search))


    countries = list()#['Country']


    # Compare each rule against standard filter list
    for file in files:
        l = loadlist(file)
      #  r = cmp(l,basic_easylist)
        country = os.path.basename(file).split('.')[0]
        countries.append(country)
        #print("%s unique rules %d" % (os.path.basename(file), len(r)))

    #df = pd.DataFrame(index=countries, columns=countries)
    inp = list()

    # Compare each filter list (rules) with other filter justdomain_lists (rules)
    for i in range(len(files)):
        filename1 = os.path.basename(files[i]).split('.')[0]
        row = list()
        for j in range(len(files)):
            filename2 = os.path.basename(files[j]).split('.')[0]
            l1 = loadlist(files[i])
            l2 = loadlist(files[j])
            r = cmp(l1, l2)
            #df.iloc[i, j] = len(r)

            #print(len(l2), len(r))

            row.append(len(r))
            #print("Compared %s with %s with diff from %d" % (filename1, filename2, len(r)))
        inp.append(row)


    df1 = pd.DataFrame(inp, index=countries, columns=countries)

    #print(df)

    sb.heatmap(df1, xticklabels=True, yticklabels=True, annot=True)
    plt.title("Difference of filter rules throught other countries (removed standard EasyList)")
    plt.show()

if __name__ == '__main__':
    # Get the URLs of potential blocking justdomain_lists
    #block_list_urls, n_list = get_list_urls()

    # Download justdomain_lists
    #download_lists()

    # Process list comparing with the standard easylist
    compare_lists()

    # Validate that the URLs hold blocking justdomain_lists
    # block_list_urls = [('EasyList Germany', 'https://easylist-downloads.adblockplus.org/easylistgermany.txt')]
    # block_list_urls = [('Polsak', 'https://raw.githubusercontent.com/adblockpolska/Adblock_PL_List/master/adblock_polska.txt')]
    # n_list = 1
    #validate_lists(block_list_urls)
    #print("TOO_SHORT_LIST", TOO_SHORT_LIST, TOO_SHORT_LIST / n_list * 100)
    #print("TOO_OLD_LIST", TOO_OLD_LIST, TOO_OLD_LIST / n_list * 100)
    #print("NO_ADBLOCK_FORMAT", NO_ADBLOCK_FORMAT, NO_ADBLOCK_FORMAT / n_list * 100)
    #print("CLOUD_NOT_LOAD", CLOUD_NOT_LOAD, CLOUD_NOT_LOAD / n_list * 100)
