import requests
import os
import csv
import glob
import seaborn as sb
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.style as style


# File that contains the filter list URLs.
LIST_INPUT_FILE = "list_links.txt"

# Folder to store the filter justdomain_lists.
LIST_FOLDER = os.path.join(os.getcwd(), "..", "Filterlist", "selected_lists")

# Folder for diagrams
DIAGRAM_OUTPUT_FOLDER = os.path.join(os.getcwd(),"plots")


def read_filter_lists():
    """
    Read the filter justdomain_lists from disc
    """
    all_lists = dict()
    all_rules = set()

    search = "*.txt"
    files = glob.glob(os.path.join(LIST_FOLDER, search))


    for file in files:
        filter_list = set()

        # Read each filter list
        with open(file, 'rb') as f:
            # Read each filter rule
            for line in f.readlines():
                line = str(line)
                # Skip comments
                if line.startswith('!'):
                    continue
                filter_list.add(line.rstrip())
                all_rules.add(line.rstrip())

        # Save list and start with the next list file
        all_lists[os.path.basename(file).replace('.txt', '')] = filter_list

    print("Read", len(all_rules), "distinct rules.")



    return all_lists

def remove_basic_easylist(input_list):
    """
    :param input_list: filter justdomain_lists as dict
    """
    basic_easylist = input_list['USA']
    for flist, rules in input_list.items():
        specific_rules = rules.difference(basic_easylist)
        #print("For country", flist, "distinct rules:", len(rules))
        #print("For country", flist, "rules besides standard easylist", len(specific_rules))
        input_list[flist] = specific_rules

    #input_list['USA'] = basic_easylist # reset basic list for usa

    # group france
    #france_fl = set()
    #for flist, rules in input_list.items():
        # filter france
    #    if "France" in flist:
     #       france_fl.update(rules)


    #input_list['France'] = france_fl
    #input_list.pop('France1')
    #input_list.pop('France2')
    #input_list.pop('France3')
    input_list.pop('USA')

    return input_list


def plot_list_sizes(input_lists):
    """
    Plots the number of rules for each identified filter list.
    """
    # Preprocess the data so that it has the correct format to be plotted
    list_sizes = []
    for country, rules in input_lists.items():
        countrydata = []
        countrydata.append(country.strip())
        countrydata.append(len(rules))
        list_sizes.append(countrydata)

    # Make teh plot pretty
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    style.use('tableau-colorblind10')
    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams.update({
       "text.usetex": True,
        "font.family": "sans-serif"
    })

    # Plot and safe the data
    df = pd.DataFrame(list_sizes, columns=['Country', 'Number of rules'])
    ax = sb.barplot(data=df, y="Country", x="Number of rules", color="black")
    ax.set(ylabel=r"\textbf{Country}", xlabel=r"\textbf{Number of distinct rules}")
    plt.tight_layout()
    plt.show()
    # plt.savefig(os.path.join(os.getcwd(), 'plots', 'p1_rules_per_list.pdf'), dpi=600, transparent=False,
    #             bbox_inches='tight', format="pdf")


def occurrence_barplot():

    all_lists = read_filter_lists()
    sortedOcc = determine_rule_occurrence(all_lists)

    occinNumbers = {}
    for occ in sortedOcc:
        if occ in occinNumbers:
            occinNumbers[occ] += 1
        else:
            occinNumbers[occ] = 1
    outputList = []
    counter = 1
    for occ, number in occinNumbers.items():

        occurence = []
        occurence.append(occ)
        occurence.append(number)
        occurence.append("Group " + str(counter))
        outputList.append(occurence)
        counter = counter + 1
        # print(outputList)

    sb.set_theme()

    df = pd.DataFrame(outputList, columns=['Occurrences in justdomain_lists', 'Number of rules', 'Groups'])
    bp = sb.barplot(data=df, x='Occurrences in justdomain_lists', y='Number of rules', orient="v"  )  # , hue="Anzahl der Regeln")
    bp.set(title="Occurrences in justdomain_lists\n(logarithmic)")
    plt.yscale('log')

    # plt.show()
    plt.savefig(os.path.join(DIAGRAM_OUTPUT_FOLDER,"Occurrences_in_lists.png"))
    plt.clf()


def occurrence_histplot():
    all_lists = read_filter_lists()
    sortedOcc = determine_rule_occurrence(all_lists)

    sb.set_theme()
    sb.set_style('whitegrid')

    df = pd.DataFrame(sortedOcc, columns=['Occurrences in justdomain_lists'])
    # kde => smooth line - does not work well with logarithmic scaling
    # see also:
    # kind="hist"
    # kind="kde"
    # kind="ecdf"
    diagram_type = ["ecdf", "empirical complementary CDF"]
    bp = sb.displot(data=df, x='Occurrences in justdomain_lists', kind=diagram_type[0], stat="count", complementary=True)
    bp.set(ylabel="Number (total number = " + str(len(sortedOcc)) + ")", title="Distribution of rules in justdomain_lists (" + diagram_type[1] + ")")
    # plt.yscale('log')

    # plt.show()
    plt.savefig(os.path.join(DIAGRAM_OUTPUT_FOLDER,"occurences_in_lists_hist.png"))
    plt.clf()

def determine_rule_occurrence(initital_dict):
    # first we build our dictionary
    occurrences = {}


    for list2 in initital_dict.values():
        for rule in list2:
            if rule in occurrences:
                occurrences[rule] += 1
            else:
                occurrences[rule] = 1

    # check if aggregation works
    # print("Number of rules in total", len(occurrences))
    # first_key = next(iter(occurrences))
    first_key = list(occurrences.keys())[0]
    # print("First rule", first_key, "occurrs in", occurrences[first_key], "country justdomain_lists.")

    # secondly we convert the dictionary to a list
    # warning: this removes all of the list keys
    sortedOcc = sorted(occurrences.values(), reverse=True)
    # print(sortedOcc)

    # thirdly we try to create and render our diagramm
    return sortedOcc


if __name__ == '__main__':

    all_lists = read_filter_lists()

    r = dict()
    for list, rules in all_lists.items():
        r[list] = [len(rules)]

    # all_lists = remove_basic_easylist(all_lists)
    plot_list_sizes(all_lists)

    for list, rules in all_lists.items():
        r[list].append(len(rules))

    for k,v in r.items():
        print(k,v)