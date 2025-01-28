import csv
import os
import requests
import glob
import numpy as np
import seaborn as sb
import pandas as pd
import matplotlib.colors as mcolors
import statistics
import hdbscan

#from sklearn.cluster import HDBSCAN
from sklearn.cluster import DBSCAN
from sklearn.cluster import OPTICS

import matplotlib.pyplot as plt
import matplotlib.colors as LogNorm

INPUT_DIR = os.path.join(os.getcwd(), '..', '..', 'Filterlist', 'sanitized_lists')
PATH_CSV = os.path.join(os.getcwd(), '..','..', 'Filterlist', 'all_easylist.txt')
OUTPUT_FOLDER = os.path.join(os.getcwd(), 'easylists')
ALL_FILTER_RULES = dict()
FILTER_RULE_THRESHOLD = 20
ALL_RULES = list()


def read_csv(csv_path):
    data = []
    with open(csv_path, 'r', newline='', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter=';')
        for row in csv_reader:
            data.append(row)

    return data


def download_and_save_txt(country,url, save_path=OUTPUT_FOLDER):
    try:
        # Get file
        response = requests.get(url)
        response.raise_for_status()  # Check for errors

        filename = country + "_filterlist.txt"

        # Store the file as txt
        with open(os.path.join(save_path, filename), 'wb') as file:
            file.write(response.content)

        print(f"Successfully downloaded and stored under {os.path.join(save_path, filename)}.")

    except requests.exceptions.RequestException as e:
        print(f"Error while downloading file: {e}")

def get_data():
    csv_path = PATH_CSV
    csv_data = read_csv(csv_path)

    data = dict()
    for row in csv_data:
        country = row['country']
        link = row['link']

        download_and_save_txt(country, link)


def remove_general_easylist_rules(input_list):
    file = glob.glob(os.path.join(OUTPUT_FOLDER, 'USA_filterlist.txt'))
    filter_rules = set()
    with open(file[0], 'rb') as f:
        # Read each filter rule
        for line in f.readlines():
            line = str(line)
            # Skip comments
            if line.startswith('!'):
                continue
            filter_rules.add(line.rstrip())

    return input_list.difference(filter_rules)



def read_filter_lists():
    """
    Read the filter justdomain_lists from disc
    """
    all_lists = dict()
    all_rules = set()

    search = "*.txt"
    files = glob.glob(os.path.join(INPUT_DIR, search))

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
                if line not in ALL_FILTER_RULES:
                    ALL_FILTER_RULES[line] = 1
                else:
                    ALL_FILTER_RULES[line] += 1
                filter_list.add(line.rstrip())
                all_rules.add(line.rstrip())
                ALL_RULES.append(line.rstrip())

        # Save list and start with the next list file
        all_lists[os.path.basename(file).replace('.txt', '').split('_')[0]] = filter_list
        # if os.path.basename(file) == "USA_filterlist.txt":
        #     all_lists[os.path.basename(file).replace('.txt', '').split('_')[0]] = filter_list
        # else:
        #     all_lists[os.path.basename(file).replace('.txt', '').split('_')[0]] = remove_general_easylist_rules(filter_list)

    # print(f"[flov.1.0] Total filter justdomain_lists from EasyLists: {len(list(all_lists.keys()))}")
    total_rules = sum(list(ALL_FILTER_RULES.values()))
    print("[flov.1.1] Read", len(all_rules), "(", len(all_rules) / total_rules *100 ,"%) distinct rules.")

    total_rules_per_list = [len(rule_set) for rule_set in list(all_lists.values())]
    print(f"[flov.1.1.1] Rules per list - Mean: {np.mean(total_rules_per_list)} , min: {np.min(total_rules_per_list)}, max: {np.max(total_rules_per_list)}, sd: {np.std(total_rules_per_list)}")

    return all_lists, all_rules


def compute_jaccard_distance(set1, set2):
    """
    Computes the Jaccard distance between both sets.
    """
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return 1 - (len(intersection) / len(union))


def compare_rules_to_basic_easylist(data):
    basic_easylist = data['USA']

    total_easylist = 0

    for list, rules in data.items():
        #print(f"Compare {list} with basic easylist...")
        specific_rules = rules.difference(basic_easylist)
        #print(f"Before: {len(rules)}, after: {len(specific_rules)}")

        if specific_rules == 0 and list != "USA":
            total_easylist += 1

    print(f"[flov.1.0.1] Number of justdomain_lists with only basic easy list elements: {total_easylist}")

def count_rule_occurrence_groups(occurrences):
    occinNumbers = {}
    for occ in occurrences:
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

    return outputList


def determine_rule_occurrence(rule_dict):
    occurrences = dict()

    # Count the occurrence of each rule
    for rules_in_list in rule_dict.values():
        for rule in rules_in_list:
            if rule in occurrences:
                occurrences[rule] += 1
            else:
                occurrences[rule] = 1

    # Sort the occurrences
    sorted_occurrences = sorted(occurrences.values(), reverse=True)

    return sorted_occurrences

def occurrence_barplot(data):
    # Get sorted occurrences of each rule.
    sorted_occurrences = determine_rule_occurrence(data)

    # Get count of rule sin each 'occurrence group.'
    outputList = count_rule_occurrence_groups(sorted_occurrences)

    # Make the plot pretty...
    sb.set(rc={'figure.figsize': (13, 5.2)})
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['text.usetex'] = False
    plt.rcParams['axes.labelweight'] = 'bold'
    plt.rcParams['font.family'] = 'sans-serif'
    plt.tight_layout()

    # Create the plot
    df = pd.DataFrame(outputList, columns=['Occurrences in justdomain_lists', 'Number of rules', 'Groups'])
    bp = sb.barplot(data=df, x='Occurrences in justdomain_lists', y='Number of rules', orient="v", color="black")
    plt.yscale('log')
    bp.bar_label(bp.containers[0], fmt='{:,.0f}', fontsize=11)

    # Safe the plot
    # plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'plots', "p2_number_of_rules.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")
    plt.clf()


def comparing_two_lists(data):

    d = dict()

    for i in range(len(data)):
        fl1 = list(data.keys())[i]
        r1 = data[fl1]
        for j in range(len(data)):
            fl2 = list(data.keys())[j]
            r2 = data[fl2]

            if fl1 == fl2:
                continue
            else:
                int = r1.intersection(r2)
                d[(fl1,fl2)] = int
                total_rules = len(r1) + len(r2)
                #print(fl1, fl2, total_rules, len(int))
                #print(f"Compare list {fl1} with {fl2} - intersection of {len(int)} -> {(len(int)/total_rules)*100}%")

    v = [len(e) for e in list(d.values())]
    print(f"[flov.1.2] Two list comparison - mean: {np.mean(v)}, min: {np.min(v)}, max: {np.max(v)}, sd: {np.std(v)}, median: {statistics.median(v)}")
    most_sim_lists = ""
    for lists, rules in d.items():
        if len(rules) == np.max(v):
            print(lists, len(rules))


def sanitize_filter_rules(input_rules):
    """
    Removes rules from the provided dictionary of filter list rules that are share by multiple justdomain_lists. The function
    manipulates the provided dictionary of filter list rules.
    """
    rule_stats = []
    for country, rules in input_rules.items():
        rules_copy = rules.copy()
        list_length = len(rules_copy)
        rules_removed = 0
        for rule in rules_copy:
            if rule in ALL_FILTER_RULES and ALL_FILTER_RULES[rule] > FILTER_RULE_THRESHOLD:
                rules.remove(rule)
                rules_removed += 1
        rule_stats.append(rules_removed / list_length * 100)

    print("[flov.1.3] mean:", np.mean(rule_stats), "SD:", np.std(rule_stats), "min:", np.min(rule_stats), "max:", np.max(rule_stats))


def compare_rules(data):

    for i in range(len(data)):
        set1 = set(data[i])
        for j in range(len(data)):
            set2 = set(data[j])

            compute_jaccard_distance(set1, set2)


def build_distance_matrix(data):
    """
    Computes the distance matrix for the given sets of filter justdomain_lists.
    """
    distance_matrix = []
    keys = list(data.keys())

    # Nested iteration over all sets to compute the pairwise distances
    for i in range(len(keys)):
        distances = []
        for j in range(len(keys)):
            set_a = data[keys[i]]
            set_b = data[keys[j]]
            distances.append(compute_jaccard_distance(set_a, set_b))

        distance_matrix.append(distances)

    return distance_matrix


def plot_distance_matrix(data):
    # Define a custom discrete colormap
    min_val, max_val, step = 0.0, 1.0, 0.05
    n_colors = int((max_val - min_val) / step) + 1
    colors = plt.cm.viridis(np.linspace(0, 1, n_colors))  # Choose any colormap that fits your needs
    cmap = mcolors.LinearSegmentedColormap.from_list("custom_cmap", colors, N=n_colors)
    norm = mcolors.BoundaryNorm(boundaries=np.arange(min_val - step / 2, max_val + step, step), ncolors=n_colors)


    sb.heatmap(data, annot=True, vmin=0.6, vmax=1)

    # Make the plot pretty...
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams['text.usetex'] = False
    plt.rcParams['axes.labelweight'] = 'bold'
    plt.rcParams['font.family'] = 'sans-serif'
    plt.tight_layout()

    # Safe the plot
    # plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'plots', "p3_similarity_of_lists.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")
    plt.clf()


def cluster_info(labels):
    # Number of clusters in labels, ignoring noise if present.
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise_ = list(labels).count(-1)

    print("Estimated number of clusters: %d" % n_clusters_)
    print("Estimated number of noise points: %d" % n_noise_)
    print("Cluster labels:",  labels)


def cluster(data):
    """
    Perform the clustering of the filter rules.
    """
    distance_matrix = build_distance_matrix(data)
    print("##### Computed distance matrix #####")
    print(np.matrix(distance_matrix))

    print("##### Plot distance matrix #####")
    plot_distance_matrix(distance_matrix)

    print("##### [flov.1.4] DBSCAN #####")
    clustering_algo = DBSCAN(metric="precomputed")
    dbscan_fit = clustering_algo.fit(distance_matrix)
    labels_dbscan = dbscan_fit.labels_
    cluster_info(labels_dbscan)

    print("##### HDBSCAN #####")
    hdb = hdbscan.HDBSCAN(metric="precomputed")
    hdbscan_fit = hdb.fit(distance_matrix)
    hdbscan_labels = hdbscan_fit.labels_
    cluster_info(hdbscan_labels)

    print("##### OPTICS #####")
    optics = OPTICS(metric="precomputed", min_samples=2)
    optics_fit = optics.fit(distance_matrix)
    optics_labels = optics_fit.labels_
    cluster_info(optics_labels)


def plot_occurence_distribution(all_lists):
    tmp_rule_distribution = dict()
    rule_distribution = dict()
    for name, list_rules in all_lists.items():
        for rule in list_rules:
            if rule not in tmp_rule_distribution:
                tmp_rule_distribution[rule] = 1
            else:
                tmp_rule_distribution[rule] += 1

    for rule, occ in  tmp_rule_distribution.items():
        if occ not in rule_distribution:
            rule_distribution[occ] = 1
        else:
            rule_distribution[occ] +=1

    print(rule_distribution)


if __name__ == "__main__":
    print("Reading filter justdomain_lists...")
    data, rules = read_filter_lists()

    print("Occurency from filter rules...")
    occurrence_barplot(data)

    print("Compare two justdomain_lists...")
    comparing_two_lists(data)

    print("Compare to basic easy list...")
    compare_rules_to_basic_easylist(data)

    # print("Sanitizing filter justdomain_lists... ")
    # sanitize_filter_rules(data)

    print("Clustering filter justdomain_lists...")
    cluster(data)

    print("new")
    plot_occurence_distribution(rules)



