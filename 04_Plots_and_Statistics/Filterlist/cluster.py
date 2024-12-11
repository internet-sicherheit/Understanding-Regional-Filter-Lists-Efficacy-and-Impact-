import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.cluster import DBSCAN
from sklearn.cluster import OPTICS
import os

INPUT_PATH = os.path.join(os. getcwd(), "sanitized_lists")
INPUT_FILES = os.listdir(INPUT_PATH)
ALL_FILTER_RULES = dict()
FILTER_RULE_THRESHOLD = 15


def read_data(files, path):
    """
    Read data into a dict with key = filename and value = list of filter rules
    :return: none
    """
    data = dict()
    for file in files:
        filename = file.strip().replace(" ", "").replace(".txt", "")
        filter_rules = list()
        with open(os.path.join(path, file), "rb") as f:
            for line in f:
                line = line.decode("utf-8").strip()
                if not line.startswith("!"):
                    filter_rules.append(line)
                    if line not in ALL_FILTER_RULES:
                        ALL_FILTER_RULES[line] = 1
                    else:
                        ALL_FILTER_RULES[line] += 1
        data[filename] = set(filter_rules)

    return data


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

    print("mean:", np.mean(rule_stats), "SD:", np.std(rule_stats), "min:", np.min(rule_stats), "max:", np.max(rule_stats))


def compute_jaccard_distance(set1, set2):
    """
    Computes the Jaccard distance between both sets.
    """
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return 1 - (len(intersection) / len(union))


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

    print("##### DBSCAN #####")
    clustering_algo = DBSCAN(metric="precomputed")
    dbscan_fit = clustering_algo.fit(distance_matrix)
    labels_dbscan = dbscan_fit.labels_
    cluster_info(labels_dbscan)

    print("##### HDBSCAN #####")
    hdb = HDBSCAN(metric="precomputed")
    hdbscan_fit = hdb.fit(distance_matrix)
    hdbscan_labels = hdbscan_fit.labels_
    cluster_info(hdbscan_labels)

    print("##### OPTICS #####")
    optics = OPTICS(metric="precomputed", min_samples=2)
    optics_fit = optics.fit(distance_matrix)
    optics_labels = optics_fit.labels_
    cluster_info(optics_labels)


if __name__ == '__main__':
    """
    Main method. Implement overall use case logic here. 
    """
    # Read all filter justdomain_lists
    print("Reading filter justdomain_lists...")
    input_filter_rules = read_data(INPUT_FILES, INPUT_PATH)
    print("Read %d justdomain_lists" % len(input_filter_rules))
    # Sanitize filter rules
    # print("Sanitizing filter justdomain_lists...")
    # sanitize_filter_rules(input_filter_rules)
    # Perform the clustering
    print("Clustering filter justdomain_lists...")
    cluster(input_filter_rules)
