import matplotlib.style as style
import seaborn as sb
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib
import os
from cluster import build_distance_matrix

LIST_FOLDER = "filter_lists"


def read_filter_lists():
    """
    Read the filter justdomain_lists from disc.
    """
    all_lists = dict()
    all_rules = set()

    # Define the input dir
    input_dir = os.path.join(os.getcwd(), LIST_FOLDER)

    # Iterate over all files (filter justdomain_lists) in the input directory
    for filename in os.listdir(input_dir):
        filter_list = set()

        # Read each filter list
        with open(os.path.join(input_dir, filename), 'rb') as file:
            # Read each filter rule
            for line in file.readlines():
                line = str(line)
                # Skip comments
                if line.startswith('!'):
                    continue
                filter_list.add(line.rstrip())
                all_rules.add(line.rstrip())

        # Save list and start with the next list file
        all_lists[filename.replace('.txt', '')] = filter_list

    return all_lists, all_rules


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

    # Make the plot pretty...
    sb.set(rc={'figure.figsize': (13, 5.2)})
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    matplotlib.rcParams['font.family'] = 'sans-serif'
    plt.tight_layout()

    # Plot and safe the data
    df = pd.DataFrame(list_sizes, columns=['Country', 'Number of rules'])
    print(df.describe())
    ax = sb.barplot(data=df, y="Country", x="Number of rules", color="black")
    ax.set(ylabel="Country", xlabel="Number of rules")
    plt.tight_layout()

    plt.show()
    # plt.savefig(os.path.join(os.getcwd(), 'plots', 'p1_rules_per_list.pdf'), dpi=600, transparent=False,
    #             bbox_inches='tight', format="pdf")
    # plt.savefig(os.path.join(os.getcwd(), 'plots', 'p1_rules_per_list.png'), dpi=600, transparent=False,
    #             bbox_inches='tight', format="png")


def similarity_heatmap(input_lists):
    matrix = build_distance_matrix(input_lists)
    sb.heatmap(matrix)

    # Make the plot pretty...
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    matplotlib.rcParams['font.family'] = 'sans-serif'
    plt.tight_layout()

    # Safe the plot
    # plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'plots', "p3_similarity_of_lists.png"), dpi=600,
                transparent=False, bbox_inches='tight', format="png")
    plt.clf()


def occurrence_barplot(input_lists):
    # Get sorted occurrences of each rule.
    sorted_occurrences = determine_rule_occurrence(input_lists)

    # Get count of rule sin each 'occurrence group.'
    outputList = count_rule_occurrence_groups(sorted_occurrences)

    # Make the plot pretty...
    sb.set(rc={'figure.figsize': (13, 5.2)})
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    matplotlib.rcParams['font.family'] = 'sans-serif'
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


def rule_distribution(imput_lists):
    #TODO: Data preparation

    # Make the plot pretty...
    sb.set(rc={'figure.figsize': (13, 5.2)})
    sb.set_theme(style="whitegrid")
    sb.set_context("paper", font_scale=1.5)
    sb.set_style("ticks")
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    matplotlib.rcParams['font.family'] = 'sans-serif'
    plt.tight_layout()


    # Safe the plot
    plt.show()
    # plt.savefig(os.path.join(os.getcwd(), 'plots', "p2_number_of_rules.pdf"), dpi=600,
    #             transparent=False, bbox_inches='tight', format="pdf")
    plt.clf()


if __name__ == '__main__':
    all_lists, all_rules = read_filter_lists()
    print("Read %d justdomain_lists containing %d rules." % (len(all_lists), len(all_rules)))

    # rule_distribution(all_lists)
    # similarity_heatmap(all_lists)
    plot_list_sizes(all_lists)
    # occurrence_barplot(all_lists)
