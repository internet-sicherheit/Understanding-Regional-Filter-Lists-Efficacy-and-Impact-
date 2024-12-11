import os
import sys
from google.cloud import bigquery
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd
import numpy as np


def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "google.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def general_numbers():
    result_df = exec_select_query("""SELECT COUNT(*) AS number, (COUNT(*) / (SELECT COUNT(*) FROM `measurement.filterlist_rules`) * 100) AS share 
                                        FROM `measurement.filterlist_rules` 
                                        WHERE identified_tr = 0
                                            AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%');""")

    print("[RU 1.1] Number of rules that were never used", result_df['number'].iloc[0], "share", result_df['share'].iloc[0])

    result_df = exec_select_query("""SELECT rule, filterlist,  identified_tr, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0
                                        ORDER BY identified_tr DESC
                                        LIMIT 10;""")

    print("[RU 1.2] Top 10 rules", result_df)

    result_df = exec_select_query("""SELECT sum(identified_tr_share)
                                        FROM (
                                        SELECT rule, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')
                                        ORDER BY identified_tr DESC
                                        LIMIT 100);""")

    print("[RU 1.3.1] Coverage of justdomain_lists containing the top 100 rules: ", result_df)


    result_df = exec_select_query("""SELECT sum(identified_tr_share)
                                        FROM (
                                        SELECT rule, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')
                                        ORDER BY identified_tr DESC
                                        LIMIT 500);""")

    print("[RU 1.3.1] Coverage of justdomain_lists containing the top 500 rules: ", result_df)


    result_df = exec_select_query("""SELECT sum(identified_tr_share)
                                        FROM (
                                        SELECT rule, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')
                                        ORDER BY identified_tr DESC
                                        LIMIT 1000);""")

    print("[RU 1.3.1] Coverage of justdomain_lists containing the top 1000 rules: ", result_df)

    result_df = exec_select_query("""SELECT sum(identified_tr_share)
                                        FROM (
                                        SELECT rule, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0 AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')
                                        ORDER BY identified_tr DESC
                                        LIMIT 1500);""")

    print("[RU 1.3.1] Coverage of justdomain_lists containing the top 1500 rules: ", result_df)



def plotting():
    result_df = exec_select_query("""SELECT filterlist AS list, identified_tr
                                        FROM `measurement.filterlist_rules`
                                        WHERE (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%');""")
    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={"font.size": 18, "axes.titlesize": 14, "axes.labelsize": 14,
               "legend.fontsize": 14, "xtick.labelsize": 12, "ytick.labelsize": 12}, style="white")

    g = sb.FacetGrid(result_df, col="list", col_wrap=3, sharex=False)
    g.map(sb.histplot, "identified_tr", bins=20, color="black").set(yscale='log') #bins=50
    g.set_titles("{col_name}", weight='bold', fontsize=18)
    g.set_axis_labels(x_var='Blocked requests', y_var="Number of rules")
    # plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'plots', "p8_rule_usage_by_list.pdf"), dpi=600,
                 transparent=False, bbox_inches='tight', format="pdf")





    #TODO make me pretty...
    result_df = exec_select_query("""SELECT rule, filterlist,  identified_tr, (identified_tr / (SELECT SUM(identified_tr) FROM `measurement.filterlist_rules` WHERE identified_tr > 0) *100) AS identified_tr_share
                                        FROM `measurement.filterlist_rules`
                                        WHERE identified_tr > 0
                                            AND (rule NOT LIKE '##%' OR rule NOT LIKE '%#@#%')
                                        ORDER BY identified_tr DESC;""")

    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={"font.size": 18, "axes.titlesize": 14, "axes.labelsize": 14,
               "legend.fontsize": 14, "xtick.labelsize": 12, "ytick.labelsize": 12}, style="white")

    fig, ax = plt.subplots()
    g = sb.ecdfplot(data=result_df, x="identified_tr_share", ax=ax)
    g.set_xlim([0.0, 0.8])
    g.set_ylim([0.9, 1.0])

    ax2 = plt.axes([0.5, 0.3, .25, .25])
    sb.ecdfplot(data=result_df, x="identified_tr_share", ax=ax2)
    ax2.set_title('Full ECDF Plot')
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p9_ecdf_rules.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")


def map_names_fl(name):
    names = {
        "openwpm_native_no": "Scandinavia",
        "openwpm_native_us": "USA",
        "openwpm_native_cn": "China",
        "openwpm_native_de": "Germany",
        "openwpm_native_is": "Israel",
        "openwpm_native_in": "India",
        "openwpm_native_ae": "VAE",
        "openwpm_native_fr": "France",
        "openwpm_native_jp": "Japan"
    }
    return names.get(name, name)



def download_rule_usage_on_etld_per_filerlists():
    filterlists = ['filterlist_China_matched_rule', 'filterlist_France_matched_rule',
                   'filterlist_Germany_matched_rule', 'filterlist_Indian_matched_rule'
                   , 'filterlist_Israel_matched_rule', 'filterlist_Japanese_matched_rule',
                   'filterlist_Scandinavia_matched_rule', 'filterlist_USA_matched_rule',
                   'filterlist_VAE_matched_rule']


    for filterliste in filterlists:
        country = filterliste.replace('filterlist_', '')
        country = filterliste.replace('_matched_rule', '')

        query = f"""
                SELECT
                  {filterliste} AS rule_{country},
                  COUNT(DISTINCT etld) blocked_etld
                FROM
                  measurement.etld_blocked_rules
                WHERE
                  {filterliste} != ''
                GROUP BY
                  rule_{country}
                ORDER BY blocked_etld desc;
                """

        result_df = exec_select_query(query)

        result_df.to_csv(os.path.join(os.getcwd(), 'data', f'urls_{filterliste}.csv'), index=False)


def plot_grid(df):
    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={"font.size": 18, "axes.titlesize": 14, "axes.labelsize": 14,
               "legend.fontsize": 14, "xtick.labelsize": 12, "ytick.labelsize": 12}, style="white")

    # Create the FacetGrid with Seaborn
    g = sb.FacetGrid(df, col="Country", col_wrap=3, sharey=False, sharex=False)
    g.map_dataframe(sb.barplot, x="X", y="Y", color="black")

    # Set titles and labels
    g.set_titles("{col_name}", weight='bold', fontsize=18)
    g.set_axis_labels("Blocked eTLDs", "Number of rules",)

    for ax in g.axes.flat:
        ax.set_xlabel("Blocked eTLDs", fontsize=14, weight='bold')
        ax.set_ylabel("Number of rules", fontsize=14, weight='bold')
        #ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

        # Only show every 2nd label, but keep first and last labels
        num_bars = len(ax.get_xticklabels())
        if num_bars > 4:
            for i, label in enumerate(ax.get_xticklabels()):
                # Show the first and last labels, hide every second label in between
                if i != 0 and i != num_bars - 1 and i % 2 != 0:
                    label.set_visible(False)

    plt.tight_layout()
    #plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p16_usage_of_rules_by_tld.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")

def prep_data_rules_for_tlds():
    path = os.path.join(os.getcwd(), 'data')
    files = ['urls_filterlist_China_matched_rule.csv', 'urls_filterlist_France_matched_rule.csv',
                   'urls_filterlist_Germany_matched_rule.csv', 'urls_filterlist_Indian_matched_rule.csv'
        , 'urls_filterlist_Israel_matched_rule.csv', 'urls_filterlist_Japanese_matched_rule.csv',
                   'urls_filterlist_Scandinavia_matched_rule.csv', 'urls_filterlist_USA_matched_rule.csv',
                   'urls_filterlist_VAE_matched_rule.csv']

    rule_blocked = dict()
    rule_list = dict()

    d = dict()
    for file in files:
        country = file.replace('urls_filterlist_', '')
        country = country.replace('_matched_rule.csv', '')
        df = pd.read_csv(os.path.join(path, file))

        for index, row in df.iterrows():
            rule = row[f'rule_filterlist_{country}']
            b_etlds = row['blocked_etld']

            rule_blocked[rule] = b_etlds
            rule_list[rule] = country


        num_of_blocked_etlds = list(set(df['blocked_etld'].tolist()))

        grouped_df = df.groupby('blocked_etld')

        _d = dict()
        for num_of_blocked_etld in num_of_blocked_etlds:
            _df = grouped_df.get_group(num_of_blocked_etld)
            _d[len(_df)] = num_of_blocked_etld


        d[country] = _d

    data = []
    for country, values in d.items():
        for y, x in values.items():
            data.append({"Country": country, "Y": y, "X": x})


    blocked_etlds_by_rule = list(rule_blocked.values())
    print(f"[rus.1.0.] Rule blocks eTLDS mean: {np.mean(blocked_etlds_by_rule)}, "
          f"min: {np.min(blocked_etlds_by_rule)}, "
          f"max: {np.max(blocked_etlds_by_rule)}, "
          f"SD: {np.std(blocked_etlds_by_rule)}")
    print(f"[rus.1.1.] most blocked eTLDs {rule_blocked[max(rule_blocked, key=rule_blocked.get)]}, "
          f"in {rule_list[max(rule_blocked, key=rule_blocked.get)]},"
          f"rule: {max(rule_blocked, key=rule_blocked.get)}")

    plot_grid(pd.DataFrame(data))



def testing():
    pass

if __name__ == '__main__':
    # general_numbers()
    plotting()
    # testing()

    #download_rule_usage_on_etld_per_filerlists()
    #prep_data_rules_for_tlds()
