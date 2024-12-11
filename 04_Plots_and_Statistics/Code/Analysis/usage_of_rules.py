import os
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd
import numpy as np
import seaborn as sns
from scipy import stats
from google.cloud import bigquery

from lca_standard_grpahs import plot_grouped_stackedbars, build_comparison_table


def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "..", "resources", "google.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


def rule_list():
    result_df = exec_select_query(""" 
                                    SELECT
                                      rule,
                                      identified_tr
                                    FROM (
                                      SELECT
                                        DISTINCT rule,
                                        identified_tr
                                      FROM
                                        `filterlists.measurement.filterlist_rules`
                                      WHERE
                                        identified_tr > 0)
                                    ORDER BY
                                      identified_tr desc
                                """)

    result_df.to_csv('rules_masterlist.csv', index=False)


def diff_to_baseline():
    result_df_baseline = exec_select_query("""
                    SELECT
                          COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA
                        FROM
                  measurement.requests;
                    """)

    result_df_master_list = exec_select_query("""
                                        SELECT SUM(tr) s FROM (
                                    SELECT
                                      rule,
                                      identified_tr tr
                                    FROM (
                                      SELECT
                                        DISTINCT rule,
                                        identified_tr
                                      FROM
                                        `filterlists.measurement.filterlist_rules`
                                      WHERE
                                        identified_tr > 0)
                                    ORDER BY
                                      identified_tr desc);
                                        """)

    baseline = result_df_baseline['blocked_by_USA'].tolist()[0]
    master_list = result_df_master_list['s'].tolist()[0]

    print("[UOR.4.30.0] masterlist blocks", master_list-baseline, "(",baseline/master_list*100,"%) more requests")


if __name__ == '__main__':
    #rule_list()

    diff_to_baseline()

