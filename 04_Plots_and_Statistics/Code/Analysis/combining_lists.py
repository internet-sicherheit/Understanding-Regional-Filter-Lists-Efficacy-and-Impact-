import os
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import operator
import pandas as pd
import itertools
import numpy as np
from scipy import stats
from google.cloud import bigquery
from matplotlib.ticker import FuncFormatter

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

def map_browser_id_to_country(browser_id):
    mapping = {
        'openwpm_native_in': 'IN.IN',
        'openwpm_native_us': 'US.US',
        'openwpm_native_cn': 'CN.CN',
        'openwpm_native_jp': 'JP.JP',
        'openwpm_native_de': 'DE.DE',
        'openwpm_native_fr': 'FR.FR',
        'openwpm_native_no': 'NO.NO',
        'openwpm_native_il': 'IS.IS',
        'openwpm_native_ae': 'AE.AE',
        'openwpm_native_in_2': 'IN.INT',
        'openwpm_native_us_2': 'US.INT',
        'openwpm_native_cn_2': 'CN.INT',
        'openwpm_native_jp_2': 'JP.INT',
        'openwpm_native_de_2': 'DE.INT',
        'openwpm_native_fr_2': 'FR.INT',
        'openwpm_native_no_2': 'NO.INT',
        'openwpm_native_il_2': 'IS.INT',
        'openwpm_native_ae_2': 'AE.INT'

    }

    return mapping.get(browser_id, browser_id)


def combining_filterlists():

    result_df_w_baseline = exec_select_query("""
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'US' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_USA_is_blocked
                                              AND browser_id LIKE 'openwpm_native_us'
                                            GROUP BY
                                              browser_id

                                              UNION ALL

                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE+USA' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              (filterlist_Germany_is_blocked
                                                OR filterlist_USA_is_blocked)
                                              AND browser_id LIKE 'openwpm_native_de'
                                            GROUP BY
                                              browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'AE+USA' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  (filterlist_VAE_is_blocked
                                                    OR filterlist_USA_is_blocked)
                                                  AND browser_id LIKE 'openwpm_native_ae'
                                                GROUP BY
                                                  browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'CN+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_China_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_cn'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'JP+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_VAE_is_blocked
                                                   OR filterlist_Japanese_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_jp'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'IL+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Israel_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_il'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'IN+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Indian_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_in'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'FR+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_France_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_fr'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'NO+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Scandinavia_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_no'
                                               GROUP BY
                                                 browser_id

                                                """)

    result_df_wo_baseline = exec_select_query("""
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'US' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_USA_is_blocked
                                              AND browser_id LIKE 'openwpm_native_us'
                                            GROUP BY
                                              browser_id

                                            UNION ALL

                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Germany_is_blocked
                                              AND browser_id LIKE 'openwpm_native_de'
                                            GROUP BY
                                              browser_id

                                              UNION ALL

                                              SELECT
                                                   browser_id,
                                                   COUNT(*) count,
                                                   'AE' AS Blocking_List
                                                 FROM
                                                   measurement.requests
                                                 WHERE
                                                   filterlist_VAE_is_blocked
                                                   AND browser_id LIKE 'openwpm_native_ae'
                                                 GROUP BY
                                                   browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'CN' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_China_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_cn'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'JP' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_VAE_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_jp'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'IL' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Israel_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_il'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'IN' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Indian_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_in'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'FR' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_France_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_fr'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'NO' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Scandinavia_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_no'
                                                GROUP BY
                                                  browser_id
                                                """)

    result_df_only_baseline = exec_select_query("""
                                                SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'US' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_USA_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_us'
                                                GROUP BY
                                                  browser_id


                                                UNION ALL

                                                SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'US' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_USA_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_de'
                                                GROUP BY
                                                  browser_id

                                                  UNION ALL

                                                  SELECT
                                                       browser_id,
                                                       COUNT(*) count,
                                                       'US' AS Blocking_List
                                                     FROM
                                                       measurement.requests
                                                     WHERE
                                                       filterlist_USA_is_blocked
                                                       AND browser_id LIKE 'openwpm_native_ae'
                                                     GROUP BY
                                                       browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                     'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_cn'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_jp'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                     'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_il'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_in'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_fr'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_no'
                                                    GROUP BY
                                                      browser_id
                                                """)

    result_df_w_baseline['browser_id'] = result_df_w_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_w_baseline = result_df_w_baseline.sort_values(by='count', ascending=False)
    result_df_wo_baseline['browser_id'] = result_df_wo_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_wo_baseline = result_df_wo_baseline.sort_values(by='count', ascending=False)
    result_df_only_baseline['browser_id'] = result_df_only_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_only_baseline = result_df_only_baseline.sort_values(by='count', ascending=False)


    merged_df = result_df_w_baseline.merge(result_df_wo_baseline, on='browser_id', how='outer', suffixes=('_List1', '_List2')).merge(result_df_only_baseline, on='browser_id', how='outer', suffixes=('', '_List3'))

    merged_df = merged_df.rename(columns={
        'count': 'count_List3',
        'Blocking_List': 'Blocking_List3'
    })

    total_requests_df = exec_select_query(""" SELECT
                                                  COUNT(*) as total_requests
                                                FROM
                                                  measurement.requests; """)

    fac_total = total_requests_df['total_requests'].tolist()[0]

    merged_df['count_list1_rel_total_requests'] = round(merged_df['count_List1'] / fac_total * 100, 2)
    merged_df['count_list2_rel_total_requests'] =  round(merged_df['count_List2'] / fac_total * 100, 2)
    merged_df['count_list3_rel_total_requests'] =  round(merged_df['count_List3'] / fac_total * 100, 2)

    total_blocked_requests_df = exec_select_query(""" SELECT
                                                  COUNT(*) as total_requests
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  is_blocked_by_filterlist """)

    fac = total_blocked_requests_df['total_requests'].tolist()[0] # 75240401

    merged_df['count_list1_rel'] = round(merged_df['count_List1'] / fac * 100, 2)
    merged_df['count_list2_rel'] =  round(merged_df['count_List2'] / fac * 100, 2)
    merged_df['count_list3_rel'] =  round(merged_df['count_List3'] / fac * 100, 2)

    merged_df = merged_df.sort_values(by='count_List1', ascending=False)

    merged_df = merged_df.fillna(0)

    x_labels = merged_df['browser_id']

    width = 0.30

    r1 = np.arange(len(merged_df['browser_id']))
    r2 = [x + width for x in r1]
    r3 = [x + width for x in r2]

    fig, ax = plt.subplots()
    # bars1 = ax.bar(r1, merged_df['count_List1'], width, label='Local filter list + Baselist')
    # bars2 = ax.bar(r2, merged_df['count_List2'], width, label='Local filter list')
    # bars3 = ax.bar(r3, merged_df['count_List3'], width, color='grey', label='Baselist')

    bars1 = ax.bar(r1, merged_df['count_list1_rel'], width, label='Local filter list + Baseline')
    bars2 = ax.bar(r2, merged_df['count_list2_rel'], width, label='Local filter list')
    bars3 = ax.bar(r3, merged_df['count_list3_rel'], width, color='grey', label='Baseline')


    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sns.set(rc={"font.size": 18, "axes.titlesize": 18, "axes.labelsize": 18,
               "legend.fontsize": 18, "xtick.labelsize": 18, "ytick.labelsize": 18}, style="white")
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    matplotlib.rcParams['font.family'] = 'sans-serif'

    ax.set_xlabel('Profile', fontsize=18,  weight='bold')
    ax.set_ylabel('Relative share of blocked \n requests (in %)', fontsize=18, weight='bold')
    ax.set_xticks(r1)
    ax.set_xticklabels(x_labels, fontsize=14)
    plt.xticks(rotation=45)
    ax.legend()
    plt.tight_layout()


    # Plot
    plt.show()




def statistics_combining_filterlists():
    result_df_w_baseline = exec_select_query("""
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE+USA' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              (filterlist_Germany_is_blocked
                                                OR filterlist_USA_is_blocked)
                                              AND browser_id LIKE 'openwpm_native_de'
                                            GROUP BY
                                              browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'AE+USA' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  (filterlist_VAE_is_blocked
                                                    OR filterlist_USA_is_blocked)
                                                  AND browser_id LIKE 'openwpm_native_ae'
                                                GROUP BY
                                                  browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'CN+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_China_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_cn'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'JP+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_VAE_is_blocked
                                                   OR filterlist_Japanese_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_jp'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'IL+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Israel_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_il'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'IN+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Indian_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_in'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'FR+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_France_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_fr'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'NO+USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 (filterlist_Scandinavia_is_blocked
                                                   OR filterlist_USA_is_blocked)
                                                 AND browser_id LIKE 'openwpm_native_no'
                                               GROUP BY
                                                 browser_id

                                            UNION ALL

                                            SELECT
                                                 browser_id,
                                                 COUNT(*) count,
                                                 'USA' AS Blocking_List
                                               FROM
                                                 measurement.requests
                                               WHERE
                                                 filterlist_VAE_is_blocked
                                                 AND browser_id LIKE 'openwpm_native_us'
                                               GROUP BY
                                                 browser_id
                                                """)

    result_df_wo_baseline = exec_select_query("""
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Germany_is_blocked
                                              AND browser_id LIKE 'openwpm_native_de'
                                            GROUP BY
                                              browser_id

                                              UNION ALL

                                              SELECT
                                                   browser_id,
                                                   COUNT(*) count,
                                                   'AE' AS Blocking_List
                                                 FROM
                                                   measurement.requests
                                                 WHERE
                                                   filterlist_VAE_is_blocked
                                                   AND browser_id LIKE 'openwpm_native_ae'
                                                 GROUP BY
                                                   browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'CN' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_China_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_cn'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'JP' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_VAE_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_jp'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'IL' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Israel_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_il'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'IN' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Indian_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_in'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'FR' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_France_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_fr'
                                                GROUP BY
                                                  browser_id

                                             UNION ALL

                                             SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'NO' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Scandinavia_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_no'
                                                GROUP BY
                                                  browser_id
                                                """)

    result_df_only_baseline = exec_select_query("""
                                                SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'US' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_USA_is_blocked
                                                  AND browser_id LIKE 'openwpm_native_de'
                                                GROUP BY
                                                  browser_id

                                                  UNION ALL

                                                  SELECT
                                                       browser_id,
                                                       COUNT(*) count,
                                                       'US' AS Blocking_List
                                                     FROM
                                                       measurement.requests
                                                     WHERE
                                                       filterlist_USA_is_blocked
                                                       AND browser_id LIKE 'openwpm_native_ae'
                                                     GROUP BY
                                                       browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                     'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_cn'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_jp'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                     'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_il'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_in'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_fr'
                                                    GROUP BY
                                                      browser_id

                                                 UNION ALL

                                                 SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'US' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_USA_is_blocked
                                                      AND browser_id LIKE 'openwpm_native_no'
                                                    GROUP BY
                                                      browser_id
                                                """)

    result_df_w_baseline['browser_id'] = result_df_w_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_w_baseline = result_df_w_baseline.sort_values(by='count', ascending=False)
    result_df_wo_baseline['browser_id'] = result_df_wo_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_wo_baseline = result_df_wo_baseline.sort_values(by='count', ascending=False)
    result_df_only_baseline['browser_id'] = result_df_only_baseline['browser_id'].apply(map_browser_id_to_country)
    result_df_only_baseline = result_df_only_baseline.sort_values(by='count', ascending=False)


    merged_df = result_df_w_baseline.merge(result_df_wo_baseline, on='browser_id', how='outer', suffixes=('_List1', '_List2')).merge(result_df_only_baseline, on='browser_id', how='outer', suffixes=('', '_List3'))

    merged_df = merged_df.rename(columns={
        'count': 'count_List3',
        'Blocking_List': 'Blocking_List3'
    })


    merged_df = merged_df.sort_values(by='count_List1', ascending=False)

    merged_df = merged_df.fillna(0)


    def calculate_percentage_difference(row):
        number1 = row['count_List1']
        number2 = row['count_List2']
        return round(((number1 - number2) / number1) * 100, 2)
        #return ((number1 - number2) / number2) * 100

    merged_df['Difference'] = merged_df.apply(calculate_percentage_difference, axis=1)


    print(merged_df)

    print(f"max: {merged_df['Difference'].max()}, min: {merged_df['Difference'].min()}, mean: {merged_df['Difference'].mean()}, SD: {merged_df['Difference'].std()}")


def sites_per_profile():
    result_df_domains = exec_select_query("""
                                    with url_counts AS (SELECT
                                      top_level_url,
                                      COUNT(DISTINCT browser_id) AS Anzahl
                                    FROM
                                      measurement.requests
                                    WHERE
                                      subpage_id= 0
                                      AND browser_id NOT LIKE '%2'
                                    GROUP BY
                                      top_level_url
                                    HAVING
                                      COUNT(DISTINCT browser_id) >= 1
                                    ORDER BY
                                      Anzahl DESC)


                                    SELECT Anzahl, COUNT(top_level_url) AS url_count
                                    FROM url_counts
                                    WHERE Anzahl BETWEEN 1 AND 9
                                    GROUP BY Anzahl
                                    ORDER BY Anzahl;
                                    """)

    result_df_pages = exec_select_query("""
                                    with url_counts AS (SELECT
                                      url,
                                      COUNT(DISTINCT browser_id) AS Anzahl
                                    FROM
                                      measurement.requests
                                    WHERE
                                      browser_id NOT LIKE '%2'
                                    GROUP BY
                                      url
                                    HAVING
                                      COUNT(DISTINCT browser_id) >= 1
                                    ORDER BY
                                      Anzahl DESC)


                                    SELECT Anzahl, COUNT(url) AS url_count
                                    FROM url_counts
                                    WHERE Anzahl BETWEEN 1 AND 9
                                    GROUP BY Anzahl
                                    ORDER BY Anzahl;
                                """)

    result_df_requests = exec_select_query("""
                                    with foo as (SELECT
                                      url,
                                      COUNT(*) AS request_count,
                                      COUNT(DISTINCT Browser_id) AS unique_browser_count
                                    FROM
                                      measurement.requests
                                    WHERE
                                      browser_id NOT LIKE '%2'
                                    GROUP BY
                                      url
                                    ORDER BY
                                      request_count DESC)


                                    SELECT unique_browser_count, sum(request_count) request_count
                                    FROM foo
                                    group by foo.unique_browser_count
                                    order by unique_browser_count
                                """)


    bar_width = 0.35

    r1 = np.arange(len(result_df_domains['Anzahl']))
    r2 = [x + bar_width for x in r1]
    r3 = [x + bar_width for x in r2]

    matplotlib.rcParams.update({'font.size': 18})

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.bar(r1, result_df_domains['url_count'], width=bar_width, edgecolor='grey', label='Domain count')
    ax.bar(r2, result_df_pages['url_count'], width=bar_width, edgecolor='grey', label='Page count')
    ax.bar(r3, result_df_requests['request_count'], width=bar_width, edgecolor='grey', label='Request count')

    ax.set_xlabel('Number of occurrence in the profiles', fontweight='bold')
    ax.set_ylabel('Number of sites, pages \nand requests', fontweight='bold')
    #ax.set_title('Comparison of URL Counts and Additional Counts')
    ax.set_xticks([r + bar_width/2 for r in range(len(result_df_domains['Anzahl']))])
    ax.set_xticklabels(result_df_domains['Anzahl'].tolist())

    ax.set_yscale('log')

    plt.grid(axis='y', linestyle='--', alpha=0.7)


    plt.legend(loc='upper right',
           ncol=3, fancybox=False, shadow=False, fontsize=14) # bbox_to_anchor=(0.4, 1.05),

    plt.show()


    total_domains = result_df_domains['url_count'].sum()
    unique_domains = result_df_domains[result_df_domains.iloc[:, 0] == 1]['url_count'].tolist()[0]


    total_sites = result_df_pages['url_count'].sum()
    unique_sites = result_df_pages[result_df_pages.iloc[:, 0] == 1]['url_count'].tolist()[0]

    print(f"Percentage of unique sites: {round(unique_sites/total_sites*100, 2)}")

    total_requests = result_df_requests['request_count'].sum()
    unique_requests = result_df_requests[result_df_requests.iloc[:, 0] == 1]['request_count'].tolist()[0]
    multi_requests = result_df_requests[result_df_requests.iloc[:, 0] == 9]['request_count'].tolist()[0]

    print(f"Percentage of unique requests: {round(unique_requests/total_requests*100, 2)}")
    print(f"Percentage of multiple requests (9): {round(multi_requests/total_requests*100, 2)}")


if __name__ == '__main__':
    combining_filterlists()
    #statistics_combining_filterlists()
    # sites_per_profile()
