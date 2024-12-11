import os
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import operator
import pandas as pd
import itertools
import numpy as np
from scipy import stats
from google.cloud import bigquery

from statistics import mean, median, stdev


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


def jaccard_similarity(set1, set2):
    # intersection of two sets
    intersection = len(set1.intersection(set2))
    # Unions of two sets
    union = len(set1.union(set2))

    return intersection / union


def compute_eta_square(h, k, n):
    """
    Computes the eta^2 ("eta square") value:
    eta2[h] = (h - k + 1) / (n - k)

    :param h: The result of the test. Not the p-value!
    :param k: The number of different categories
    :param n: The numbe rof samples
    :return:  The computed eta^2 value
    """
    return (h - k + 1) / (n - k)

def tracker_per_category():
    result_df = exec_select_query(""" SELECT
                                      browser_id,
                                      cookie_category,
                                      COUNT(*) AS tracker
                                    FROM
                                      measurement.requests
                                    WHERE
                                      is_tracker=1
                                    GROUP BY
                                      browser_id,
                                      cookie_category
                                    ORDER BY
                                      browser_id; """)

    grouped_df = result_df.groupby('browser_id')
    df1 = grouped_df.get_group("openwpm_native_ae")['tracker']
    df2 = grouped_df.get_group("openwpm_native_cn")['tracker']
    df3 = grouped_df.get_group("openwpm_native_de")['tracker']
    df4 = grouped_df.get_group("openwpm_native_fr")['tracker']
    df5 = grouped_df.get_group("openwpm_native_il")['tracker']
    df6 = grouped_df.get_group("openwpm_native_in")['tracker']
    df7 = grouped_df.get_group("openwpm_native_jp")['tracker']
    df8 = grouped_df.get_group("openwpm_native_no")['tracker']
    df9 = grouped_df.get_group("openwpm_native_us")['tracker']
    df10 = grouped_df.get_group("openwpm_native_ae_2")['tracker']
    df11 = grouped_df.get_group("openwpm_native_cn_2")['tracker']
    df12 = grouped_df.get_group("openwpm_native_de_2")['tracker']
    df13 = grouped_df.get_group("openwpm_native_fr_2")['tracker']
    df14 = grouped_df.get_group("openwpm_native_il_2")['tracker']
    df15 = grouped_df.get_group("openwpm_native_in_2")['tracker']
    df16 = grouped_df.get_group("openwpm_native_jp_2")['tracker']
    df17 = grouped_df.get_group("openwpm_native_no_2")['tracker']
    df18 = grouped_df.get_group("openwpm_native_us_2")['tracker']

    kruskal_category = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13,
                                               df14, df15, df16, df17, df18)
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_category[0], 18, n)
    print("[ST.4.1] p-value >", kruskal_category[1], "with eta square", eta_square)


def tracker_per_location():
    result_df = exec_select_query(""" SELECT
                                      browser_id,
                                      url,
                                      COUNT(*) AS tracker
                                    FROM
                                      measurement.requests
                                    WHERE
                                      is_tracker=1
                                    GROUP BY
                                      browser_id,
                                      url
                                    ORDER BY
                                      browser_id """)

    grouped_df = result_df.groupby('browser_id')
    df1 = grouped_df.get_group("openwpm_native_ae")['tracker']
    df2 = grouped_df.get_group("openwpm_native_cn")['tracker']
    df3 = grouped_df.get_group("openwpm_native_de")['tracker']
    df4 = grouped_df.get_group("openwpm_native_fr")['tracker']
    df5 = grouped_df.get_group("openwpm_native_il")['tracker']
    df6 = grouped_df.get_group("openwpm_native_in")['tracker']
    df7 = grouped_df.get_group("openwpm_native_jp")['tracker']
    df8 = grouped_df.get_group("openwpm_native_no")['tracker']
    df9 = grouped_df.get_group("openwpm_native_us")['tracker']
    df10 = grouped_df.get_group("openwpm_native_ae_2")['tracker']
    df11 = grouped_df.get_group("openwpm_native_cn_2")['tracker']
    df12 = grouped_df.get_group("openwpm_native_de_2")['tracker']
    df13 = grouped_df.get_group("openwpm_native_fr_2")['tracker']
    df14 = grouped_df.get_group("openwpm_native_il_2")['tracker']
    df15 = grouped_df.get_group("openwpm_native_in_2")['tracker']
    df16 = grouped_df.get_group("openwpm_native_jp_2")['tracker']
    df17 = grouped_df.get_group("openwpm_native_no_2")['tracker']
    df18 = grouped_df.get_group("openwpm_native_us_2")['tracker']

    kruskal_browser_id_tracker = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df17,df18)
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_browser_id_tracker[0], 18, n)
    print("[ST.4.0] p-value >",kruskal_browser_id_tracker[1], "with eta square",eta_square)


def tracker_per_filterlist():
    result_df = exec_select_query(""" """)

    grouped_df = result_df.groupby('browser_id')
    



    kruskal_filterlists = stats.kruskal()
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_filterlists[0], 18, n)
    print("[ST.4.2] p-value >", kruskal_filterlists[1], "with eta square", eta_square)


def jaccard_distance_cookies():
    result_df = exec_select_query(""" SELECT
                                      distinct browser_id,
                                      name
                                    FROM
                                      measurement.cookies; """)

    grouped_df = result_df.groupby("browser_id")
    df1 = grouped_df.get_group("openwpm_native_ae")['name']
    df2 = grouped_df.get_group("openwpm_native_cn")['name']
    df3 = grouped_df.get_group("openwpm_native_de")['name']
    df4 = grouped_df.get_group("openwpm_native_fr")['name']
    df5 = grouped_df.get_group("openwpm_native_il")['name']
    df6 = grouped_df.get_group("openwpm_native_in")['name']
    df7 = grouped_df.get_group("openwpm_native_jp")['name']
    df8 = grouped_df.get_group("openwpm_native_no")['name']
    df9 = grouped_df.get_group("openwpm_native_us")['name']
    df10 = grouped_df.get_group("openwpm_native_ae_2")['name']
    df11 = grouped_df.get_group("openwpm_native_cn_2")['name']
    df12 = grouped_df.get_group("openwpm_native_de_2")['name']
    df13 = grouped_df.get_group("openwpm_native_fr_2")['name']
    df14 = grouped_df.get_group("openwpm_native_il_2")['name']
    df15 = grouped_df.get_group("openwpm_native_in_2")['name']
    df16 = grouped_df.get_group("openwpm_native_jp_2")['name']
    df17 = grouped_df.get_group("openwpm_native_no_2")['name']
    df18 = grouped_df.get_group("openwpm_native_us_2")['name']

    data = dict()
    data['openwpm_native_ae'] = set(df1.tolist())
    data['openwpm_native_cn'] = set(df2.tolist())
    data['openwpm_native_de'] = set(df3.tolist())
    data['openwpm_native_fr'] = set(df4.tolist())
    data['openwpm_native_il'] = set(df5.tolist())
    data['openwpm_native_in'] = set(df6.tolist())
    data['openwpm_native_jp'] = set(df7.tolist())
    data['openwpm_native_no'] =set(df8.tolist())
    data['openwpm_native_us'] = set(df9.tolist())
    data['openwpm_native_ae_2'] = set(df10.tolist())
    data['openwpm_native_cn_2'] = set(df11.tolist())
    data['openwpm_native_de_2'] = set(df12.tolist())
    data['openwpm_native_fr_2'] = set(df13.tolist())
    data['openwpm_native_il_2'] = set(df14.tolist())
    data['openwpm_native_in_2'] = set(df15.tolist())
    data['openwpm_native_jp_2'] = set(df16.tolist())
    data['openwpm_native_no_2'] = set(df17.tolist())
    data['openwpm_native_us_2'] = set(df18.tolist())

    results = list()
    for browser_id_1, set_cookie_names_1 in data.items():
        for browser_id_2, set_cookie_names_2 in data.items():
            if browser_id_1 == browser_id_2:
                continue
            else:
                similarity = jaccard_similarity(set_cookie_names_1,set_cookie_names_2)
                results.append(similarity)
                print("Jaccard similarity between", browser_id_1, "and", browser_id_2, "is", similarity)

    print("[ST.4.3] MIN:", min(results), ", MAX:", max(results), ", Mean:", mean(results), ", median:", median(results), ", SD:", stdev(results))


def cookies_profile():
    result_df = exec_select_query(""" SELECT
                                          browser_id,
                                          site_id,
                                          COUNT(*) ct
                                        FROM
                                          measurement.cookies
                                        WHERE
                                          in_cookiejar=1
                                        GROUP BY
                                          site_id,
                                          browser_id; """)

    grouped_df = result_df.groupby("browser_id")
    df1 = grouped_df.get_group("openwpm_native_ae")['ct']
    df2 = grouped_df.get_group("openwpm_native_cn")['ct']
    df3 = grouped_df.get_group("openwpm_native_de")['ct']
    df4 = grouped_df.get_group("openwpm_native_fr")['ct']
    df5 = grouped_df.get_group("openwpm_native_il")['ct']
    df6 = grouped_df.get_group("openwpm_native_in")['ct']
    df7 = grouped_df.get_group("openwpm_native_jp")['ct']
    df8 = grouped_df.get_group("openwpm_native_no")['ct']
    df9 = grouped_df.get_group("openwpm_native_us")['ct']
    df10 = grouped_df.get_group("openwpm_native_ae_2")['ct']
    df11 = grouped_df.get_group("openwpm_native_cn_2")['ct']
    df12 = grouped_df.get_group("openwpm_native_de_2")['ct']
    df13 = grouped_df.get_group("openwpm_native_fr_2")['ct']
    df14 = grouped_df.get_group("openwpm_native_il_2")['ct']
    df15 = grouped_df.get_group("openwpm_native_in_2")['ct']
    df16 = grouped_df.get_group("openwpm_native_jp_2")['ct']
    df17 = grouped_df.get_group("openwpm_native_no_2")['ct']
    df18 = grouped_df.get_group("openwpm_native_us_2")['ct']

    kruskal_cookie_profile= stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10,df11,df12,df13,df14,df15,df16,df17,df18)
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_cookie_profile[0], 18, n)
    print("[ST.4.4] profile ~ cookies (p-value >", kruskal_cookie_profile[1], ") with eta square", eta_square)

def jaccard_distnace_tracker():
    results = list()
    print("[ST.4.5] MIN:", min(results), ", MAX:", max(results), ", Mean:", mean(results), ", median:", median(results), ", SD:", stdev(results))

def trackers_profile():
    result_df = exec_select_query(""" SELECT
                                      browser_id,
                                      site_id,
                                      COUNT(*) ct
                                    FROM
                                      measurement.requests
                                    WHERE
                                      is_blocked_by_filterlist
                                    GROUP BY
                                      site_id,
                                      browser_id; """)

    grouped_df = result_df.groupby("browser_id")
    df1 = grouped_df.get_group("openwpm_native_ae")['ct']
    df2 = grouped_df.get_group("openwpm_native_cn")['ct']
    df3 = grouped_df.get_group("openwpm_native_de")['ct']
    df4 = grouped_df.get_group("openwpm_native_fr")['ct']
    df5 = grouped_df.get_group("openwpm_native_il")['ct']
    df6 = grouped_df.get_group("openwpm_native_in")['ct']
    df7 = grouped_df.get_group("openwpm_native_jp")['ct']
    df8 = grouped_df.get_group("openwpm_native_no")['ct']
    df9 = grouped_df.get_group("openwpm_native_us")['ct']
    df10 = grouped_df.get_group("openwpm_native_ae_2")['ct']
    df11 = grouped_df.get_group("openwpm_native_cn_2")['ct']
    df12 = grouped_df.get_group("openwpm_native_de_2")['ct']
    df13 = grouped_df.get_group("openwpm_native_fr_2")['ct']
    df14 = grouped_df.get_group("openwpm_native_il_2")['ct']
    df15 = grouped_df.get_group("openwpm_native_in_2")['ct']
    df16 = grouped_df.get_group("openwpm_native_jp_2")['ct']
    df17 = grouped_df.get_group("openwpm_native_no_2")['ct']
    df18 = grouped_df.get_group("openwpm_native_us_2")['ct']

    kruskal_tracker_profile = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14,
                                           df15, df16, df17, df18)
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_tracker_profile[0], 18, n)
    print("[ST.4.6] profile ~ trackers (p-value >", kruskal_tracker_profile[1], ") with eta square", eta_square)

def map_browser_id_to_country(browser_id):
    mapping = {
        'openwpm_native_in': 'IN.IN',
        'openwpm_native_us': 'US.US',
        'openwpm_native_cn': 'CN.CN',
        'openwpm_native_jp': 'JP.JP',
        'openwpm_native_de': 'DE.DE',
        'openwpm_native_fr': 'FR.FR',
        'openwpm_native_no': 'NO.NO',
        'openwpm_native_il': 'IL.IL',
        'openwpm_native_ae': 'AE.AE',
        'openwpm_native_in_2': 'IN.INT',
        'openwpm_native_us_2': 'US.INT',
        'openwpm_native_cn_2': 'CN.INT',
        'openwpm_native_jp_2': 'JP.INT',
        'openwpm_native_de_2': 'DE.INT',
        'openwpm_native_fr_2': 'FR.INT',
        'openwpm_native_no_2': 'NO.INT',
        'openwpm_native_il_2': 'IL.INT',
        'openwpm_native_ae_2': 'AE.INT'

    }
    return mapping.get(browser_id, browser_id)

def combining_lists():
    result_df_p1 = exec_select_query("""SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Germany_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
                                            GROUP BY
                                              browser_id
                                            UNION ALL
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'IS' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Israel_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              filterlist_Japanese_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
                                            GROUP BY
                                              browser_id
                                            UNION ALL
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'SC' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Indian_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
                                            GROUP BY
                                              browser_id
                                            UNION ALL
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'VE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_VAE_is_blocked
                                              AND browser_id NOT LIKE '%_2'
                                            GROUP BY
                                              browser_id ;""")

    result_df_p2 = exec_select_query("""SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Germany_is_blocked
                                              AND browser_id LIKE '%_2'
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
                                              AND browser_id LIKE '%_2'
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
                                              AND browser_id LIKE '%_2'
                                            GROUP BY
                                              browser_id
                                            UNION ALL
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'IS' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Israel_is_blocked
                                              AND browser_id LIKE '%_2'
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
                                              filterlist_Japanese_is_blocked
                                              AND browser_id LIKE '%_2'
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
                                              AND browser_id LIKE '%_2'
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
                                              AND browser_id LIKE '%_2'
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
                                              AND browser_id LIKE '%_2'
                                            GROUP BY
                                              browser_id
                                            UNION ALL
                                            SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'VE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_VAE_is_blocked
                                              AND browser_id LIKE '%_2'
                                            GROUP BY
                                              browser_id;""")

    result_df_p1['browser_id'] = result_df_p1['browser_id'].apply(map_browser_id_to_country)
    result_df_p1 = result_df_p1.sort_values(by='count', ascending=False)
    result_df_p2['browser_id'] = result_df_p2['browser_id'].apply(map_browser_id_to_country)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)

    df_merged = pd.concat([result_df_p1, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)

    grouped_df = df_merged.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IL.IL")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")
    df10 = grouped_df.get_group("AE.INT")
    df11 = grouped_df.get_group("CN.INT")
    df12 = grouped_df.get_group("DE.INT")
    df13 = grouped_df.get_group("FR.INT")
    df14 = grouped_df.get_group("IL.INT")
    df15 = grouped_df.get_group("IN.INT")
    df16 = grouped_df.get_group("JP.INT")
    df17 = grouped_df.get_group("NO.INT")
    df18 = grouped_df.get_group("US.INT")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18]

    l = list()
    for df in dfs:
        df_total_blocks = df['count'].sum()
        for index, row in df.iterrows():
            browser_id1 = row['browser_id']
            count1 = row['count']
            blocked_list1 = row['Blocking_List']

            for index, row in df.iterrows():
                browser_id2 = row['browser_id']
                count2 = row['count']
                blocked_list2 = row['Blocking_List']

                if blocked_list1 != blocked_list2:
                    l.append({'browser_id': browser_id1, 'count': count1+count2,'count_perc':(count1+count2)/df_total_blocks * 100, 'list1': blocked_list1, 'list2': blocked_list2})

    df = pd.DataFrame(l)

    print("[STA.4.15.1] Blocked requests with two combined justdomain_lists: Mean:",df['count'].mean(), '(', df['count_perc'].mean(), '%)' ,"Max:",df['count'].max(), "Min:",df['count'].min(), "SD:", df['count'].std())

    l = list()
    for df in dfs:
        for index, row in df.iterrows():
            browser_id1 = row['browser_id']
            count1 = row['count']
            blocked_list1 = row['Blocking_List']

            for index, row in df1.iterrows():
                browser_id2 = row['browser_id']
                count2 = row['count']
                blocked_list2 = row['Blocking_List']

                for index, row in df1.iterrows():
                    browser_id3 = row['browser_id']
                    count3 = row['count']
                    blocked_list3 = row['Blocking_List']

                if blocked_list1 != blocked_list2 and blocked_list1 != blocked_list3 and blocked_list3 != blocked_list2:
                    l.append({'browser_id': browser_id1, 'count': count1 + count2 + count3, 'list1': blocked_list1,
                              'list2': blocked_list2, 'list2': blocked_list3})

    df = pd.DataFrame(l)

    print("[STA.4.15.2] Blocked requests with two combinet justdomain_lists: Mean:",df['count'].mean(), "Max:",df['count'].max(), "Min:",df['count'].min(), "SD:", df['count'].std())

    print("[STA.4.17.0]", )


    grouped_df = df_merged.groupby('Blocking_List')
    df1 = grouped_df.get_group("VE")['count']
    df2 = grouped_df.get_group("CN")['count']
    df3 = grouped_df.get_group("DE")['count']
    df4 = grouped_df.get_group("FR")['count']
    df5 = grouped_df.get_group("IS")['count']
    df6 = grouped_df.get_group("IN")['count']
    df7 = grouped_df.get_group("JP")['count']
    df8 = grouped_df.get_group("NO")['count']
    df9 = grouped_df.get_group("US")['count']
    #df10 = grouped_df.get_group("AE.INT")['count']
    #df11 = grouped_df.get_group("CN.INT")['count']
    #df12 = grouped_df.get_group("DE.INT")['count']
    #df13 = grouped_df.get_group("FR.INT")['count']
    #df14 = grouped_df.get_group("IS.INT")['count']
    #df15 = grouped_df.get_group("IN.INT")['count']
    #df16 = grouped_df.get_group("JP.INT")['count']
    #df17 = grouped_df.get_group("NO.INT")['count']
    #df18 = grouped_df.get_group("US.INT")['count']

    kruskal_used_list = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9)#, df10, df11, df12, df13, df14,
                                            #df15, df16, df17, df18)
    n = len(df_merged)
    eta_square = compute_eta_square(kruskal_used_list[0], 9, n)
    print("[ST.4.16.0] blocked requests ~ filter justdomain_lists (p-value >", kruskal_used_list[1], ") with eta square", eta_square)


def effect_of_country_specific_list():
    result_df = exec_select_query(""" SELECT
                                          browser_id,
                                          COUNT(*) count,
                                          'DE' AS Blocking_List
                                        FROM
                                          measurement.requests
                                        WHERE
                                          filterlist_Germany_is_blocked
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          filterlist_Japanese_is_blocked
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
                                        GROUP BY
                                          browser_id ;
                                          """)

    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.sort_values(by='count', ascending=False)

    grouped_df = result_df.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IL.IL")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9]

    l = list()
    for df in dfs:
        name = df['browser_id'].tolist()[0].split(".")[0]
        row = df.loc[df['Blocking_List'] == name]
        row = row.iloc[0]
        count = row['count']
        percent = count / df['count'].sum()
        #print(percent)
        l.append({'browser_id':name, 'count': count, 'percent':percent})

    df = pd.DataFrame(l)
    print("[ST.4.17.0] Mean:", df['count'].mean() ,", Mean (%):", df['percent'].mean() ,", Min:",df['count'].min(),", Max:",df['count'].max(),", SD:", df['count'].std())

    result_df_p2 = exec_select_query("""SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'DE' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Germany_is_blocked
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  filterlist_Japanese_is_blocked
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
                                                GROUP BY
                                                  browser_id;""")


    result_df_p2['browser_id'] = result_df_p2['browser_id'].apply(map_browser_id_to_country)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)

    df_merged = pd.concat([result_df, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)

    grouped_df = df_merged.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IL.IL")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")
    df10 = grouped_df.get_group("AE.INT")
    df11 = grouped_df.get_group("CN.INT")
    df12 = grouped_df.get_group("DE.INT")
    df13 = grouped_df.get_group("FR.INT")
    df14 = grouped_df.get_group("IL.INT")
    df15 = grouped_df.get_group("IN.INT")
    df16 = grouped_df.get_group("JP.INT")
    df17 = grouped_df.get_group("NO.INT")
    df18 = grouped_df.get_group("US.INT")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18]

    l = list()
    for df in dfs:
        browser_id = df['browser_id'].tolist()[0]
        name = df['browser_id'].tolist()[0].split(".")[0]
        row = df.loc[df['Blocking_List'] == name]
        row = row.iloc[0]
        count = row['count']


        for index, row in df.iterrows():
            name2 = row['browser_id']
            count2 = row['count']
            list2 = row['Blocking_List']

            if name != list2:
                v = count + count2
                percent = v / df['count'].sum()

                l.append({'browser_id':browser_id, 'list':list2, 'count': v, 'percentage': percent })

    df = pd.DataFrame(l)
    print("[ST.4.18.0] Mean:", df['count'].mean() ,", Mean (%):", df['percentage'].mean() ,", Min:",df['count'].min(),", Max:",df['count'].max(),", SD:", df['count'].std())






def calc(t1,t2,total):
    return (t1+t2)/total * 100


def effect_of_local_filterlists():
    result_df_p1 = exec_select_query(""" SELECT
                                          browser_id,
                                          COUNT(*) count,
                                          'DE' AS Blocking_List
                                        FROM
                                          measurement.requests
                                        WHERE
                                          filterlist_Germany_is_blocked
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          filterlist_Japanese_is_blocked
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
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
                                          AND browser_id NOT LIKE '%_2'
                                        GROUP BY
                                          browser_id ;
                                          """)

    result_df_p2 = exec_select_query("""SELECT
                                                      browser_id,
                                                      COUNT(*) count,
                                                      'DE' AS Blocking_List
                                                    FROM
                                                      measurement.requests
                                                    WHERE
                                                      filterlist_Germany_is_blocked
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      filterlist_Japanese_is_blocked
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
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
                                                      AND browser_id LIKE '%_2'
                                                    GROUP BY
                                                      browser_id;""")

    result_df_p1['browser_id'] = result_df_p1['browser_id'].apply(map_browser_id_to_country)
    result_df_p1 = result_df_p1.sort_values(by='count', ascending=False)
    result_df_p2['browser_id'] = result_df_p2['browser_id'].apply(map_browser_id_to_country)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)
    df_merged = pd.concat([result_df_p1, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)

    grouped_df = df_merged.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IL.IL")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")
    df10 = grouped_df.get_group("AE.INT")
    df11 = grouped_df.get_group("CN.INT")
    df12 = grouped_df.get_group("DE.INT")
    df13 = grouped_df.get_group("FR.INT")
    df14 = grouped_df.get_group("IL.INT")
    df15 = grouped_df.get_group("IN.INT")
    df16 = grouped_df.get_group("JP.INT")
    df17 = grouped_df.get_group("NO.INT")
    df18 = grouped_df.get_group("US.INT")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18]

    l = list()
    for df in dfs:
        max = df['count'].max()

        for index,row in df.iterrows():
            count = row['count']

            if count == max:
                browser_id = row['browser_id']
                fl = row['Blocking_List']

                local_list = browser_id.split(".")[0] == fl
                l.append({"Profile": browser_id, "Filterlist":fl, "blocked_requests": max, "local_list": local_list})

    df = pd.DataFrame(l)
    local_list = df['local_list'].tolist()

    print(local_list.count(True))


def comp_int_loc():
    result_df = exec_select_query(""" 
                                    SELECT
                                  *
                                FROM (
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_Germany_is_blocked
                                    AND browser_id LIKE 'openwpm_native_de%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_France_is_blocked
                                    AND browser_id LIKE 'openwpm_native_fr%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_USA_is_blocked
                                    AND browser_id LIKE 'openwpm_native_us%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_China_is_blocked
                                    AND browser_id LIKE 'openwpm_native_cn%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_Japanese_is_blocked
                                    AND browser_id LIKE 'openwpm_native_jp%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_Scandinavia_is_blocked
                                    AND browser_id LIKE 'openwpm_native_no%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_Indian_is_blocked
                                    AND browser_id LIKE 'openwpm_native_in%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_Israel_is_blocked
                                    AND browser_id LIKE 'openwpm_native_il%'
                                  GROUP BY
                                    browser_id
                                  UNION ALL
                                  SELECT
                                    browser_id,
                                    COUNT(*) blocked_requests
                                  FROM
                                    measurement.requests
                                  WHERE
                                    filterlist_VAE_is_blocked
                                    AND browser_id LIKE 'openwpm_native_ae%'
                                  GROUP BY
                                    browser_id)
                                ORDER BY
                                  browser_id;
                                    """)

    comparisons = []
    for i in range(0, len(result_df), 2):
        if i + 1 < len(result_df):
            entry1 = result_df.iloc[i]
            entry2 = result_df.iloc[i + 1]

            diff = ""
            if entry1['blocked_requests'] > entry2['blocked_requests']:
                diff = "local"
            else:
                diff = "international"

            comparison = {
                'browser_id_1': entry1['browser_id'],
                'blocked_requests_1': entry1['blocked_requests'],
                'browser_id_2': entry2['browser_id'],
                'blocked_requests_2': entry2['blocked_requests'],
                'blocked_requests_diff':  diff
            }
            comparisons.append(comparison)

    print(pd.DataFrame(comparisons))



def effect_on_different_blocks():
    result_df_p1 = exec_select_query(""" SELECT
                                              browser_id,
                                              COUNT(*) count,
                                              'DE' AS Blocking_List
                                            FROM
                                              measurement.requests
                                            WHERE
                                              filterlist_Germany_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              filterlist_Japanese_is_blocked
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
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
                                              AND browser_id NOT LIKE '%_2'
                                            GROUP BY
                                              browser_id ;
                                              """)

    result_df_p2 = exec_select_query("""SELECT
                                                          browser_id,
                                                          COUNT(*) count,
                                                          'DE' AS Blocking_List
                                                        FROM
                                                          measurement.requests
                                                        WHERE
                                                          filterlist_Germany_is_blocked
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          filterlist_Japanese_is_blocked
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
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
                                                          AND browser_id LIKE '%_2'
                                                        GROUP BY
                                                          browser_id;""")

    result_df_p1 = result_df_p1.sort_values(by='count', ascending=False)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)
    df_merged = pd.concat([result_df_p1, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)
    df_merged['browser_id'] = df_merged['browser_id'].apply(map_browser_id_to_country)

    grouped_df = df_merged.groupby("Blocking_List")
    df1 = grouped_df.get_group("AE")
    df2 = grouped_df.get_group("CN")
    df3 = grouped_df.get_group("DE")
    df4 = grouped_df.get_group("FR")
    df5 = grouped_df.get_group("IL")
    df6 = grouped_df.get_group("IN")
    df7 = grouped_df.get_group("JP")
    df8 = grouped_df.get_group("NO")
    df9 = grouped_df.get_group("US")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9]

    l = list()
    for df in dfs:
        fl = df['Blocking_List'].tolist()[0]
        international = 0
        local = 0

        for index, row in df.iterrows():
            browser_id = row['browser_id']
            count = row['count']

            if ".IN" in browser_id:
                # International
                international += count
            else:
                # Local
                local += count

        if local > international:
            greater = "local"
            diff = 100 - (international / local * 100)
        elif local == international:
            greater ="even"
        else:
            greater ="international"
            diff = 100 - (local / international * 100)



        l.append({"blocking_list":fl, "international":international, "local":local, "greater": greater, "diff": diff})

    df = pd.DataFrame(l)
    print(df)

def effect_on_categories():
    result_df = exec_select_query(""" SELECT
                                              browser_id,
                                              category,
                                              COUNTIF(filterlist_China_is_blocked) AS blocked_by_China,
                                              COUNTIF(filterlist_France_is_blocked) AS blocked_by_France,
                                              COUNTIF(filterlist_Germany_is_blocked) AS blocked_by_Germany,
                                              COUNTIF(filterlist_Indian_is_blocked) AS blocked_by_India,
                                              COUNTIF(filterlist_Israel_is_blocked) AS blocked_by_Israel,
                                              COUNTIF(filterlist_Japanese_is_blocked) AS blocked_by_Japan,
                                              COUNTIF(filterlist_Scandinavia_is_blocked) AS blocked_by_Scandinavia,
                                              COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA,
                                              COUNTIF(filterlist_VAE_is_blocked) AS blocked_by_VAE
                                            FROM
                                              measurement.requests
                                            GROUP BY
                                              browser_id,
                                              category
                                              order by browser_id;
                                            """)

    grouped_df = result_df.groupby("category")

    categories = list(set(result_df['category'].tolist()))

    l = list()
    for category in categories:
        df = grouped_df.get_group(category)
        df = df.drop('browser_id', axis=1)

        l.append({'category': category, 'Blocking_List': 'US', 'count': df['blocked_by_USA'].sum()})
        l.append({'category': category, 'Blocking_List': 'IS', 'count': df['blocked_by_Israel'].sum()})
        l.append({'category': category, 'Blocking_List': 'AE', 'count': df['blocked_by_VAE'].sum()})
        l.append({'category': category, 'Blocking_List': 'IN', 'count': df['blocked_by_India'].sum()})
        l.append({'category': category, 'Blocking_List': 'CN', 'count': df['blocked_by_China'].sum()})
        l.append({'category': category, 'Blocking_List': 'NO', 'count': df['blocked_by_Scandinavia'].sum()})
        l.append({'category': category, 'Blocking_List': 'FR', 'count': df['blocked_by_France'].sum()})
        l.append({'category': category, 'Blocking_List': 'DE', 'count': df['blocked_by_Germany'].sum()})
        l.append({'category': category, 'Blocking_List': 'JP', 'count': df['blocked_by_Japan'].sum()})

    df = pd.DataFrame(l)

    print(df)

def local_list_with_baselist():
    result_df_p1 = exec_select_query("""SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'DE' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Germany_is_blocked
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  filterlist_Japanese_is_blocked
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
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
                                                  AND browser_id NOT LIKE '%_2'
                                                GROUP BY
                                                  browser_id ;""")

    result_df_p2 = exec_select_query("""SELECT
                                                  browser_id,
                                                  COUNT(*) count,
                                                  'DE' AS Blocking_List
                                                FROM
                                                  measurement.requests
                                                WHERE
                                                  filterlist_Germany_is_blocked
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  filterlist_Japanese_is_blocked
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
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
                                                  AND browser_id LIKE '%_2'
                                                GROUP BY
                                                  browser_id;""")

    result_df_p1['browser_id'] = result_df_p1['browser_id'].apply(map_browser_id_to_country)
    result_df_p1 = result_df_p1.sort_values(by='count', ascending=False)
    result_df_p2['browser_id'] = result_df_p2['browser_id'].apply(map_browser_id_to_country)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)

    df_merged = pd.concat([result_df_p1, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)

    grouped_df = df_merged.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IL.IL")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")
    df10 = grouped_df.get_group("AE.INT")
    df11 = grouped_df.get_group("CN.INT")
    df12 = grouped_df.get_group("DE.INT")
    df13 = grouped_df.get_group("FR.INT")
    df14 = grouped_df.get_group("IL.INT")
    df15 = grouped_df.get_group("IN.INT")
    df16 = grouped_df.get_group("JP.INT")
    df17 = grouped_df.get_group("NO.INT")
    df18 = grouped_df.get_group("US.INT")

    dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16, df17, df18]

    l = list()
    for df in dfs:
        baselist = df.loc[df['Blocking_List'] == 'US']
        browser_id = df['browser_id'].tolist()[0]
        local_list = df.loc[df['Blocking_List'] == browser_id.split(".")[0]]

        blocked_requests_by_baselist = baselist['count'].values[0]
        blocked_requests_by_local_list = local_list['count'].values[0]

        combinet_list = blocked_requests_by_baselist+blocked_requests_by_local_list

        l.append({"browser_id":browser_id, 'increased_blocked_requests': ((combinet_list-blocked_requests_by_local_list)/blocked_requests_by_local_list)*100, 'blocked_requests':combinet_list})

    df = pd.DataFrame(l)

    print("[STA.4.18.0] Blocked requests with combinet local- and baselists: Mean:",df['blocked_requests'].mean(), "Max:",df['blocked_requests'].max(), "Min:",df['blocked_requests'].min(), "SD:", df['blocked_requests'].std())
    print("[STA.4.18.1] Blocked requests with combinet local- and baselists: Mean:",df['increased_blocked_requests'].mean(), "Max:",df['increased_blocked_requests'].max(), "Min:",df['increased_blocked_requests'].min(), "SD:", df['increased_blocked_requests'].std())



if __name__ == '__main__':
    #tracker_per_category()
    #tracker_per_location()

    # jaccard_distance_cookies()

    #cookies_profile()

    # trackers_profile()

    combining_lists()

    #effect_of_country_specific_list()


    #comp_int_loc()

    #effect_on_different_blocks()

    #local_list_with_baselist()

"""
grouped_df = df_merged.groupby('browser_id')
    df1 = grouped_df.get_group("AE.AE")
    df2 = grouped_df.get_group("CN.CN")
    df3 = grouped_df.get_group("DE.DE")
    df4 = grouped_df.get_group("FR.FR")
    df5 = grouped_df.get_group("IS.IS")
    df6 = grouped_df.get_group("IN.IN")
    df7 = grouped_df.get_group("JP.JP")
    df8 = grouped_df.get_group("NO.NO")
    df9 = grouped_df.get_group("US.US")
    df10 = grouped_df.get_group("AE.INT")
    df11 = grouped_df.get_group("CN.INT")
    df12 = grouped_df.get_group("DE.INT")
    df13 = grouped_df.get_group("FR.INT")
    df14 = grouped_df.get_group("IS.INT")
    df15 = grouped_df.get_group("IN.INT")
    df16 = grouped_df.get_group("JP.INT")
    df17 = grouped_df.get_group("NO.INT")
    df18 = grouped_df.get_group("US.INT")

    dfs = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12,df13,df14,df15,df16,df17,df18]

    l = list()
    for df in dfs:
        d = dict()
        for index, row in df.iterrows():
            c1 = row['count']
            l1 = row['Blocking_List']
            for index, row in df.iterrows():
                c2 = row['count']
                l2 = row['Blocking_List']

                if l1 != l2:
                    #d[(l1,l2)] = calc(c1, c2, total_blocked_requests_df1)
                    d[(l1, l2)] = c1 + c2

        max_pair = max(d.items(), key=operator.itemgetter(1))[0]
        max_pair_value = d[max_pair]

        l.append(d)

    df = pd.DataFrame.from_records(l)

    print(df)
"""