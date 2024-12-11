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


def categories_overview():
    result_df = exec_select_query(""" SELECT
                                      category,
                                      COUNT(*) count
                                    FROM (
                                      SELECT
                                        NET.REG_DOMAIN(url),
                                        category
                                      FROM
                                        measurement.requests)
                                    GROUP BY
                                      category
                                    ORDER BY
                                      count desc;
                                      """)

    total = result_df['count'].sum()

    for index, row in result_df.iterrows():
        cat = row['category']
        count = row['count']

        print("Detected", count, "(", round(count/total*100,2), "%)" ,"from category", cat)

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

    print(list(set(df['category'].tolist())))

    l = list()

    grouped_df = df.groupby('category')
    for category in categories:
        df = grouped_df.get_group(category)

        max_blocked = df['count'].max()

        row_max_blocked = df.loc[df['count'].idxmax()]
        fl = row_max_blocked['Blocking_List']
        perc = (max_blocked / df['count'].sum()) * 100

        l.append(fl)

        print("[CAT.4.25.0] for category", category, "most tracker blocked by list", fl, "with", perc ,"%")

    d = {i:l.count(i) for i in l}
    print(d)

    #l = list()
    #grouped_df = df.groupby("category")
    #for category in categories:
    #    df = grouped_df.get_group(category)['count']
    #    l.append({'category':category, 'tracker': df.sum(), 'percentage':df.sum()})

    #df = pd.DataFrame(l)
    #df = df.sort_values(by='tracker', ascending=False)

    #total_tracker = df['tracker'].sum()

    #df['percentage'] = round(df['percentage'] / total_tracker * 100, 4)



    #print(df)


def categories_per_profile():
    result_df = exec_select_query("""
                                    SELECT
                                  browser_id,
                                  category,
                                  COUNT(*) c
                                FROM
                                  measurement.requests 
                                GROUP BY
                                  browser_id,
                                  category
                                ORDER BY
                                  browser_id,
                                  c desc;
                                    """)

    grouped_df = result_df.groupby('browser_id')
    browser_ids = result_df['browser_id'].tolist()
    for browser_id in browser_ids:
        df = grouped_df.get_group(browser_id)
        print(df)


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
def statistical_impact():
    result_df = exec_select_query(""" SELECT
                                      browser_id,
                                      category,
                                      site_id,
                                      COUNT(*) count
                                    FROM
                                      measurement.requests
                                    WHERE
                                      (filterlist_China_is_blocked
                                        OR filterlist_France_is_blocked
                                        OR filterlist_Germany_is_blocked
                                        OR filterlist_Indian_is_blocked
                                        OR filterlist_Israel_is_blocked
                                        OR filterlist_Japanese_is_blocked
                                        OR filterlist_Scandinavia_is_blocked
                                        OR filterlist_USA_is_blocked
                                        OR filterlist_VAE_is_blocked)
                                    GROUP BY
                                      browser_id,
                                      site_id,
                                      category;
                                       """)

    grouped_df = result_df.groupby('category')
    df1 = grouped_df.get_group('Sports')['count']
    df2 = grouped_df.get_group('Unknown')['count']
    df3 = grouped_df.get_group('Law & Government')['count']
    df4 = grouped_df.get_group('Computers & Electronics')['count']
    df5 = grouped_df.get_group('Food & Drink')['count']
    df6 = grouped_df.get_group('Pets & Animals')['count']
    df7 = grouped_df.get_group('Beauty & Fitness')['count']
    df8 = grouped_df.get_group('Online Communities')['count']
    df9 = grouped_df.get_group('Autos & Vehicles')['count']
    df10 = grouped_df.get_group('Travel & Transportation')['count']
    df11 = grouped_df.get_group('People & Society')['count']
    df12 = grouped_df.get_group('Business & Industrial')['count']
    df13 = grouped_df.get_group('News')['count']
    df14 = grouped_df.get_group('Finance')['count']
    df15 = grouped_df.get_group('Hobbies & Leisure')['count']
    df16 = grouped_df.get_group('Real Estate')['count']
    df17 = grouped_df.get_group('Books & Literature')['count']
    df18 = grouped_df.get_group('Jobs & Education')['count']
    df19 = grouped_df.get_group('Shopping')['count']
    df20 = grouped_df.get_group('Games')['count']
    df21 = grouped_df.get_group('Arts & Entertainment')['count']
    df22 = grouped_df.get_group('Home & Garden')['count']
    df23 = grouped_df.get_group('Internet & Telecom')['count']

    kruskal_tracker_categories = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14,
                                            df15, df16, df17, df18, df19, df20, df21, df22, df23)
    n = len(result_df)
    eta_square = compute_eta_square(kruskal_tracker_categories[0], 18, n)
    print("[ST.4.26.0] categories ~ trackers (p-value >", kruskal_tracker_categories[1], ") with eta square", eta_square)


def statistical_impact_lists():
    result_df_us = exec_select_query("""
                                    SELECT
                                  browser_id,
                                  category,
                                  site_id,
                                  COUNT(*) count
                                FROM
                                  measurement.requests
                                WHERE
                                  (filterlist_China_is_blocked
                                    OR filterlist_France_is_blocked
                                    OR filterlist_Germany_is_blocked
                                    OR filterlist_Indian_is_blocked
                                    OR filterlist_Israel_is_blocked
                                    OR filterlist_Japanese_is_blocked
                                    OR filterlist_Scandinavia_is_blocked
                                    OR filterlist_VAE_is_blocked)
                                    AND filterlist_USA_is_blocked
                                GROUP BY
                                  browser_id,
                                  site_id,
                                  category;
                                    """)

    grouped_df = result_df_us.groupby('category')
    df1 = grouped_df.get_group('Sports')['count']
    df2 = grouped_df.get_group('Unknown')['count']
    df3 = grouped_df.get_group('Law & Government')['count']
    df4 = grouped_df.get_group('Computers & Electronics')['count']
    df5 = grouped_df.get_group('Food & Drink')['count']
    df6 = grouped_df.get_group('Pets & Animals')['count']
    df7 = grouped_df.get_group('Beauty & Fitness')['count']
    df8 = grouped_df.get_group('Online Communities')['count']
    df9 = grouped_df.get_group('Autos & Vehicles')['count']
    df10 = grouped_df.get_group('Travel & Transportation')['count']
    df11 = grouped_df.get_group('People & Society')['count']
    df12 = grouped_df.get_group('Business & Industrial')['count']
    df13 = grouped_df.get_group('News')['count']
    df14 = grouped_df.get_group('Finance')['count']
    df15 = grouped_df.get_group('Hobbies & Leisure')['count']
    df16 = grouped_df.get_group('Real Estate')['count']
    df17 = grouped_df.get_group('Books & Literature')['count']
    df18 = grouped_df.get_group('Jobs & Education')['count']
    df19 = grouped_df.get_group('Shopping')['count']
    df20 = grouped_df.get_group('Games')['count']
    df21 = grouped_df.get_group('Arts & Entertainment')['count']
    df22 = grouped_df.get_group('Home & Garden')['count']
    df23 = grouped_df.get_group('Internet & Telecom')['count']

    kruskal_tracker_categories_us = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13,df14,
                                               df15, df16, df17, df18, df19, df20, df21, df22, df23)
    n = len(result_df_us)
    eta_square = compute_eta_square(kruskal_tracker_categories_us[0], 18, n)
    print("[ST.4.27.0] categories baseline incl. ~ trackers (p-value >", kruskal_tracker_categories_us[1], ") with eta square",
          eta_square)


    result_df_cn = exec_select_query("""
                                        SELECT
                                      browser_id,
                                      category,
                                      site_id,
                                      COUNT(*) count
                                    FROM
                                      measurement.requests
                                    WHERE
                                      (filterlist_USA_is_blocked
                                        OR filterlist_France_is_blocked
                                        OR filterlist_Germany_is_blocked
                                        OR filterlist_Indian_is_blocked
                                        OR filterlist_Israel_is_blocked
                                        OR filterlist_Japanese_is_blocked
                                        OR filterlist_Scandinavia_is_blocked
                                        OR filterlist_VAE_is_blocked)
                                        AND filterlist_China_is_blocked
                                    GROUP BY
                                      browser_id,
                                      site_id,
                                      category;
                                        """)

    grouped_df = result_df_cn.groupby('category')
    df1 = grouped_df.get_group('Sports')['count']
    df2 = grouped_df.get_group('Unknown')['count']
    df3 = grouped_df.get_group('Law & Government')['count']
    df4 = grouped_df.get_group('Computers & Electronics')['count']
    df5 = grouped_df.get_group('Food & Drink')['count']
    df6 = grouped_df.get_group('Pets & Animals')['count']
    df7 = grouped_df.get_group('Beauty & Fitness')['count']
    df8 = grouped_df.get_group('Online Communities')['count']
    df9 = grouped_df.get_group('Autos & Vehicles')['count']
    df10 = grouped_df.get_group('Travel & Transportation')['count']
    df11 = grouped_df.get_group('People & Society')['count']
    df12 = grouped_df.get_group('Business & Industrial')['count']
    df13 = grouped_df.get_group('News')['count']
    df14 = grouped_df.get_group('Finance')['count']
    df15 = grouped_df.get_group('Hobbies & Leisure')['count']
    df16 = grouped_df.get_group('Real Estate')['count']
    df17 = grouped_df.get_group('Books & Literature')['count']
    df18 = grouped_df.get_group('Jobs & Education')['count']
    df19 = grouped_df.get_group('Shopping')['count']
    df20 = grouped_df.get_group('Games')['count']
    df21 = grouped_df.get_group('Arts & Entertainment')['count']
    df22 = grouped_df.get_group('Home & Garden')['count']
    df23 = grouped_df.get_group('Internet & Telecom')['count']

    kruskal_tracker_categories_cn = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13,
                                                  df14,
                                                  df15, df16, df17, df18, df19, df20, df21, df22, df23)
    n = len(result_df_cn)
    eta_square = compute_eta_square(kruskal_tracker_categories_cn[0], 18, n)
    print("[ST.4.27.1] categories CN fl incl. ~ trackers (p-value >", kruskal_tracker_categories_cn[1],
          ") with eta square",
          eta_square)

    result_df_jp = exec_select_query("""
                                        SELECT
                                      browser_id,
                                      category,
                                      site_id,
                                      COUNT(*) count
                                    FROM
                                      measurement.requests
                                    WHERE
                                      (filterlist_China_is_blocked
                                        OR filterlist_France_is_blocked
                                        OR filterlist_Germany_is_blocked
                                        OR filterlist_Indian_is_blocked
                                        OR filterlist_Israel_is_blocked
                                        OR filterlist_USA_is_blocked 
                                        OR filterlist_Scandinavia_is_blocked
                                        OR filterlist_VAE_is_blocked)
                                        AND filterlist_Japanese_is_blocked
                                    GROUP BY
                                      browser_id,
                                      site_id,
                                      category;
                                        """)

    grouped_df = result_df_jp.groupby('category')
    df1 = grouped_df.get_group('Sports')['count']
    df2 = grouped_df.get_group('Unknown')['count']
    df3 = grouped_df.get_group('Law & Government')['count']
    df4 = grouped_df.get_group('Computers & Electronics')['count']
    df5 = grouped_df.get_group('Food & Drink')['count']
    df6 = grouped_df.get_group('Pets & Animals')['count']
    df7 = grouped_df.get_group('Beauty & Fitness')['count']
    df8 = grouped_df.get_group('Online Communities')['count']
    df9 = grouped_df.get_group('Autos & Vehicles')['count']
    df10 = grouped_df.get_group('Travel & Transportation')['count']
    df11 = grouped_df.get_group('People & Society')['count']
    df12 = grouped_df.get_group('Business & Industrial')['count']
    df13 = grouped_df.get_group('News')['count']
    df14 = grouped_df.get_group('Finance')['count']
    df15 = grouped_df.get_group('Hobbies & Leisure')['count']
    df16 = grouped_df.get_group('Real Estate')['count']
    df17 = grouped_df.get_group('Books & Literature')['count']
    df18 = grouped_df.get_group('Jobs & Education')['count']
    df19 = grouped_df.get_group('Shopping')['count']
    df20 = grouped_df.get_group('Games')['count']
    df21 = grouped_df.get_group('Arts & Entertainment')['count']
    df22 = grouped_df.get_group('Home & Garden')['count']
    df23 = grouped_df.get_group('Internet & Telecom')['count']

    kruskal_tracker_categories_jp = stats.kruskal(df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13,
                                                  df14,
                                                  df15, df16, df17, df18, df19, df20, df21, df22, df23)
    n = len(result_df_jp)
    eta_square = compute_eta_square(kruskal_tracker_categories_jp[0], 18, n)
    print("[ST.4.27.2] categories JP fl incl. ~ trackers (p-value >", kruskal_tracker_categories_jp[1],
          ") with eta square",
          eta_square)


def blocked_requests_categories_only_baseline():
    result_df = exec_select_query(""" 
                                    SELECT 
                                    category,
                                    COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA
                                    FROM
                                  measurement.requests
                                GROUP BY
                                  category
                                ORDER BY
                                blocked_by_USA DESC;
                                    """)

    # drop uknown
    result_df = result_df[result_df.category != 'Unknown']

    print(result_df)


def compare_lists_on_news():
    result_df = exec_select_query("""
                                    SELECT
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
                                  category
                                ORDER BY
                                  blocked_by_USA DESC;
                                    """)

    grouped_df = result_df.groupby('category')
    df_news = grouped_df.get_group('News')

    print(df_news)




if __name__ == '__main__':
    #categories_overview()
    effect_on_categories()

    #categories_per_profile()

    #statistical_impact()

    #statistical_impact_lists()

    #blocked_requests_categories_only_baseline()

    #compare_lists_on_news()

