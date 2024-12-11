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

def percentage_difference(value1, value2):
    """
    Calculates the percentage difference between two values.

    Parameters:
    value1 (float): The first value (initial value).
    value2 (float): The second value.

    Returns:
    float: The percentage difference between value1 and value2.
    """
    difference = value2 - value1
    relative_difference = difference / value1
    percentage_difference = relative_difference * 100
    return percentage_difference


def combinding_lists():
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
        l.append({'browser_id':df['browser_id'].tolist()[0],'location':df['browser_id'].tolist()[0].split(".")[0],'blocked_requests':df['count'].sum()})

    df = pd.DataFrame(l)

    locations = df['location'].tolist()

    l = list()

    grouped_df = df.groupby('location')
    for location in locations:
        df = grouped_df.get_group(location)
        international_id = ""
        international_blocked = 0
        local_id = ""
        local_blocked = 0
        for index, row in df.iterrows():
            if 'INT' in row['browser_id']:
                international_id = row['location']
                international_blocked = row['blocked_requests']
            else:
                local_id = row['location']
                local_blocked = row['blocked_requests']

        performed_better_in = ""
        diff = 0
        if international_blocked > local_blocked:
            diff = percentage_difference(local_blocked, international_blocked)
            performed_better_in = "international (Scenario 2)"
            more_rules = international_blocked - local_blocked
        elif international_blocked < local_blocked:
            diff = percentage_difference(international_blocked, local_blocked)
            performed_better_in = "local (Scenario 1)"
            more_rules = local_blocked - international_blocked

            #print(diff)
        if diff <= 5 :
            performed_better_in = "equal (Scenario 1 and 2)"

        l.append({'location':local_id, 'international_requests_blocked':international_blocked, 'local_requests_blocked': local_blocked, 'better_detection': performed_better_in, 'perc_more_blocked_in_profile':diff, 'total_number_more_blocked_rules': more_rules})

    df = pd.DataFrame(l)
    df = df.drop_duplicates()

    grouped_df = df.groupby('better_detection')
    df_local = grouped_df.get_group('local (Scenario 1)')
    df_international = grouped_df.get_group('international (Scenario 2)')

   # print(df_local)

    #jp = df_local.iloc[df_local['location'] == 'JP']
    #us = df_local.iloc[df_local['location'] == 'US']

    #jp_diff = jp['local_requests_blocked'] - jp['international_requests_blocked']
    #us_diff = us['local_requests_blocked'] - us['international_requests_blocked']


    #df_local_stats = pd.DataFrame([{},{}])

    print(df)

    #print("[ECL4.20.0] List that perform better on Scenario 1 (local)", ,"List that perform better on Scenario 2 (International)",)
    print("[ECL4.20.1] blocked requests in local profile", df_local['total_number_more_blocked_rules'].mean(),"(", df_local['perc_more_blocked_in_profile'].mean(),")")
    print("[ECL4.20.2] blocked requests in international profile", df_international['total_number_more_blocked_rules'].mean(),"(", df_international['perc_more_blocked_in_profile'].mean(),")")



if __name__ == '__main__':
    combinding_lists()