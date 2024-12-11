import os
import sys
from google.cloud import bigquery
import matplotlib
import matplotlib.pyplot as plt
import seaborn as sb
import pandas as pd


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


def analyze_scenario_1():
    result_df = exec_select_query("""SELECT
                                      browser_id,
                                      COUNTIF(filterlist_China_is_blocked) AS CN,
                                      COUNTIF(filterlist_France_is_blocked) AS FR,
                                      COUNTIF(filterlist_Germany_is_blocked) AS DE,
                                      COUNTIF(filterlist_Indian_is_blocked) AS IND,
                                      COUNTIF(filterlist_Israel_is_blocked) AS ISR,
                                      COUNTIF(filterlist_Japanese_is_blocked) AS JP,
                                      COUNTIF(filterlist_Scandinavia_is_blocked) AS NOW,
                                      COUNTIF(filterlist_USA_is_blocked) AS US,
                                      COUNTIF(filterlist_VAE_is_blocked) AS VA
                                    FROM `measurement.requests`
                                    WHERE browser_id NOT LIKE "%_2"
                                      AND NOT filterlist_USA_is_blocked
                                    GROUP BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.set_index('browser_id')

    local_list_best_in_locale = 0
    for col in result_df:
        cname = col
        if col == 'IND':
            cname = 'IN'
        elif col == 'ISR':
            cname = 'IS'
        elif col == 'NOW':
            cname = "NO"

        if cname in result_df[col].idxmax():
            local_list_best_in_locale += 1

    print("[EUL 1.1] Number of justdomain_lists that worked best in target location", local_list_best_in_locale, "share",
          local_list_best_in_locale / (len(result_df) - 1) * 100)

    result_df = exec_select_query("""SELECT
                                          browser_id,
                                          COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS CN,
                                          COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS FR,
                                          COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS DE,
                                          COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS IND,
                                          COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS ISR,
                                          COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS JP,
                                          COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked) AS NOW,
                                          COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked) AS AE,
                                        FROM `measurement.requests`
                                        WHERE browser_id NOT LIKE "%_2"
                                          AND NOT filterlist_USA_is_blocked
                                        GROUP BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.set_index('browser_id')

    de_result_def = result_df['DE']
    de_result_def = de_result_def.drop("DE.DE")
    print("[EUL 1.2.1] Mean request blocked by German list. avg:", de_result_def.mean(), "min:", de_result_def.min(),
          "max:", de_result_def.max(), "std", de_result_def.std())

    urls_detected_by_lists = pd.Series([])
    for col in result_df:
        cname = col
        if col == 'IND':
            cname = 'IN'
        elif col == 'ISR':
            cname = 'IS'
        elif col == 'NOW':
            cname = "NO"

        index_name = cname + "." + cname
        urls_detected_by_lists = pd.concat([urls_detected_by_lists, pd.Series(result_df[col].drop(index_name).values)])

    # print(urls_detected_by_lists)
    print("[EUL 1.2.2] Mean request blocked by non-German list. avg:", urls_detected_by_lists.mean(), "min:",
          urls_detected_by_lists.min(), "max:", urls_detected_by_lists.max(), "std", urls_detected_by_lists.std())


def analyze_scenario_2():
    result_df = exec_select_query("""SELECT
                                          browser_id,
                                          COUNTIF(filterlist_China_is_blocked) AS CN,
                                          COUNTIF(filterlist_France_is_blocked) AS FR,
                                          COUNTIF(filterlist_Germany_is_blocked) AS DE,
                                          COUNTIF(filterlist_Indian_is_blocked) AS IND,
                                          COUNTIF(filterlist_Israel_is_blocked) AS ISR,
                                          COUNTIF(filterlist_Japanese_is_blocked) AS JP,
                                          COUNTIF(filterlist_Scandinavia_is_blocked) AS NOW,
                                          COUNTIF(filterlist_USA_is_blocked) AS US,
                                          COUNTIF(filterlist_VAE_is_blocked) AS VA
                                        FROM `measurement.requests`
                                        WHERE browser_id LIKE "%_2"
                                          AND NOT filterlist_USA_is_blocked
                                        GROUP BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.set_index('browser_id')

    local_list_best_in_locale = 0
    for col in result_df:
        cname = col
        if col == 'IND':
            cname = 'IN'
        elif col == 'ISR':
            cname = 'IS'
        elif col == 'NOW':
            cname = "NO"

        if cname in result_df[col].idxmax():
            local_list_best_in_locale += 1

    print("[EUL 1.3] Number of justdomain_lists that worked best in international setting", local_list_best_in_locale, "share",
          local_list_best_in_locale / (len(result_df) - 1) * 100)

    result_df_int = exec_select_query("""SELECT
                                              browser_id,
                                              COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS CN,
                                              COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS FR,
                                              COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS DE,
                                              COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS IND,
                                              COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS ISR,
                                              COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS JP,
                                              COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked) AS NOW,
                                              COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked) AS AE,
                                            FROM `measurement.requests`
                                            WHERE browser_id LIKE "%_2"
                                              AND NOT filterlist_USA_is_blocked
                                            GROUP BY browser_id;""")
    result_df_int['browser_id'] = result_df_int['browser_id'].apply(map_browser_id_to_country)
    result_df_int['browser_id'] = result_df_int['browser_id'].str[:2]
    result_df_int = result_df_int.set_index('browser_id')

    result_df_local = exec_select_query("""SELECT
                                          browser_id,
                                          COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS CN,
                                          COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS FR,
                                          COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS DE,
                                          COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS IND,
                                          COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS ISR,
                                          COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked) AS JP,
                                          COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked) AS NOW,
                                          COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked) AS AE,
                                        FROM `measurement.requests`
                                        WHERE browser_id NOT LIKE "%_2"
                                          AND NOT filterlist_USA_is_blocked
                                        GROUP BY browser_id;""")
    result_df_local['browser_id'] = result_df_local['browser_id'].apply(map_browser_id_to_country)
    result_df_local['browser_id'] = result_df_local['browser_id'].str[:2]
    result_df_local = result_df_local.set_index('browser_id')

    result_df_div = result_df_local.div(result_df_int) * 100
    values = result_df_div.values

    print("[EUL 1.4.1] Difference between request blocked in international setting. avg:", values.mean(), "min:", values.min(),
          "max:", values.max(), "std", values.std())


def analyze_scenario_3():
    result_df = exec_select_query("""SELECT
                                         browser_id,
                                           COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS CN,
                                           COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS FR,
                                           COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS DE,
                                           COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS IND,
                                           COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS ISR,
                                           COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS JP,
                                           COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS NOW,
                                           COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_USA_is_blocked) AS AE,
                                           COUNTIF(filterlist_USA_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked) AS US,
                                         FROM `measurement.requests`
                                         GROUP BY browser_id
                                         ORDER BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.rename(columns={'IND': 'IN', 'NOW': 'NO', 'ISR': 'IS'})
    result_df1 = result_df.set_index('browser_id')

    best_in_sec3 = 0
    requests_blocked_sec3 = 0
    for col in result_df1:
        if col == 'browser_id':
            continue
        max_row = result_df1[col].idxmax()
        requests_blocked_sec3 += result_df1[col].sum()
        if max_row.startswith(col):
            best_in_sec3 += 1

    print("[EUL 3.1] Number of justdomain_lists that worked best in scenario setting", best_in_sec3, "share",
          best_in_sec3 / (len(result_df1) / 2) * 100)
    print("[EUL 3.2] Number of requests blocked in Secnario 3", requests_blocked_sec3)


def analyze_appendix():
    result_df = exec_select_query("""SELECT
                                          browser_id,
                                          COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_USA_is_blocked) AS AE,
                                          COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS CN,
                                          COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS DE,
                                          COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS FR,
                                          COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS ISR,
                                          COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS IND,
                                          COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS JP,
                                          COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS NOW,
                                          COUNTIF(filterlist_USA_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_China_is_blocked) AS US,
                                        FROM `measurement.requests`
                                        WHERE browser_id NOT LIKE "%_2"
                                        GROUP BY browser_id
                                        ORDER BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.rename(columns={'IND': 'IN', 'NOW': 'NO', 'ISR': 'IS'})
    result_df['CN'] = result_df['CN'].map('{:,}'.format)
    result_df['FR'] = result_df['FR'].map('{:,}'.format)
    result_df['DE'] = result_df['DE'].map('{:,}'.format)
    result_df['IN'] = result_df['IN'].map('{:,}'.format)
    result_df['IS'] = result_df['IS'].map('{:,}'.format)
    result_df['JP'] = result_df['JP'].map('{:,}'.format)
    result_df['NO'] = result_df['NO'].map('{:,}'.format)
    result_df['AE'] = result_df['AE'].map('{:,}'.format)
    result_df['US'] = result_df['US'].map('{:,}'.format)
    print("APP[1.1] ")
    print(result_df.to_latex(float_format="'{:,}'.format"))


def plot_scenario_overview():
    result_df = exec_select_query("""SELECT
                                        browser_id,
                                          COUNTIF(filterlist_China_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS CN,
                                          COUNTIF(filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS FR,
                                          COUNTIF(filterlist_Germany_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS DE,
                                          COUNTIF(filterlist_Indian_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS IND,
                                          COUNTIF(filterlist_Israel_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS ISR,
                                          COUNTIF(filterlist_Japanese_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS JP,
                                          COUNTIF(filterlist_Scandinavia_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked AND NOT filterlist_USA_is_blocked) AS NOW,
                                          COUNTIF(filterlist_VAE_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_USA_is_blocked) AS AE,
                                          COUNTIF(filterlist_USA_is_blocked AND NOT filterlist_France_is_blocked AND NOT filterlist_Germany_is_blocked AND NOT filterlist_Indian_is_blocked AND NOT filterlist_Israel_is_blocked AND NOT filterlist_Japanese_is_blocked AND NOT filterlist_Scandinavia_is_blocked AND NOT filterlist_China_is_blocked AND NOT filterlist_VAE_is_blocked) AS US,
                                        FROM `measurement.requests`
                                        GROUP BY browser_id
                                        ORDER BY browser_id;""")
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df = result_df.rename(columns={'IND': 'IN', 'NOW': 'NO', 'ISR': 'IS'})
    result_df1 = result_df.set_index('browser_id')

    data_sec_1 = []
    scenario_1_raw_data = result_df[~result_df['browser_id'].str.endswith('.INT')]
    for col in scenario_1_raw_data:
        if col == 'browser_id':
            continue
        row_index = col+'.'+col
        data_sec_1.append((col, row_index, "Scenario 1", result_df1.loc[row_index][col]))
    scenario_1_data = pd.DataFrame(data_sec_1, columns=['list', 'measurement', 'scenario', "blocked"])
    # print(scenario_1_data)

    data_sec_2 = []
    scenario_2_raw_data = result_df[result_df['browser_id'].str.endswith('.INT')]
    for col in scenario_2_raw_data:
        if col == 'browser_id':
            continue
        row_index = col+'.INT'
        data_sec_2.append((col, row_index, "Scenario 2", result_df1.loc[row_index][col]))
    scenario_2_data = pd.DataFrame(data_sec_2, columns=['list', 'measurement', 'scenario', "blocked"])
    # print(scenario_2_data)

    data_sec_3 = []
    for col in result_df:
        if col == 'browser_id':
            continue
        scenario_3_raw_data_tmp = result_df[~result_df['browser_id'].str.startswith(col+".")]
        for index, row in scenario_3_raw_data_tmp.iterrows():
            data_sec_3.append((col,  result_df1.loc[row['browser_id']][col]))
    scenario_3_data = pd.DataFrame(data_sec_3, columns=['list', "blocked"])
    scenario_3_data = scenario_3_data.groupby(['list']).mean()
    scenario_3_data['scenario'] = 'Scenario 3'
    scenario_3_data['measurement'] = 'Mean.INT'
    scenario_3_data = scenario_3_data.rename_axis('list').reset_index()
    # print(scenario_3_data)

    all_scenario_data = pd.concat([scenario_1_data, scenario_2_data, scenario_3_data])
    # print(all_scenario_data)

    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={'figure.figsize': (13, 6), "font.size": 18, "axes.titlesize": 14, "axes.labelsize": 14,
               "legend.fontsize": 14, "xtick.labelsize": 12, "ytick.labelsize": 12}, style="white")

    g = sb.FacetGrid(all_scenario_data, col="scenario")
    g.map(sb.barplot, "list", "blocked")#.set(yscale = 'log')
    g.set_titles("{col_name}")
    g.set_axis_labels(x_var='Used list', y_var="Blocked requests")
    g.set_xticklabels(rotation=90)
    # plt.show()
    plt.savefig(os.path.join(os.getcwd(), 'plots', "p7_tracker_in_scenario.pdf"), dpi=600,
                 transparent=False, bbox_inches='tight', format="pdf")


if __name__ == '__main__':
    # analyze_scenario_1()
    # analyze_scenario_2()
    analyze_scenario_3()
    # plot_scenario_overview()
    # analyze_appendix()
