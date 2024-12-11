import os
from google.cloud import bigquery
from scipy import stats


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


def get_measurement_overview():
    query_string = """WITH visited_sites AS (SELECT browser_id, count(distinct site_id) as visited_sites FROM measurement.requests GROUP BY browser_id),
                        
                        visited_subpages AS (SELECT browser_id, count(distinct visit_id) as visited_subpage FROM measurement.requests GROUP BY browser_id),
                        
                        known_tracker AS (SELECT browser_id, count(*) as tracker FROM measurement.requests WHERE is_tracker = 1 GROUP BY browser_id),
                        
                        blocker AS (SELECT
                          browser_id,
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
                          browser_id),
                        
                        cookies  AS (SELECT
                          browser_id,
                          COUNTIF(category = 'Unknown') AS unkown,
                          COUNTIF(category='Strictly Necessary') AS necessary,
                          COUNTIF(category = 'Functionality') AS functionality,
                          COUNTIF(category = 'Performance') AS performance,
                          COUNTIF(category = 'Targeting/Advertising') AS advertising
                        FROM
                          `measurement.cookies`
                        GROUP BY
                          browser_id)
                        
                        SELECT vs.browser_id, vs.visited_sites, vp.visited_subpage, c.unkown, c.necessary,c.functionality,
                        c.performance,c.advertising,  b.blocked_by_VAE, b.blocked_by_China,
                        b.blocked_by_Germany,b.blocked_by_France,b.blocked_by_Israel,
                        b.blocked_by_India,b.blocked_by_Japan,b.blocked_by_Scandinavia,
                        b.blocked_by_USA, FROM 
                        visited_subpages vp 
                          JOIN visited_sites vs ON vs.browser_id = vp.browser_id 
                          JOIN cookies c ON  vs.browser_id = c.browser_id 
                          JOIN known_tracker kt ON vs.browser_id = kt.browser_id
                          JOIN blocker b ON vs.browser_id = b.browser_id
                        ORDER BY vs.browser_id;
                            """

    result_df = exec_select_query(query_string)
    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df['visited_sites'] = result_df['visited_sites'].map('{:,}'.format)
    result_df['visited_subpage'] = result_df['visited_subpage'].map('{:,}'.format)
    result_df['unkown'] = result_df['unkown'].map('{:,}'.format)
    result_df['necessary'] = result_df['necessary'].map('{:,}'.format)
    result_df['functionality'] = result_df['functionality'].map('{:,}'.format)
    result_df['performance'] = result_df['performance'].map('{:,}'.format)
    result_df['advertising'] = result_df['advertising'].map('{:,}'.format)

    result_df['blocked_by_VAE'] = result_df['blocked_by_VAE'].map('{:,}'.format)
    result_df['blocked_by_China'] = result_df['blocked_by_China'].map('{:,}'.format)
    result_df['blocked_by_Germany'] = result_df['blocked_by_Germany'].map('{:,}'.format)
    result_df['blocked_by_France'] = result_df['blocked_by_France'].map('{:,}'.format)
    result_df['blocked_by_Israel'] = result_df['blocked_by_Israel'].map('{:,}'.format)
    result_df['blocked_by_India'] = result_df['blocked_by_India'].map('{:,}'.format)
    result_df['blocked_by_Japan'] = result_df['blocked_by_Japan'].map('{:,}'.format)
    result_df['blocked_by_Scandinavia'] = result_df['blocked_by_Scandinavia'].map('{:,}'.format)
    result_df['blocked_by_USA'] = result_df['blocked_by_USA'].map('{:,}'.format)

    print(result_df)

    print(result_df.to_latex())


def tracker_overview():
    result_df = exec_select_query("""SELECT count(*) AS number, (count(*) / (SELECT count(*) FROM measurement.requests ) * 100) AS perc,
                                    FROM `measurement.requests` 
                                    WHERE filterlist_USA_is_blocked;""")
    print("[4.9.1] Number of requests blocked by EasyList", result_df)

    result_df = exec_select_query("""SELECT count(*) AS blocked_etld1, 
                                    FROM (SELECT DISTINCT(etld) 
                                            FROM `measurement.requests`
                                            WHERE filterlist_USA_is_blocked);""")
    print("[4.9.2] Number of blocked eTLD+1 by EasyList", result_df)

    result_df = exec_select_query("""SELECT
                                      browser_id,
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
                                    GROUP BY browser_id""")
    # TODO
    #print(result_df.to_latex())
    print("[4.9.4] Lists that worked better in other locations", result_df)


    result_df = exec_select_query("""SELECT
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
                                         measurement.requests;""")
    df_transposed = result_df.T
    df_transposed.columns = ['number']
    df_sorted = df_transposed.sort_values(by='number', ascending=False)
    print("[4.9.5] Top justdomain_lists\n", df_sorted)

    baseline_blocked = df_sorted.loc['blocked_by_USA'].iloc[0]
    df_sorted = df_sorted.drop('blocked_by_USA')
    df_sorted['number'] = df_sorted['number'].apply(lambda x: 100 - (x / baseline_blocked * 100))
    print("[4.9.6] Less requests blocked than US list: Mean:", df_sorted['number'].mean(), "Max:",
          df_sorted['number'].max(), "Min:", df_sorted['number'].min(), "SD:", df_sorted['number'].std())

    result_df = exec_select_query("""SELECT COUNT(*) AS number, (COUNT(*) / (SELECT COUNT(*)FROM `measurement.requests` WHERE filterlist_USA_is_blocked)*100) AS share 
                                        FROM `measurement.requests`
                                        WHERE filterlist_USA_is_blocked 
                                        AND (filterlist_China_is_blocked 
                                            OR filterlist_France_is_blocked 
                                            OR filterlist_Germany_is_blocked 
                                            OR filterlist_Indian_is_blocked 
                                            OR filterlist_Israel_is_blocked 
                                            OR filterlist_Japanese_is_blocked 
                                            OR filterlist_Scandinavia_is_blocked 
                                            OR filterlist_VAE_is_blocked);""")

    print("[4.9.7] Share of request blocked by EAsyList AND one other list", result_df)

    result_df_baseline = exec_select_query("""SELECT COUNT(*) FROM `measurement.requests` WHERE filterlist_USA_is_blocked;""")
    number_of_blocked_requests_by_baseline = float(result_df_baseline.iloc[0])

    result_df = exec_select_query("""SELECT
                                      COUNTIF(filterlist_China_is_blocked) AS blocked_by_China,
                                      COUNTIF(filterlist_France_is_blocked) AS blocked_by_France,
                                      COUNTIF(filterlist_Germany_is_blocked) AS blocked_by_Germany,
                                      COUNTIF(filterlist_Indian_is_blocked) AS blocked_by_India,
                                      COUNTIF(filterlist_Israel_is_blocked) AS blocked_by_Israel,
                                      COUNTIF(filterlist_Japanese_is_blocked) AS blocked_by_Japan,
                                      COUNTIF(filterlist_Scandinavia_is_blocked) AS blocked_by_Scandinavia,
                                      COUNTIF(filterlist_USA_is_blocked) AS blocked_by_USA,
                                      COUNTIF(filterlist_VAE_is_blocked) AS blocked_by_VAE
                                    FROM `measurement.requests`
                                    WHERE filterlist_USA_is_blocked;""")
    df_transposed = result_df.T
    df_transposed.columns = ['number']
    df_transposed['number'] = df_transposed['number'].map(lambda x: x / number_of_blocked_requests_by_baseline * 100)
    df_transposed = df_transposed.drop('blocked_by_USA')

    print("[4.9.8] Share of request blocked by EasyList AND one other list groped by list\n", df_transposed['number'])
    print("[4.9.9] Less requests blocked than US list: Mean:", df_transposed['number'].mean(), "Max:",
          df_transposed['number'].max(), "Min:", df_transposed['number'].min(), "SD:", df_transposed['number'].std())


def testing():
    pass


if __name__ == '__main__':
    #get_measurement_overview()
    # testing()
    tracker_overview()
