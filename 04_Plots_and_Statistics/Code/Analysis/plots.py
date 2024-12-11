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
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
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


def tracker_on_locations():
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

    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={'figure.figsize': (14, 9), "font.size": 18, "axes.titlesize": 18, "axes.labelsize": 18,
               "legend.fontsize": 18, "xtick.labelsize": 18, "ytick.labelsize": 18}, style="white")
#
    #result_df_p1 = result_df_p1.sort_values(by='browser_id', ascending=False)
    #result_df_p2 = result_df_p2.sort_values(by='browser_id', ascending=False)
    #result_df_p1['browser_id'] = result_df_p1['browser_id'].apply(map_browser_id_to_country)
    result_df_p1 = result_df_p1.sort_values(by='count', ascending=False)
    #result_df_p2['browser_id'] = result_df_p2['browser_id'].apply(map_browser_id_to_country)
    result_df_p2 = result_df_p2.sort_values(by='count', ascending=False)
    df_merged = pd.concat([result_df_p1, result_df_p2], ignore_index=True, sort=False)
    df_merged = df_merged.sort_values(by="browser_id", ascending=False)
    df_merged['browser_id'] = df_merged['browser_id'].apply(map_browser_id_to_country)

    fig, ax1 = plt.subplots()
    #ax1.legend(ncol=3, loc=2, bbox_to_anchor=(0.5, -0.05))
    p = sns.histplot(df_merged, x='browser_id', hue='Blocking_List', weights='count', multiple='stack', ax=ax1)
    #ax1.set_yscale('log')

    sns.move_legend(p, loc=1, ncol=3, title="", bbox_to_anchor=(1, 1))

    plt.xlabel('Measurement location', fontsize=18,  weight='bold')
    plt.ylabel('Number of trackers', fontsize=18,  weight='bold')
    # ax1.set_xticklabels(fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p4_tracker_per_country.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")


def impact_filterlists():
    result_df = exec_select_query(""" SELECT
                                      *
                                    FROM (
                                      SELECT
                                        "China" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_China_is_blocked
                                      GROUP BY
                                        filterlist_China_is_blocked
                                      UNION ALL
                                      SELECT
                                        "USA" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_USA_is_blocked
                                      GROUP BY
                                        filterlist_USA_is_blocked
                                      UNION ALL
                                      SELECT
                                        "VAE" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_VAE_is_blocked
                                      GROUP BY
                                        filterlist_VAE_is_blocked
                                      UNION ALL
                                      SELECT
                                        "Indian" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_Indian_is_blocked
                                      GROUP BY
                                        filterlist_Indian_is_blocked
                                      UNION ALL
                                      SELECT
                                        "Japanese" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_Japanese_is_blocked
                                      GROUP BY
                                        filterlist_Japanese_is_blocked
                                      UNION ALL
                                      SELECT
                                        "Scandinavia" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_Scandinavia_is_blocked
                                      GROUP BY
                                        filterlist_Scandinavia_is_blocked
                                      UNION ALL
                                      SELECT
                                        "Israel" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_Israel_is_blocked
                                      GROUP BY
                                        filterlist_Israel_is_blocked
                                      UNION ALL
                                      SELECT
                                        "Germany" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_Germany_is_blocked
                                      GROUP BY
                                        filterlist_Germany_is_blocked
                                      UNION ALL
                                      SELECT
                                        "France" AS country,
                                        COUNT(*) AS detected
                                      FROM
                                        measurement.requests_backup
                                      WHERE
                                        is_tracker=1
                                        AND filterlist_France_is_blocked
                                      GROUP BY
                                        filterlist_France_is_blocked) """)

    result_df = result_df.sort_values(by='detected', ascending=False)
    plt.figure(figsize=(10, 6))
    plt.bar(result_df['country'], result_df['detected'], color='blue')
    plt.yscale('log')  # Logarithmische Skala auf der y-Achse
    plt.xlabel('Country')
    plt.ylabel('Identified Tracker by filterlists (log scale)')
    plt.title('Detected tracker by filter justdomain_lists from different countries')
    plt.xticks(rotation=45)
    plt.tight_layout()
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p5_detected_tracker_per_filterlist.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")


def occurency_of_cookies_in_locations():
    result_df = exec_select_query(""" SELECT
                                          c,
                                          COUNT(c) occurency
                                        FROM (
                                          SELECT
                                            name,
                                            COUNT(DISTINCT browser_id) c
                                          FROM (
                                            SELECT
                                              browser_id,
                                              name
                                            FROM
                                              measurement.cookies)
                                          GROUP BY
                                            name
                                          ORDER BY
                                            c)
                                        GROUP BY
                                          c
                                          ORDER BY
                                          c; """)

    result_df = result_df.sort_values(by='occurency', ascending=False)
    result_df['c'] = result_df['c'].astype(str)

    plt.figure(figsize=(10, 6))
    bars = plt.bar(result_df['c'], result_df['occurency'], color='blue')
    plt.yscale('log')  # Logarithmische Skala auf der y-Achse
    plt.xlabel('Profile')
    plt.ylabel('Occurency of cookies (log scale)')
    plt.title('Occurency of cookies in profiles')
    plt.xticks(rotation=45)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2, height, str(round(height, 2)), ha='center', va='bottom',
                 rotation=45)

    plt.tight_layout()
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p6_cookie_occurency.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")


def blocked_trackers_hm():
    result_df = exec_select_query(""" SELECT
                                          browser_id,
                                          COUNT(*) AS blocked
                                        FROM
                                          measurement.requests
                                        WHERE
                                          is_blocked_by_filterlist
                                        GROUP BY
                                          browser_id;  """)

    # Pivot-Tabelle erstellen, um die Daten für die Heatmap vorzubereiten
    pivot_df = result_df.pivot(index='browser_id', columns='blocked_trackers', values='blocked_trackers')

    # Heatmap erstellen
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_df, annot=True, cmap='coolwarm')
    plt.title('Blocked Trackers by Locations')
    plt.xlabel('Number of Blocked Trackers')
    plt.ylabel('Location')
    plt.show()

    # plt.savefig(os.path.join(os.getcwd(), 'plots', "p7_blocked_tracker.pdf"), dpi=600,
    #            transparent=False, bbox_inches='tight', format="pdf")

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

def stacked_bar_charts():
    result_df = exec_select_query("""  SELECT
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
                                            browser_id;
                                            """)


    result_df['browser_id'] = result_df['browser_id'].apply(map_browser_id_to_country)
    result_df.set_index('browser_id', inplace=True)
    print(result_df)

    # Create stacked bar chart
    ax = result_df.plot(kind='bar', stacked=True)
    ax.set_yscale('log')

    # Set meta data
    plt.title('Blocked Request for each country by each filter justdomain_lists')
    plt.xlabel('Country')
    plt.ylabel('Blocked requests')
    ax.legend(title='Werte', bbox_to_anchor=(0.5, -0.2), loc='upper center', ncol=3, bbox_transform=ax.transAxes)

    plt.show()


def tracker_on_category():
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
        l.append({'category': category, 'Blocking_List':'CN','count':df['blocked_by_China'].sum()})
        l.append({'category': category, 'Blocking_List': 'NO', 'count': df['blocked_by_Scandinavia'].sum()})
        l.append({'category': category, 'Blocking_List': 'FR', 'count': df['blocked_by_France'].sum()})
        l.append({'category': category, 'Blocking_List': 'DE', 'count': df['blocked_by_Germany'].sum()})
        l.append({'category': category, 'Blocking_List': 'JP', 'count': df['blocked_by_Japan'].sum()})


    df = pd.DataFrame(l)
    df = df[df.category != 'Unknown']

    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={'figure.figsize': (13, 7), "font.size": 18, "axes.titlesize": 18, "axes.labelsize": 18,
               "legend.fontsize": 18, "xtick.labelsize": 18, "ytick.labelsize": 18}, style="white")

    fig, ax1 = plt.subplots()
    p = sns.histplot(df, x='category', hue='Blocking_List', weights='count', multiple='stack', ax=ax1)

    #ax1.set_yscale('log')

    sns.move_legend(p, ncol=1, title="", loc="upper left", bbox_to_anchor=(1, 1))

    plt.xlabel('Categories', weight='bold', fontsize=24, va="bottom")
    plt.ylabel('Number of trackers', weight='bold', fontsize=24)
    plt.xticks(rotation=90, fontsize=20)
    plt.tight_layout()
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p7_tracker_on_category.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")


if __name__ == '__main__':
    tracker_on_locations()
    # tracker_on_category()
    # impact_filterlists()
    # occurency_of_cookies_in_locations()
    # blocked_trackers_hm()

    #stacked_bar_charts()
    # tracker_on_category()
