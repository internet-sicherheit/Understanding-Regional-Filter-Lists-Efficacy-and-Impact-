import pandas as pd
import os
from google.cloud import bigquery

OUTPUT_PATH = os.path.join(os.getcwd(), '..', '..', 'Domains')


def exec_select_query(query):
    """
    Executes the given SQL query using the static Google authentication credentials.

    :param query: The SQL query
    :return: A (pandas) dataframe that contains the results
    """
    # Initialize teh Google BigQuery client. The authentication token should be placed in the working directory in the
    # following path: /resources/google.json
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "resources", "google_bkp.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "..", "..", "resources", "google_gcp.json")
    client = bigquery.Client()

    # Execute the query and retrieve result data (as pandas dataframe)
    result_df = client.query(query).to_dataframe()

    return result_df


if __name__ == '__main__':
    countries = ["us", "cn", "jp", "in", "de", "no", "fr","il", "ae"]
    for country in countries:
        # Get top 10k domains
        query = f""" SELECT
                      distinct net.reg_domain(origin) as domain, experimental.popularity.rank
                    FROM
                      `chrome-ux-report.country_{country}.202402` 
                    order by rank
                    LIMIT 10000; """

        # Get top 10k domains
        result_df = exec_select_query(query)
        # Reduce to the top 1k domains
        result_df_1k = result_df.head(1000)

        filename_1k = f"{country}_top_1k_domains.csv"
        filename_10k = f"{country}_top_10k_domains.csv"

        # Store as CSV
        result_df.to_csv(os.path.join(OUTPUT_PATH, filename_10k), index=False)
        result_df_1k.to_csv(os.path.join(OUTPUT_PATH, filename_1k), index=False)