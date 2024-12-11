import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy.stats import spearmanr
from matplotlib.patches import Patch
from google.cloud import bigquery

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


def map_names(name):
    names = {
        "abpvn": "Vietnamese",
        "adblock-latvian": "Latvian",
        "adblockbg": "Bulgarian",
        "adfilt": "Scandinavia",
        "antiadblockfilters": "Anti Adblock Filter list",
        "easylist": "USA",
        "easylistchina": "China",
        "easylistczechandslovak": "Czech and Slovak",
        "easylistdutch": "Dutch",
        "easylistgermany": "Germany",
        "EasyListHebrew": "Israel",
        "easylistitaly": "Italy",
        "easylistpolish": "Polish",
        "easylistportuguese": "Portuguese",
        "easylistspanish": "Spanish",
        "easylist_lithuania": "Lithuania",
        "IndianList": "India",
        "indonesianadblockrules": "Indonesian",
        "KoreanList": "Korea",
        "listear": "VAE",
        "listefr": "France",
        "ruadlist": "Russia",
        "filter": "Japan"
    }
    return names.get(name, name)

def map_names_fl(name):
    names = {
        "blocked_by_Scandinavia": "Scandinavia",
        "blocked_by_USA": "USA",
        "blocked_by_China": "China",
        "blocked_by_Gernaby": "Germany",
        "blocked_by_Israel": "Israel",
        "blocked_by_India": "India",
        "blocked_by_VAE": "VAE",
        "blocked_by_France": "France",
        "blocked_by_Japan": "Japan"
    }
    return names.get(name, name)


def split_on_commit(text):
    # Split the text on the word "commit" and keep the "commit" in each block
    blocks = text.split("new_commit")

    # Reattach the "commit" to the beginning of each block (skip the first split part if it's empty)
    blocks = [("commit" + block).strip() for block in blocks if block.strip()]

    return blocks




def create_stacked_bar(df):
    plt.rcParams.update({'font.size': 19})

    # Apply the 'map_names' function to 'repository' column
    df['repository'] = df['repository'].apply(map_names)

    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure 'additions' and 'deletions' are numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce').fillna(0)
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce').fillna(0)

    # Filter data from 2022 onwards to 4.2024
    df = df[(df['timestamp'] >= "2023-04-01") & (df['timestamp'] < "2024-04-01")]

    # Extract month and year for grouping
    df['month_year'] = df['timestamp'].dt.to_period('M').dt.to_timestamp()

    # Filter only 'added', 'modified', 'deleted' commit types (case-insensitive)
    df['commit_type'] = df['commit_type'].str.lower()
    df_filtered = df[df['commit_type'].isin(['added', 'modified', 'deleted'])]

    # Check if df_filtered is empty
    if df_filtered.empty:
        print("No data available for the specified commit types and date range.")
        return

    # Group by 'month_year', 'commit_type', 'repository' and count the occurrences
    counts_df = df_filtered.groupby(['month_year', 'commit_type', 'repository']).size().reset_index(name='count')

    # Get the unique months, commit types, and repositories
    months = sorted(counts_df['month_year'].unique())
    commit_types = ['added', 'modified', 'deleted']  # The order as per your request
    repositories = sorted(counts_df['repository'].unique())

    N = len(months)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.25  # the width of the bars

    fig, ax = plt.subplots(figsize=(14, 8))

    # Colors for the repositories (cycling through colors)
    colors = plt.cm.get_cmap('tab20', len(repositories)).colors

    # Hatch patterns for commit types
    hatch_patterns = {
        'added': '  ',
        'modified': '+++',
        'deleted': '...'
    }

    # Initialize lists for legends
    repo_handles = []
    repo_labels = []
    commit_type_handles = []
    commit_type_labels = []

    for i, commit_type in enumerate(commit_types):
        # Initialize bottom to zero for stacking
        bottoms = np.zeros(N)
        # Get data for the specific commit_type
        dfs = counts_df[counts_df['commit_type'] == commit_type]
        # Pivot to get repositories as columns
        dfs = dfs.pivot(index='month_year', columns='repository', values='count').fillna(0)
        # Reindex to ensure all months and repositories are included
        dfs = dfs.reindex(index=months, columns=repositories, fill_value=0)
        for j, repository in enumerate(repositories):
            counts = dfs[repository].values.astype(float)
            # Plot the bar with hatch patterns and edge colors
            bars = ax.bar(ind + i * width, counts, width, bottom=bottoms,
                          color=colors[j % len(colors)],
                          hatch=hatch_patterns[commit_type],
                          edgecolor='black')
         

            # Update the bottom for stacking
            bottoms += counts

            # Add repository to legend once
            if i == 0:
                repo_handles.append(Patch(facecolor=colors[j % len(colors)], label=repository))
                repo_labels.append(repository)
        # Add commit type to legend once
        commit_type_handles.append(Patch(facecolor='white', edgecolor='black',
                                         hatch=hatch_patterns[commit_type], label=commit_type))
        commit_type_labels.append(commit_type)

    for i, month in enumerate(months):
            # Calculate the center of the bar group
            x_pos = ind[i] + width  # Center position for each bar group
            # Place the labels below the bars and above the x-axis labels
            ax.text(x_pos, -36, 'A M D', ha='center', va='center', fontsize=12, fontweight='bold', color='black',
                    transform=ax.transData)
            
    # Set labels and title
    ax.set_xticks(ind + width)
    ax.set_xticklabels([m.strftime('%b %Y') for m in months], rotation=45, ha="right")
    ax.set_ylabel('Added, modified and deleted rules')
    ax.tick_params(axis='x', pad=15)  # Increase padding between tick labels and axis
    #ax.set_title('Commit Types per Month (Stacked by Repository)')

    # Create legends
    # Repository legend (colors)
    repo_legend = ax.legend(handles=repo_handles, title='Repository',ncol=4, bbox_to_anchor=(0.215, 1.015), loc='upper left', fontsize=15)
    # Commit type legend (hatch patterns)
    commit_type_legend = ax.legend(handles=commit_type_handles, title='Commit Type',bbox_to_anchor=(0.08, 1.015), loc='upper center', fontsize=15) #,
    # Add the repository legend back
    ax.add_artist(repo_legend)

    plt.tight_layout()
    plt.show()


def create_stacked_ba2r(df):
    plt.rcParams.update({'font.size': 16})

    # Apply the 'map_names' function to 'repository' column
    df['repository'] = df['repository'].apply(map_names)

    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure 'additions' and 'deletions' are numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce').fillna(0)
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce').fillna(0)

    # Filter data from 2022 onwards to 4.2024
    df = df[(df['timestamp'].dt.year >= 2022) & (df['timestamp'] <= "2024-04-01")]

    # Extract month and year for grouping
    df['month_year'] = df['timestamp'].dt.to_period('M').dt.to_timestamp()

    # Filter only 'added', 'modified', 'deleted' commit types (case-insensitive)
    df['commit_type'] = df['commit_type'].str.lower()
    df_filtered = df[df['commit_type'].isin(['added', 'modified', 'deleted'])]

    # Check if df_filtered is empty
    if df_filtered.empty:
        print("No data available for the specified commit types and date range.")
        return

    # Group by 'month_year', 'commit_type', 'repository' and count the occurrences
    counts_df = df_filtered.groupby(['month_year', 'commit_type', 'repository']).size().reset_index(name='count')

    # Get the unique months, commit types, and repositories
    months = sorted(counts_df['month_year'].unique())
    commit_types = ['added', 'modified', 'deleted']  # The order as per your request
    repositories = sorted(counts_df['repository'].unique())

    N = len(months)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.25  # the width of the bars

    fig, ax = plt.subplots(figsize=(14, 8))

    # Colors for the repositories (cycling through colors)
    colors = plt.cm.get_cmap('tab20', len(repositories)).colors

    # Hatch patterns for commit types
    hatch_patterns = {
        'added': '/',
        'modified': 'x',
        'deleted': '\\'
    }

    # Initialize lists for legends
    repo_handles = []
    repo_labels = []
    commit_type_handles = []
    commit_type_labels = []

    for i, commit_type in enumerate(commit_types):
        # Initialize bottom to zero for stacking
        bottoms = np.zeros(N)
        # Get data for the specific commit_type
        dfs = counts_df[counts_df['commit_type'] == commit_type]
        # Pivot to get repositories as columns
        dfs = dfs.pivot(index='month_year', columns='repository', values='count').fillna(0)
        # Reindex to ensure all months and repositories are included
        dfs = dfs.reindex(index=months, columns=repositories, fill_value=0)
        for j, repository in enumerate(repositories):
            counts = dfs[repository].values.astype(float)
            # Plot the bar with hatch patterns and edge colors
            bars = ax.bar(ind + i * width, counts, width, bottom=bottoms,
                          color=colors[j % len(colors)],
                          hatch=hatch_patterns[commit_type],
                          edgecolor='black')

            # Update the bottom for stacking
            bottoms += counts

            # Add repository to legend once
            if i == 0:
                repo_handles.append(Patch(facecolor=colors[j % len(colors)], label=repository))
                repo_labels.append(repository)
        # Add commit type to legend once
        commit_type_handles.append(Patch(facecolor='white', edgecolor='black',
                                         hatch=hatch_patterns[commit_type], label=commit_type))
        commit_type_labels.append(commit_type)

    # Set labels and title
    ax.set_xticks(ind + width)
    ax.set_xticklabels([m.strftime('%b %Y') for m in months], rotation=45, ha="right")
    ax.set_xlabel('Months per year')
    ax.set_ylabel('Rules added or removed')
    #ax.set_title('Commit Types per Month (Stacked by Repository)')

    # Create legends
    # Repository legend (colors)
    repo_legend = ax.legend(handles=repo_handles, title='Repository',ncol=3, bbox_to_anchor=(0.585, 1), loc='upper left', fontsize=10)
    # Commit type legend (hatch patterns)
    commit_type_legend = ax.legend(handles=commit_type_handles, title='Commit Type',bbox_to_anchor=(0.45, 1), loc='upper center', fontsize=10) #,
    # Add the repository legend back
    ax.add_artist(repo_legend)

    plt.tight_layout()
    plt.show()

    plt.savefig("C:/tmp/pow/git_commit_analysis_types.pdf", format='pdf')
    
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.patches import Patch


def create_stacked_bar_subplot_for_each_filterlist(df):
    plt.rcParams.update({'font.size': 14})

    # Apply the 'map_names' function to 'repository' column
    df['repository'] = df['repository'].apply(map_names)

    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure 'additions' and 'deletions' are numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce').fillna(0)
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce').fillna(0)

    # Filter data from 2022 onwards to 4.2024
    df = df[(df['timestamp'].dt.year >= 2022) & (df['timestamp'] <= "2024-04-01")]

    # Extract month and year for grouping
    df['month_year'] = df['timestamp'].dt.to_period('M').dt.to_timestamp()

    # Filter only 'added', 'modified', 'deleted' commit types (case-insensitive)
    df['commit_type'] = df['commit_type'].str.lower()
    df_filtered = df[df['commit_type'].isin(['added', 'modified', 'deleted'])]

    # Check if df_filtered is empty
    if df_filtered.empty:
        print("No data available for the specified commit types and date range.")
        return

    # Group by 'month_year', 'commit_type', 'repository' and count the occurrences
    counts_df = df_filtered.groupby(['month_year', 'commit_type', 'repository']).size().reset_index(name='count')

    # Get the unique months, commit types, and repositories
    months = sorted(counts_df['month_year'].unique())
    commit_types = ['added', 'modified', 'deleted']  # The order as per your request
    repositories = sorted(counts_df['repository'].unique())

    N = len(months)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.6  # Slightly wider bars

    # Create subplots
    num_repositories = len(repositories)
    cols = 5  # Number of columns in the subplot grid
    rows = -(-num_repositories // cols)  # Calculate rows (ceil division)

    fig, axes = plt.subplots(rows, cols, figsize=(22, 3.5 * rows), constrained_layout=True)
    axes = axes.flatten()  # Flatten axes for easy iteration

    # Colors for commit types
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # Distinct colors for added, modified, deleted

    # Calculate standardized y-axis range
    max_count = 15000 #counts_df['count'].max()
    y_max = (max_count + 10) // 10 * 10  # Round up to the nearest 10

    # Iterate over repositories and create a subplot for each
    for idx, repository in enumerate(repositories):
        ax = axes[idx]
        repo_data = counts_df[counts_df['repository'] == repository]

        # Pivot to get data for plotting
        dfs = repo_data.pivot(index='month_year', columns='repository', values=['additions', 'deletions']).fillna(0)
        dfs = dfs.reindex(index=months, fill_value=0)
        bottoms = np.zeros(N)
        for i, commit_type in enumerate(commit_types):
            counts = dfs[commit_type].values.astype(float)
            ax.bar(ind, counts, width, bottom=bottoms,
                   color=colors[i],
                   edgecolor='black',
                   label=commit_type if idx == 0 else "")

            bottoms += counts

        # Adjust x-axis labels to show every second label
        ax.set_xticks(ind)
        ax.set_xticklabels([m.strftime('%m/%Y') if i % 2 == 0 else '' for i, m in enumerate(months)], rotation=45, ha="right", fontsize=12)
        # ax.set_xlabel('Months', fontsize=12)
        ax.set_ylabel('Count', fontsize=12)
        ax.set_title(repository, fontsize=14)
        ax.set_ylim(0, y_max)

        if idx == 0:  # Add legend only once
            ax.legend(title="Commit Type", loc='upper right', fontsize=12)

    # Remove unused subplots
    for idx in range(num_repositories, len(axes)):
        fig.delaxes(axes[idx])

    plt.savefig("C:/tmp/pow/all_filterlists_subplots_adjusted.pdf", format='pdf')
    plt.show()
    
def create_stacked_bar_subplot_adds_removals(df):
    plt.rcParams.update({'font.size': 16})

    # Apply the 'map_names' function to 'repository' column
    df['repository'] = df['repository'].apply(map_names)

    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure 'additions' and 'deletions' are numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce').fillna(0)
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce').fillna(0)

    # Filter data from 2022 onwards to 4.2024
    df = df[(df['timestamp'].dt.year >= 2022) & (df['timestamp'] <= "2024-04-01")]

    # Extract month and year for grouping
    df['month_year'] = df['timestamp'].dt.to_period('M').dt.to_timestamp()

    # Group by 'month_year' and 'repository' and sum the additions and deletions
    counts_df = df.groupby(['month_year', 'repository']).agg({'additions': 'sum', 'deletions': 'sum'}).reset_index()

    # Get the unique months and repositories
    months = sorted(counts_df['month_year'].unique())
    repositories = sorted(counts_df['repository'].unique())

    N = len(months)
    ind = np.arange(N)  # the x locations for the groups
    width = 0.7  # Wider bars

    # Create subplots
    num_repositories = len(repositories)
    cols = 5  # Number of columns in the subplot grid
    rows = -(-num_repositories // cols)  # Calculate rows (ceil division)

    fig, axes = plt.subplots(rows, cols, figsize=(22, 5 * rows), constrained_layout=True)
    axes = axes.flatten()  # Flatten axes for easy iteration

    # Colors for additions and deletions
    colors = ['#1f77b4', '#ff7f0e']  # Distinct colors for additions and deletions

    # Iterate over repositories and create a subplot for each
    for idx, repository in enumerate(repositories):
        ax = axes[idx]
        repo_data = counts_df[counts_df['repository'] == repository]

        # Prepare data for plotting
        additions = repo_data.set_index('month_year').reindex(months)['additions'].fillna(0).values
        deletions = repo_data.set_index('month_year').reindex(months)['deletions'].fillna(0).values

        # Plot additions and deletions
        ax.bar(ind - width / 2, additions, width, color=colors[0], edgecolor='black')
        ax.bar(ind + width / 2, deletions, width, color=colors[1], edgecolor='black')

        # Adjust x-axis labels to show every second label
        ax.set_xticks(ind)
        ax.set_xticklabels([m.strftime('%m/%Y') if i % 2 == 0 else '' for i, m in enumerate(months)], rotation=45, ha="right", fontsize=11)
        # ax.set_ylabel('Count', fontsize=16)
        
        if idx // cols == 2 and idx % cols == 0:  # Only for the first subplot in the 3rd row
            ax.set_ylabel('Number of Added and Removed Rules', fontsize=16)
            
        ax.set_title(repository, fontsize=16)
        ax.set_ylim(0, max(additions.max(), deletions.max()) * 1.1)  # Dynamically set y-axis limit

    # Remove unused subplots
    for idx in range(num_repositories, len(axes)):
        fig.delaxes(axes[idx])

    # Add a single legend for the entire figure
    legend_elements = [
        Patch(facecolor=colors[0], edgecolor='black', label='Additions'),
        Patch(facecolor=colors[1], edgecolor='black', label='Deletions')
    ]
    fig.legend(handles=legend_elements, title="Change Type", loc='lower center', bbox_to_anchor=(0.8, 0.1), ncol=2, fontsize=16)

    plt.savefig("C:/tmp/pow/all_filterlists_subplots_adjusted.pdf", format='pdf', bbox_inches='tight')
    plt.show()






def create_stacked_bar_charts(df):
    plt.rcParams.update({'font.size': 16})

    df['repository'] = df['repository'].apply(map_names)

    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Ensure 'additions' and 'deletions' are numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce').fillna(0)
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce').fillna(0)

    # Filter data from 2022 onwards to 4.2024
    df = df[(df['timestamp'].dt.year >= 2022) & (df['timestamp'] <= "2024-04-01")]

    # Extract month and year for grouping
    df['month_year'] = df['timestamp'].dt.to_period('M').dt.to_timestamp()

    #print(df.columns)
    #exit()

    # Group data by month and repository, summing additions and deletions
    grouped = df.groupby(['month_year', 'repository', 'commit_type']).agg({
        'additions': 'sum',
        'deletions': 'sum'
    }).reset_index()

    # Pivot the data to have repositories as columns
    additions_pivot = grouped.pivot(index='month_year', columns='repository', values='additions').fillna(0)
    deletions_pivot = grouped.pivot(index='month_year', columns='repository', values='deletions').fillna(0)

    # Ensure the indices are sorted
    additions_pivot = additions_pivot.sort_index()
    deletions_pivot = deletions_pivot.sort_index()

    # Prepare data for plotting
    months = additions_pivot.index
    num_months = len(months)
    ind = np.arange(num_months)  # the x locations for the groups
    width = 0.4  # the width of the bars

    # Define colors for repositories
    colors = plt.cm.tab20.colors
    repo_colors = {repo: colors[i % len(colors)] for i, repo in enumerate(additions_pivot.columns)}

    # Plot additions and deletions side by side
    fig, ax = plt.subplots(figsize=(15, 7))

    # Stack additions per repository
    bottom_additions = np.zeros(num_months)
    for i, repo in enumerate(additions_pivot.columns):
        values = additions_pivot[repo].values
        ax.bar(ind - width / 2, values, width, bottom=bottom_additions, color=repo_colors[repo])
        bottom_additions += values

    # Stack deletions per repository
    bottom_deletions = np.zeros(num_months)
    for i, repo in enumerate(deletions_pivot.columns):
        values = deletions_pivot[repo].values
        ax.bar(ind + width / 2, values, width, bottom=bottom_deletions, color=repo_colors[repo], hatch='//')
        bottom_deletions += values

    # Formatting the plot
    #ax.set_title('Monthly Additions and Removed rules per Repository (since 2022)')
    ax.set_xlabel('Months per year')
    ax.set_ylabel('Rules added or removed')
    ax.set_xticks(ind)
    ax.set_xticklabels([dt.strftime('%Y-%m') for dt in months], rotation=45)
    plt.tight_layout()

    # Legend
    legend_handles = [Patch(facecolor=repo_colors[repo], label=repo) for repo in additions_pivot.columns]
    first_legend = ax.legend(handles=legend_handles, title='Repositories', ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1), fontsize=10)

    # add legend
    ax.add_artist(first_legend)

    # Legend
    addition_patch = Patch(facecolor='white', edgecolor='black', label='Additions')
    deletion_patch = Patch(facecolor='white', edgecolor='black', hatch='//', label='Deletions')
    second_legend = ax.legend(handles=[addition_patch, deletion_patch], title='Types')

    plt.show()

    #plt.savefig(os.getcwd()+'/plots/git_commit_analysis.pdf', format='pdf')


def statistical_analysis(df):
    commit_count = list()
    addition_count = list()
    deletion_count = list()
    rules_added = list()
    rules_removed = list()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df[df['timestamp'] <= pd.to_datetime('2024-04-16')]

    df['date'] = df['timestamp'].dt.strftime('%Y-%m')

    dates = list(set(df['date'].tolist()))
    grouped_df = df.groupby('date')

    for date in dates:
        df_date = grouped_df.get_group(date)

        addition_count.append(len(df_date[(df_date['deletions'].astype(float)==0) & (df_date['additions'].astype(float) > 0)]))
        deletion_count.append(len(df_date[(df_date['additions'].astype(float)==0) & (df_date['deletions'].astype(float) > 0)]))

        commit_count.append(len(df_date))

        adds = df_date['additions'].astype(float).sum()
        dele = df_date['deletions'].astype(float).sum()

        rules_added.append(adds)
        rules_removed.append(dele)


    print(
        f"[gha.2.0] Commits per month mean: {np.mean(commit_count)}, min: {np.min(commit_count)}, max: {np.max(commit_count)}, SD: {np.std(commit_count)}")
    print(
        f"[gha.2.1] Commits with only adds per month mean: {np.mean(addition_count)}, min: {np.min(addition_count)}, max: {np.max(addition_count)}, SD: {np.std(addition_count)}")
    print(
        f"[gha.2.2] Commits with only removes per month mean: {np.mean(deletion_count)}, min: {np.min(deletion_count)}, max: {np.max(deletion_count)}, SD: {np.std(deletion_count)}")
    print(
        f"[gha.2.3] Rules added per month mean: {np.mean(rules_added)}, min: {np.min(rules_added)}, max: {np.max(rules_added)}, SD: {np.std(rules_added)}")
    print(
        f"[gha.2.4] Rules removed per month mean: {np.mean(rules_removed)}, min: {np.min(rules_removed)}, max: {np.max(rules_removed)}, SD: {np.std(rules_removed)}")

def statistic_tracker_commits(df):
    df['repository'] = df['repository'].apply(map_names)
    grouped_df = df.groupby('repository')
    df_tracker = pd.read_csv(os.path.join(os.getcwd(), 'data', 'general_overview.csv'))

    list_of_blocking_lists = ["blocked_by_China", "blocked_by_France", "blocked_by_Germany", "blocked_by_India", "blocked_by_Israel", "blocked_by_Japan"
                              , "blocked_by_Scandinavia", "blocked_by_USA", "blocked_by_VAE"]

    for blocking_list in list_of_blocking_lists:
        avg_blocked_requests = df_tracker[blocking_list].astype(float).mean()
        blocking_list_name = blocking_list.replace('blocked_by_', '')

        commits_for_blocking_list = grouped_df.get_group(blocking_list_name)
        addition_commits = commits_for_blocking_list[commits_for_blocking_list['additions'].astype(float)>1]['additions'].astype(float).sum()

        corr_coeff, p_value = spearmanr(avg_blocked_requests, addition_commits)

        print(f"Spearman Correlation Coefficient: {corr_coeff}")
        print(f"P-value: {p_value}")


import gzip
import shutil
from glob import glob
from tqdm import tqdm


def decompress_gzip(input_gzip_file, output_file):
    """
    Decompresses a .gz file and writes the decompressed content to a new file.

    :param input_gzip_file: str, path to the input .gz file
    :param output_file: str, path to the output decompressed file
    """
    with gzip.open(input_gzip_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)  # Copy the content of the gzipped file to the output

    print(f"File '{input_gzip_file}' has been decompressed and saved as '{output_file}'")

def community_statistic(df_commits):
    folder_path = 'D:/a_archiv/tmp/a/'
    files = glob(folder_path + '/*.csv')

    df_list = [pd.read_csv(file, encoding='utf8') for file in tqdm(files, desc='Read files', total=len(files))]
    df_urls = pd.concat(df_list, ignore_index=True)

    df_commits['filter_list'] = df_commits['repository'].apply(map_names)

    # Prepare data commits - count the commit types for each filter list
    commit_counts = df_commits.pivot_table(index='filter_list', columns='commit_type',
                                           aggfunc='size', fill_value=0).reset_index()
    commit_counts.columns.name = None

    commit_counts['total_commits'] = commit_counts['Added'] + commit_counts['Deleted'] + commit_counts['Modified']

    author_count = df_commits.pivot_table(index='filter_list', columns='author', aggfunc='size', fill_value=0).reset_index()
    author_count.columns.name = None
    author_list = list(set(df_commits['author'].tolist()))

    author_count['total_authors'] = 0
    for author in author_list:
        author_count['total_authors'] += author_count[author]

    # Prepare data urls - count on how many filterlists a URL (request) has been blocked
    block_counts = df_urls.iloc[:, 1:].sum().reset_index()
    block_counts.columns = ['filter_list', 'block_count']
    # block_counts['filter_list'] = block_counts['filter_list'].str.extract('(\d+)').astype(int)
    block_counts['filter_list'] = block_counts['filter_list'].apply(map_names_fl)

    # Merge prepared data with commits
    df_merged = pd.merge(block_counts, author_count, on='filter_list', how='left')

    # should not effect the list ...
    df_merged = df_merged.fillna(0)

    #lists = list(set(df_merged['filter_list'].tolist()))
    #df_grouped_by_fl = df_merged.groupby('filter_list')

    corr, p_value = spearmanr(df_merged['block_count'], df_merged['total_authors'])
    print(
            f"\nSpearman-correlation (blocked requests ~ authors on repo): correlation = {corr:.2f}, p-Wert = {p_value:.4f}")




def global_statistic(df_commits):
    folder_path = 'D:/a_archiv/tmp/a/'
    files = glob(folder_path + '/*.csv')

    # files = glob(folder_path + '/*.csv')
    #for i, file in enumerate(files):
     #   decompress_gzip(file, folder_path+str(i)+'_requests.csv')


    df_list = [pd.read_csv(file, encoding='utf8') for file in tqdm(files, desc='Read files', total=len(files))]
    df_urls = pd.concat(df_list, ignore_index=True)

    df['filter_list'] = df['repository'].apply(map_names)

    # Prepare data commits - count the commit types for each filter list
    commit_counts = df_commits.pivot_table(index='filter_list', columns='commit_type',
                                           aggfunc='size', fill_value=0).reset_index()
    commit_counts.columns.name = None

    commit_counts['total_commits'] = commit_counts['Added'] + commit_counts['Deleted'] + commit_counts['Modified']

    # Prepare data urls - count on how many filterlists a URL (request) has been blocked
    block_counts = df_urls.iloc[:, 1:].sum().reset_index()
    block_counts.columns = ['filter_list', 'block_count']
    #block_counts['filter_list'] = block_counts['filter_list'].str.extract('(\d+)').astype(int)
    block_counts['filter_list'] = block_counts['filter_list'].apply(map_names_fl)

    # Merge prepared data with commits
    df_merged = pd.merge(block_counts, commit_counts, on='filter_list', how='left')

    # should not effect the list ...
    df_merged = df_merged.fillna(0)

    # Correlation between blocked requests and commit types with spearman
    correlations = {}
    for col in ['Added', 'Deleted', 'Modified']:
        corr, p_value = spearmanr(df_merged['block_count'], df_merged[col])
        correlations[col] = (corr, p_value)

    print("\nSpearman-correlation (blocked requests ~ Commit types):")
    for key, (corr, p_value) in correlations.items():
        print(f"{key}: Correlation = {corr:.2f}, p-value = {p_value:.4f}")

    corr, p_value = spearmanr(df_merged['block_count'], df_merged['total_commits'])
    print(f"\nSpearman-correlation (blocked requests vs. total commits): correlation = {corr:.2f}, p-Wert = {p_value:.4f}")

    # Poisson regression to show correlations
    poisson_model_combined = smf.poisson('block_count ~ total_commits', data=df_merged).fit()


    print("\nPoisson regression results:")
    print(poisson_model_combined.summary())

    # Visualization

def commits_on_repos(df):
    df['repository'] = df['repository'].apply(map_names)
    repos = list(set(df['repository'].tolist()))
    grouped_df = df.groupby('repository')

    d = dict()
    d_commits = dict()
    d_authors = dict()

    for repo in repos:
        df_repo = grouped_df.get_group(repo)
        additions = df_repo['additions'].astype(int).tolist()
        deletions = df_repo['deletions'].astype(int).tolist()
        commits = len(df_repo)

        d_commits[repo] = commits

        d[(repo, commits)] = np.sum(additions) + np.sum(deletions)

        if repo != 'USA':
            authors = list(set(df_repo['author'].tolist()))
            d_authors[repo] = len(authors)

    max_rules = max(d, key=d.get)
    min_rules = min(d, key=d.get)
    print(f"[gha.4.0] max: {max_rules[0]} ({max_rules[1]} commits) {d[max_rules]} rules added/removed")
    print(f"[gha.4.1] min: {min_rules[0]} ({min_rules[1]} commits) {d[min_rules]} rules added/removed")

    max_authors = max(d_authors, key=d_authors.get)
    min_authors = min(d_authors, key=d_authors.get)
    total_authors = list(d_authors.values())

    print(f"[gha.4.2] Authors on Github: Mean: {np.mean(total_authors)}, min: {np.min(total_authors)}, max:{np.max(total_authors)}, SD: {np.std(total_authors)}")
    print(f"[gha.4.3] Max authors in {max_authors} ({d_authors[max_authors]} authors and {d_commits[max_authors]} commits) and min authors in {min_authors} ({d_authors[min_authors]} authors and {d_commits[min_authors]} commits))")


def repos_in_measurement_time(df):
    df['repository'] = df['repository'].apply(map_names)
    #lists_in_analysis = ['USA', 'Germany', 'France', 'Scandinavia', 'China', 'Israel', 'India', 'VAE', 'Japan']
    lists_in_analysis = list(set(df['repository'].tolist()))

    grouped_df = df.groupby('repository')

    commits_per_repo_during_study = dict()

    for l in lists_in_analysis:
        df_of_list = grouped_df.get_group(l)
        df_of_list = df_of_list[(pd.to_datetime(df_of_list['timestamp']) <=  pd.to_datetime('2024-04-16'))
                                & (pd.to_datetime(df_of_list['timestamp']) >=  pd.to_datetime('2022-01-01'))] # reduce to our measurement

        added_rules = df_of_list['additions'].astype(int).tolist()
        deleted_rules = df_of_list['deletions'].astype(int).tolist()

        #print(l, len(df_of_list.values), 'commits', np.sum(added_rules), 'rules added and', np.sum(deleted_rules), 'rules removed')

        commits_per_repo_during_study[l] = len(df_of_list)

    print("[gha.1.0] mean: ",np.mean(list(commits_per_repo_during_study.values())), 'min:',np.min(list(commits_per_repo_during_study.values())),
          'max:', np.max(list(commits_per_repo_during_study.values())), 'SD:', np.std(list(commits_per_repo_during_study.values())))

    max_r = max(commits_per_repo_during_study, key=commits_per_repo_during_study.get)
    min_r = min(commits_per_repo_during_study, key=commits_per_repo_during_study.get)
    print("[gha.1.1] max commits:", max_r, '(', commits_per_repo_during_study[max_r] ,')' ,'min commits:', min_r, '(', commits_per_repo_during_study[min_r] ,')')
    print("[gha.1.2] Germany:",commits_per_repo_during_study['Germany'], 'Scandinavia:',commits_per_repo_during_study['Scandinavia'] )

def different_commit_types(df):
    commit_types = list(set(df['commit_type'].tolist()))
    grouped_df = df.groupby('commit_type')

    total_commits = len(df)

    for commit_type in commit_types:
        _df = grouped_df.get_group(commit_type)
        print("[gha.5.0]",commit_type, ':', len(_df), '(', len(_df)/total_commits * 100 , '%)')

def get_commit_type(message):
    message = message.strip()
    if message.startswith('A:'):
        return 'Added'
    elif message.startswith('D:'):
        return 'Deleted'
    elif message.startswith('M:'):
        return 'Modified'
    elif message.startswith('Auto build'):
        return 'Auto build update'
    else:
        return 'Unknown'


def analyse_on_commit_type(df):
    df['repository'] = df['repository'].apply(map_names)
    repos = list(set(df['repository'].tolist()))
    grouped_df_by_repo = df.groupby('repository')

    mod = list()
    add = list()
    rem = list()
    uk = list()

    for repo in repos:
        _df = grouped_df_by_repo.get_group(repo)

        commit_types = list(set(_df['commit_type'].tolist()))
        grouped_df = _df.groupby('commit_type')

        for commit_type in commit_types:
            c_df = grouped_df.get_group(commit_type)
            #print(repo, commit_type, ':', len(c_df))

            if commit_type == 'Added':
                add.append(len(c_df))
            elif commit_type == 'Deleted':
                rem.append(len(c_df))
            elif commit_type == 'Modified':
                mod.append(len(c_df))
            elif commit_type == 'Unknown':
                uk.append(len(c_df))

    print(f"[gha.3.0] Additions mean: {np.mean(add)}, min: {np.min(add)}, max: {np.max(add)}, SD: {np.std(add)}")
    print(f"[gha.3.1] Removes mean: {np.mean(rem)}, min: {np.min(rem)}, max: {np.max(rem)}, SD: {np.std(rem)}")
    print(f"[gha.3.2] Modified mean: {np.mean(mod)}, min: {np.min(mod)}, max: {np.max(mod)}, SD: {np.std(mod)}")
    print(f"[gha.3.3] Unknown mean: {np.mean(uk)}, min: {np.min(uk)}, max: {np.max(uk)}, SD: {np.std(uk)}")


if __name__ == "__main__":
    path = os.path.join(os.getcwd(), '..', '..', '02_Measurement_Data' ,"filterlisten - githubs/")

    dir = os.listdir(path)

    folders = [d for d in dir if os.path.isdir(os.path.join(path, d))]

    d = list()
    for folder in folders:
        output_file_path = os.path.join(path, folder, 'output_all.txt')
        with open(output_file_path, 'r', encoding='utf8') as f:
            data = f.read()

        # Split the text
        commit_blocks = split_on_commit(data)

        for i, block in enumerate(commit_blocks, 1):

            try:
                commit, date, message,file, additions, deletions, author = block.splitlines()
            except:
                print(block)
                exit()

            data_row = {
                'repository': folder,
                'file': file.replace('file changed:', ''),
                'commit_sha': commit.replace('commit:', ''),
                'message': message.replace('message:', ''),
                'commit_type': get_commit_type(message.replace('message:', '')),
                'timestamp': date.replace('date:', ''),
                'author': author.replace('author:', ''),
                'additions': additions.replace('number_of_adds:', ''),
                'deletions': deletions.replace('number_of_removes:', '')
            }

            d.append(data_row)


    df = pd.DataFrame.from_records(d)
    #print(df)

    print("Statistical analysis")
    #statistical_analysis(df)

    print("#------------------------------------------------------------------------#")

    # we are here
    print("Plot the bar chart ...")
    #create_stacked_bar_charts(df)
    create_stacked_bar(df)
    # create_stacked_bar_subplot_adds_removals(df)

    print("#------------------------------------------------------------------------#")

    print('Statistics on blocked requests and commits')
    #statistic_tracker_commits(df)

    print("#------------------------------------------------------------------------#")

    print('Statistic over global blocked requests and commits')
    #global_statistic(df)

    print("#------------------------------------------------------------------------#")

    print('Statistics over the communities')
    #community_statistic(df)

    print("#------------------------------------------------------------------------#")

    print("Commits and authors on repos")

    #commits_on_repos(df)

    print("#------------------------------------------------------------------------#")

    print("Commits, added and removed rules from begin of the list till end of measurement:")
    #repos_in_measurement_time(df)

    print("#------------------------------------------------------------------------#")

    print("Commits on different commit types (A,D,M)")
    #different_commit_types(df)

    print("#------------------------------------------------------------------------#")

    print("Analyse git commits by comment")
    #analyse_on_commit_type(df)

    print("#------------------------------------------------------------------------#")
