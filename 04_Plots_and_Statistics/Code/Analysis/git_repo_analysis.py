from github import Github
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

import csv
import threading

# GitHub-Token
token = "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"
g = Github(token)

repos_filenames = {
    "ABPindo/indonesianadblockrules/": "abpindo.txt",
    "abpvn/abpvn": "abpvn.txt",
    "RealEnder/adblockbg": "adblockbg.txt",
    "DandelionSprout/adfilt": "NordicFiltersABP-Inclusion.txt",
    "easylist/easylistchina": "easylistchina.txt",
    "tomasko126/easylistczechandslovak": "filters.txt",
    "easylist/easylistdutch": "easylistdutch.txt",
    "easylist/easylistgermany": "easylistgermany_general_block.txt",
    "easylist/EasyListHebrew": "EasyListHebrew.txt",
    "easylist/easylistitaly": "easylistitaly_general_block.txt",
    "EasyList-Lithuania/easylist_lithuania": "easylistlithuania.txt",
    "easylistpolish/easylistpolish": "easylistpolish_general_block.txt",
    "easylist/easylistportuguese": "easylistportuguese_general_block.txt",
    "easylist/easylistspanish": "easylistspanish_general_block.txt",
    "mediumkreation/IndianList": "general_block.txt",
    "easylist/KoreanList": "koreanlist_general_block.txt",
    "Latvian-List/adblock-latvian": "latvian-list.txt",
    "easylist/listear": "Liste_AR.txt",
    "easylist/listefr": "liste_fr.txt",
    "easylist/ruadlist": "advblock.txt",
    "easylist/antiadblockfilters": "antiadblock_arabic.txt, antiadblock_chinese.txt, antiadblock_czech.txt, antiadblock_dutch.txt, antiadblock_english.txt, antiadblock_finnish.txt, antiadblock_french.txt, antiadblock_german.txt, antiadblock_hebrew.txt, antiadblock_indonesian.txt, antiadblock_italian.txt, antiadblock_latvian.txt, antiadblock_polish.txt, antiadblock_romanian.txt, antiadblock_russian.txt, antiadblock_slovak.txt, antiadblock_spanish.txt",
    "easylist/easylist": "easytest.txt"
}

# Define a lock for thread-safe operations
data_lock = threading.Lock()
all_data = []

def process_repo(repo_name, target_files_str, g):
    # Get repo
    repo = g.get_repo(repo_name)
    target_files = target_files_str.split(',')

    # Retrieve all commits
    commits = repo.get_commits()
    total_commits = commits.totalCount  # Get total number of commits for progress bar

    # Analyze each commit
    for commit in tqdm(commits, desc=f'Analyzing commits in {repo_name}', total=total_commits):
        # Get the files modified in this commit
        files = commit.files

        # Check each file in the commit
        for file in files:
            for target_file in target_files:
                if file.filename.endswith(target_file):  # Filter by target file
                    # Update the counters with changes specific to the target file
                    additions = file.additions
                    deletions = file.deletions

                    # Timestamp
                    timestamp = commit.commit.committer.date

                    # Commit
                    sha = commit.sha

                    # Collect data
                    data_row = {
                        'repository': repo_name,
                        'file': file.filename,
                        'commit_sha': sha,
                        'timestamp': timestamp,
                        'additions': additions,
                        'deletions': deletions
                    }

                    # Append data to the shared list in a thread-safe manner
                    with data_lock:
                        all_data.append(data_row)

    print(f"\nCompleted analysis for {repo_name}.")

def store_data_to_csv(filename='output.csv'):
    # Fieldnames for the CSV
    fieldnames = ['repository', 'file', 'commit_sha', 'timestamp', 'additions', 'deletions']

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        # Write each row from all_data
        with data_lock:
            for data_row in all_data:
                writer.writerow(data_row)

    print(f'Data has been written to {filename}')

def analyze_repos(repos, g):
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for repo_name, target_files_str in repos.items():
            future = executor.submit(process_repo, repo_name, target_files_str, g)
            futures.append(future)
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                print(f'An exception occurred: {exc}')
    # After all threads complete, store the data
    store_data_to_csv()


analyze_repos(repos=repos_filenames, g=g)
