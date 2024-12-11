from datetime import datetime, timedelta, timezone
import os

path = "D:/filterlisten - githubs/"

dir = os.listdir(path)

folders = [d for d in dir if os.path.isdir(os.path.join(path, d))]

repos_filenames = {
    "indonesianadblockrules": "abpindo.txt",
    "abpvn": "abpvn.txt",
    "adblockbg": "adblockbg.txt",
    "adfilt": "NordicFiltersABP-Inclusion.txt",
    "easylistchina": "easylistchina.txt",
    "easylistczechandslovak": "filters.txt",
    "easylistdutch": "easylistdutch.txt",
    "easylistgermany": "easylistgermany_general_block.txt",
    "EasyListHebrew": "EasyListHebrew.txt",
    "easylistitaly": "easylistitaly_general_block.txt",
    "easylist_lithuania": "easylistlithuania.txt",
    "easylistpolish": "easylistpolish_general_block.txt",
    "easylistportuguese": "easylistportuguese_general_block.txt",
    "easylistspanish": "easylistspanish_general_block.txt",
    "IndianList": "general_block.txt",
    "KoreanList": "koreanlist_general_block.txt",
    "adblock-latvian": "latvian-list.txt",
    "listear": "Liste_AR.txt",
    "listefr": "liste_fr.txt",
    "ruadlist": "advblock.txt",
    "antiadblockfilters": "antiadblock_arabic.txt, antiadblock_chinese.txt, antiadblock_czech.txt, antiadblock_dutch.txt, antiadblock_english.txt, antiadblock_finnish.txt, antiadblock_french.txt, antiadblock_german.txt, antiadblock_hebrew.txt, antiadblock_indonesian.txt, antiadblock_italian.txt, antiadblock_latvian.txt, antiadblock_polish.txt, antiadblock_romanian.txt, antiadblock_russian.txt, antiadblock_slovak.txt, antiadblock_spanish.txt",
    "easylist": "easytest.txt"
}

def split_on_commit(text):
    # Split the text on the word "commit" and keep the "commit" in each block
    blocks = text.split("commit")

    # Reattach the "commit" to the beginning of each block (skip the first split part if it's empty)
    blocks = [("commit" + block).strip() for block in blocks if block.strip()]

    return blocks


# Example usage
if __name__ == "__main__":
    for folder in folders:
        with open(os.path.join(path, folder, 'all_changes.txt'), 'r', encoding='utf8') as f:
            all_changes = f.read()

        target_files = repos_filenames[folder].split(',')

        # Split the text
        commit_blocks = split_on_commit(all_changes)

        # Print the resulting blocks
        for i, block in enumerate(commit_blocks, 1):
            additions = 0
            deletions = 0
            timestamp = ""
            sha = ""
            for line in block.splitlines():
                if line.startswith('commit'):
                    sha = line.replace('commit', '').strip()
                if line.startswith('Date:'):
                    date_string = line.replace('Date:', '').strip()
                    timestamp = datetime.strptime(date_string[:-6], '%a %b %d %H:%M:%S %Y')
                if line.startswith('+'):
                    additions += 1
                elif line.startswith('-'):
                    deletions += 1

            data_row = {
                'repository': folder,
                'file': target_files,
                'commit_sha': sha,
                'timestamp': timestamp,
                'additions': additions,
                'deletions': deletions
            }

            print(data_row)

        




