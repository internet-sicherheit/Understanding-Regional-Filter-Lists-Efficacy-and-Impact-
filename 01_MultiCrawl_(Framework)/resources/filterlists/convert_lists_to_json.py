from glob import glob
import os
import json

accepted_files = ['ABPVN_List.txt', 'ABPindo.txt', 'Bulgarian_List.txt', 'KoreanList.txt', 'EasyList_Italy.txt',
                  'EasyList_Polish.txt', 'EasyList_Portuguese.txt', 'EasyList_Spanish.txt', 'EasyList_Dutch.txt', 'RU_AdList.txt',
                  'latvian-list.txt', 'easylistlithuania.txt', 'easylistczechandslovak.txt']

path = os.path.join(os.getcwd(), '..', '..', 'adblock_test')
path2 = os.path.join(os.getcwd(), '..', '..', 'Code', 'Filterlist', 'filter_lists')

files = glob(path + '/*txt')
files2 = set(os.listdir(path2))
files += [path2+'/'+file for file in files2 if os.path.basename(file) in accepted_files]

lists = list()
for file in files:
    filename = os.path.basename(file.replace('.txt', ''))

    cleaned_file = file.replace('\\', '/')

    lists.append(
        {
            "name": f"{filename} Filterlist",
            "url": f"file://{cleaned_file}",
            "format": "adbp"
        }
    )

with open('data.json', 'w', encoding='utf-8') as f:
    json.dump(lists, f, ensure_ascii=False, indent=4)
