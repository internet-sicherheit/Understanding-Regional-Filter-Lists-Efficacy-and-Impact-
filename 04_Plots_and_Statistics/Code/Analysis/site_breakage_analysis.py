import os
import pandas as pd
from glob import glob

path_to_justdomains = os.path.join(os.getcwd(),'..', '..', 'justdomains_analysis', 'justdomains', 'lists')
justdomain_lists = glob(path_to_justdomains+'/*.txt')

blocker = dict()


for justdomain_list in justdomain_lists:
    with open(justdomain_list, 'r', encoding='utf8') as f:
        blocker[justdomain_list] = list(set(f.read().splitlines()))

blocked_rules = list()
for r in list(blocker.values()):
     blocked_rules += r

df_blocked_fps = pd.read_csv('blocked_fp_requests.csv')
df_all_fps = pd.read_csv('all_fps_urls.csv')

fp_in_blocker = 0
for index, row in df_blocked_fps.iterrows():
    etld = row['etld_one']
    if etld in blocked_rules:
        fp_in_blocker += 1

print(f"[sba.1.0] Blocked first party requests in justdomains liste on eTLD+1: {fp_in_blocker}")

fp_in_blocker = 0
for index, row in df_all_fps.iterrows():
    etld = row['etld_one']
    if etld in blocked_rules:
        fp_in_blocker += 1

print(f"[sba.1.1] General first party requests in justdomains liste on eTLD+1: {fp_in_blocker}")

