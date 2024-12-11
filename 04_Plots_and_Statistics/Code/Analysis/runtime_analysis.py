import pandas as pd
import json
import os
from glob import glob
import statistics
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import seaborn as sb


def get_data():
    path = "Todo: Add path to runtime analysis data"
    folder = glob(path+"*results.json")

    data = list()
    for file in folder:
        print("Read from file", file)
        with open(file, 'r', encoding="utf-8") as f:
            d = json.loads(f.read())
            loop = os.path.basename(file).split('_')[0]
            data.append({'Loop': loop, 'data': d})

    print("Read", len(data), "files")

    memory_usage_hosts_system = 1200000


    r1 = {"duration": [], "memory_usage": [], 'blocked_urls': []}
    r2 = {"duration": [], "memory_usage": [], 'blocked_urls': []}
    r3 = {"duration": [], "memory_usage": [], 'blocked_urls': []}
    r4 = {"duration": [], "memory_usage": [], 'blocked_urls': []}
    r5 = {"duration": [], "memory_usage": [], 'blocked_urls': []}
    for d in data:
        loop = d['Loop']
        json_obj = d['data']
        round = json_obj['Rounds']
        duration = json_obj['Duration']
        memory_usage = json_obj['Memory_used']
        blocked_urls = json_obj['Results']


        if round == 1:
            r1['duration'].append(duration)
            r1['memory_usage'].append(memory_usage)
            r1['blocked_urls'].append(len(blocked_urls))
        elif round == 2:
            r2['duration'].append(duration)
            r2['memory_usage'].append(memory_usage)
            r2['blocked_urls'].append(len(blocked_urls))
        elif round == 3:
            r3['duration'].append(duration)
            r3['memory_usage'].append(memory_usage)
            r3['blocked_urls'].append(len(blocked_urls))
        elif round == 4:
            r4['duration'].append(duration)
            r4['memory_usage'].append(memory_usage)
            r4['blocked_urls'].append(len(blocked_urls))
        elif round == 5:
            r5['duration'].append(duration)
            r5['memory_usage'].append(memory_usage)
            r5['blocked_urls'].append(len(blocked_urls))

    print("[Round 1] Mean duration:", statistics.mean(r1['duration']), "mean memory usage:", statistics.mean(r1['memory_usage']), "mean blocked urls:", statistics.mean(r1['blocked_urls']))
    print("[Round 2] Mean duration:", statistics.mean(r2['duration']), "mean memory usage:", statistics.mean(r2['memory_usage']), "mean blocked urls:", statistics.mean(r2['blocked_urls']))
    print("[Round 3] Mean duration:", statistics.mean(r3['duration']), "mean memory usage:", statistics.mean(r3['memory_usage']), "mean blocked urls:", statistics.mean(r3['blocked_urls']))
    print("[Round 4] Mean duration:", statistics.mean(r4['duration']), "mean memory usage:", statistics.mean(r4['memory_usage']), "mean blocked urls:", statistics.mean(r4['blocked_urls']))
    print("[Round 5] Mean duration:", statistics.mean(r5['duration']), "mean memory usage:", statistics.mean(r5['memory_usage']), "mean blocked urls:", statistics.mean(r5['blocked_urls']))

def plot():
    # Data
    used_rules = np.array([28734, 57464, 86194, 114924, 143654])
    runtime = np.array(
        [11.8, 12.3, 13.0, 13.9, 14.9])
    memory_usage = np.array([8088864.5/1000000, 8220191.5/1000000, 8293447.2/1000000, 8347801.8/1000000, 9615349.4/1000000])
    blocked_urls = np.array([276.493006993007/28734*100, 502.3098591549296/57464*100, 700.3286713286714/86194 * 100, 867.7730496453901/114924 * 100, 1019/143654 * 100])

    # Erstellen der Grafik
    fig, ax1 = plt.subplots()

    # Plot adjustments
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42
    matplotlib.rcParams['text.usetex'] = False
    matplotlib.rcParams['axes.labelweight'] = 'bold'
    sb.set(rc={'figure.figsize': (25, 7),"font.size": 18, "axes.titlesize": 18, "axes.labelsize": 18,
               "legend.fontsize": 18, "xtick.labelsize": 18, "ytick.labelsize": 18}, style="white")

    # Plot für die linke y-Achse (Runtime und Memory Usage)
    ax1.plot(used_rules, runtime, 'r-', label='Runtime')
    ax1.plot(used_rules, memory_usage, 'y-', label='Memory usage')
    ax1.set_xlabel('Used rules', weight='bold', fontsize=18)
    ax1.set_ylabel('Memory usage (GB) \n Runtime (seconds)', weight='bold', fontsize=18)
    # ax1.tick_params(axis='y', colors='black', size=14)
    # ax1.tick_params(axis='x', colors='green', size=14)
    # ax1.xaxis.set_major_formatter(tick.FormatStrFormatter('{:,}'))
    ax1.set_yticklabels(ax1.get_yticks(), size=16)
    ax1.set_xticklabels(ax1.get_xticks(), size=16, rotation=45)
    # ax1.locator_params(integer=True)
    # ax1.set_yticks([28734, 57464, 86194, 114924, 143654])
    ax1.legend(loc='upper center', ncol=1, fancybox=False, shadow=False, fontsize=14)

    # Zweite y-Achse teilen (Blocked URLs)
    ax2 = ax1.twinx()
    ax2.plot(used_rules, blocked_urls, 'b-', label='Blocked\nrequests\nper rule')
    ax2.set_ylabel('Blocked requests')
    # ax2.tick_params(axis='y', colors='black',)
    # ax2.tick_params(axis='x', colors='black', rotation=45)
    ax2.set_xticks([25000, 60000, 90000, 115000, 145000])
    # ax2.locator_params(integer=True)
    # plt.xticks(rotation=45)
    ax2.legend(loc='center right', fancybox=False, shadow=False, fontsize=14)

    # Layout anpassen und anzeigen
    fig.tight_layout()
    # plt.show()

    plt.savefig(os.path.join(os.getcwd(), 'plots', "p8_runtime_analysis.pdf"), dpi=600,
                transparent=False, bbox_inches='tight', format="pdf")

if __name__ == '__main__':
    plot()
