# Artifact Appendix

Paper title: **Understanding Regional Filter Lists: Efficacy and Impact**

Artifacts HotCRP Id: **#5** 

Requested Badge: Either **Available**, **Functional**

## Description
This artifact provides open access to our study's raw data, detailed data processing and evaluation code, and supplementary materials. Users can reproduce our results using the provided resources, including all figures and statistical analyses.

Our repository is comprehensive and contains different elements:
1. Framework that we used to conduct the web measurement - 01_MultiCrawl_(Framework)
2. Raw data of the web measurement - 02_Measurement_Data
3. Data processing pipeline - 03_Data_Processing_SQLs
4. Plots and statistical analysis - 04_Plots_and_Statistics

The artifacts in terms of code (Python, SQL, R), data as CSV and plots (mainly PDFs) were used for our study's analysis. The repository contains data for (1) the data-gathering process with multicrawl and (2) the analysis with Python and BigQuery.

A BigQuery (Google) account with a token and credits to run the queries is necessary for the analysis. The dataset we used for our study is private right now. Further, to collect the data we used, multiple AWS EC2 instances are needed (we do not guarantee that the approach works on other cloud providers). The cost of the EC2 instances and Google credits is around $10k. That is why we do not claim a reproduction at it.

Running the included queries requires significant resources, such as a Google Cloud Account and computational resources, which incurs costs. The process of downloading, extracting, and uploading the raw data and executing queries requires significant time and resources. We understand that reviewers may need more resources or time for these tasks. To address this, we have provided code for generating all figures and conducting statistical analyses. If these measures are inadequate for a functional badge, we will forgo it. Therefore, this document focuses solely on instructions for generating statistical analyses and plots.

### Security/Privacy Issues and Ethical Concerns (All badges)
We are not aware of any security or privacy issues with our artifact.
Multiple websites will be visited while using MultiCrawl (based on OpenWPM). That can trigger some paid ad services and share user information (e.g., User-Agent) to the website providers.

## Basic Requirements (Only for Functional and Reproduced badges)
The analysis with fewer hardware and software requirements runs on a Ubuntu 20.04 with one CPU core and 4 GB of RAM. For other analyses, higher RAM (>100GB) and CPU cores (>100) are needed (e.g., rule usage). Also, at least 1 TB of storage is required. Further, software like Python 3.x with multiple packages (see requirements.txt) and a Conda environment is required.

### Hardware Requirements
Some servers with a high number of CPU cores are necessary. Those servers can be rented (e.g., AWS). Also, multiple instances distributed worldwide are needed for the measurement itself. These can also be rented.
AWS EC2-instance: Ubuntu20.04, RAM: 32 Gib, CPU: 8 (3.0 GHz)/4 (peak of 3.1 GHz)

Measurement vantage points: on the VMs, each measurement point should run a Ubuntu 20.04 LTS - other versions are not guaranteed to work.
The analysis can be done on a Windows 11 or Ubuntu 24.01/20.04 LTS OS. We used mainly a Windows 11 Laptop with 64GB RAM and 8 CPU cores and a VM with Ubuntu 20.04 LTS, 128GB RAM, and 128 CPU cores.

### Software Requirements
All required third-party components are reported in `01_MultiCrawl_(Framework)/req-pip.txt` the file.


### Estimated Time and Storage Consumption
1. Framework:
Installing and preparing the framework: appx. 15 hours

2. Raw data of the web measurement:
Storage: xxxGB (zipped), after extraction appx. xxxB storage is needed. 
Running queries on the raw data:
Downloading the raw data: appx. 20 hours  
Extracting the raw data: appx. 5 hours 
Uploading the raw data: appx. 20 hours

3. Data processing pipeline:
Reproducing the results: appx. 7 to 10 days (scalable with increased hardware; >500 CPUs and 500GB RAM -> 7 days)

4. Plots and statistical analysis: 
Reproducing the figures: appx. 2 hours
Reproducing the statistical analysis: appx. 3 hours

## Environment 

### Accessibility (All badges)
Our Framework, data processing pipeline, and evaluations are available on GitHub as part of our provided artifact: 

### Set up the environment (Only for Functional and Reproduced badges)
Please follow the instructions in the repository.

#### Collect to visited domains ####
To collect the domains that should be visited in the measurement run for each location, create an account for Google (BigQuery). Use the **chrome-ux-report** database to get the top domains. We used the top 10k domains by etld+1 for each country. Therefore, the SQL query 

```sql
# USA
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_us.202401` 
order by rank
LIMIT 10000;

# China
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_cn.202401` 
order by rank
LIMIT 10000;

# Japanese
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_jp.202401` 
order by rank
LIMIT 10000;

# Indian
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_in.202401` 
order by rank
LIMIT 10000;

# Germany
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_de.202401` 
order by rank
LIMIT 10000;

# Norweagin
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_no.202401` 
order by rank
LIMIT 10000;

# France
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_fr.202401` 
order by rank
LIMIT 10000;

# Israel
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_il.202401` 
order by rank
LIMIT 10000;

# VAE
SELECT
  distinct net.reg_domain(origin) as domain, experimental.popularity.rank
FROM
  `chrome-ux-report.country_ae.202401` 
order by rank
LIMIT 10000;
```
Can be used for each location. The data should be stored as CSV or JSON on the local filesystem. 

After collecting the domains, insert them into a PostgreSQL database for each location. The format in the database should be:

| id  | rank | subpages | scheme | subpages_count | state_scheme | state_subpages | site_state | ready | state_openwpm_native_{country iso2} | in_scope | categorie | under_categorie | timeout | openwpm_native_{country iso2} |
|------|------|----------|--------|----------------|--------------|----------------|------------|-------|------------------------------------|----------|-----------|-----------------|---------|--------------------------------|


#### Set up the measurement points ####
First of all, the user needs to [host an EC2 instance (T3.2xlarge)](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html) for the specific location (e.g., Tokyo). Choose Ubuntu 20.04 LTS as OS for the example and create a key pair.
After creating the EC2, connect via SSH (e.g., Putty under Windows) to the EC2 instance and run the following commands:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install ubuntu-gnome-desktop -y

sudo hostnamectl set-hostname measurement-#location#

sudo apt install xubuntu-desktop -y

sudo apt install ubuntu-gnome-desktop -y

mkdir tmp
cd tmp

# Download xRDP
wget https://www.c-nergy.be/downloads/xRDP/xrdp-installer-1.4.2.zip
unzip xrdp-installer-1.4.2.zip
chmod 777 xrdp-installer-1.4.2.sh
./xrdp-installer-1.4.2.sh

# Change password for user ubuntu
sudo su
passwd ubuntu
exit

# Download conda
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-$(uname)-$(uname -m).sh

sudo apt-get install make -y
sudo apt-get install git -y

mkdir ~/Desktop
cd ~/Desktop
mkdir filterlists
cd filterlists

git clone git@
cd 2024-filterlists

# Configure git
git config --global credential.helper manager-core
git config --global credential.helper 'cache --timeout=3600000'
gsettings set org.gnome.desktop.screensaver lock-delay 3600
gsettings set org.gnome.desktop.screensaver lock-enabled false
gsettings set org.gnome.desktop.screensaver idle-activation-enabled false
git clean -df
git checkout -- .
git pull

chmod -R 777 *

sudo apt install build-essential -y

source ~/.bashrc
./install.sh

# Launch conda env und install requirements for python
conda activate openwpm
pip install -r req-pip.txt
mkdir /home/ubuntu/openwpm
touch /home/ubuntu/openwpm/openwpm.log

# Install Google Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb

sudo dpkg -i google-chrome-stable_current_amd64.deb
```

#### Prepare the measurement ####
In the next step, all subpages have to be extracted. Therefore, use the script 
```bash
python Commander_extract_Subpages.py
```
before running the script, change the **getTable** method to the names of (1) the host names of your AWS EC2 instance and (2) the name of the table on the PostgreSQL database (e.g., host measurement-1 maps on table filterlist_us). To run the script successfully, add/change the credentials of the database on the *DBOps.py* on lines **36** and **74**, where
```python
conString = "user=%username%  password=%password% host=%PostgreSQL Host% port=5432 dbname=2024_filterlists"
`` `
Have to be changed. The script has to be executed on each EC2 instance to capture the subpages of each domain. Also, the script *setup.py* have to be changed on (1) **getMode()**, and if required (2) **getConfig()**.

*Note:* if you want to use a VPN instead of a VM on a specific location, some websites won't answer and have to be excluded in the measurement run.

#### Run the measurement #####
After successfully extracting all subpages, the measurement itself can be initiated. Therefore, the script 
```bash
python QueueManager.py 
```
They have to be executed. The data will be stored in ....

### Testing the Environment (Only for Functional and Reproduced badges)
Not applicable.

## Artifact Evaluation (Only for Functional and Reproduced badges)

### Main Results and Claims
List all your paper's results and claims supported by your submitted artifacts.

#### Main Result 1: Differences in Localized Filter Lists
The results show that fewer rules (0.04%) appear in two lists, and no rules are shared from three or more lists.

#### Main Result 2: Impact of Localized Filter Lists
Our results show different impacts a localized filter list has. We observe a significant variation in the efficiency of filter lists on different locations.

#### Main Result 3: Impact of Commits and Rule Additions
The results show that there is no indication of the effectiveness of a filter list or the number of changes of rules in commits.

#### Main Result 4: Usage of Rules
The results show that only a few rules (6.6%) are used for blocking requests. At lot of regulations are optional.

### Experiments 

#### Experiment 1: Differences in Localized Filter Lists 
We used three different approaches to highlight the difference in localized filter lists.
First, we compare the different filter lists. Therefore, the [script](04_Plots_and_Statistics/Code/Analysis/filter_list_overview.py) can be used.
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python filter_list_overview.py
```
Besides the differences in filter lists, the script above will also compute the similarity.

At least, the analysis uses [JustDomains](https://github.com/justdomains/ci/tree/master) to highlight the differences at the domain level.
In the first step, the filter lists have to be transformed in domain lists with the [JustDomains](https://github.com/justdomains/ci/tree/master)
repository and be stored under the folder [justdomains_analysis](04_Plots_and_Statistics/justdomains_analysis).
In the next step, run the scrip.t 

```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python justdomains_comparison.py
```

#### Experiment 2: General Measurement Overview
The following describes the analysis needed to perform the measurement overview.
Therefore, scripts from the [querie folder](03_Data_Processing_SQLs) are required.

Run the scripts in a SQL environment.

#### Experiment 3: Impact of Localized Lists
The following experiment shows the different impacts of localized lists.

First of all, the effects of different filter lists. Therefore, run
```bash
python Figure 5 + 6 - filterlist_fraction_of_identified_TR.ipynb
```
Create figures 5 and 6 and print a statistical analysis.

Another aspect is the difference in user locations. Therefore, the analysis scripts
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python effect_user_location.py
python statistical_analysis.py
```

At least, the effect on categories can be analyzed using the scripts from the [querie folder](03_Data_Processing_SQLs)
and:
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python categories.py
```

#### Experiment 4: Combined Lists
Here, we analyze the effect of combined lists. First of all, we analyze the combination of a
local filter list and the baseline.

```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python combining_lists.py
```

Also, the script plots Figure 9 of the paper. For a  deeper look at the effects of combining
lists, run:
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python statistical_analysis.py
python effect_combined_lists.py 
```

#### Experiment 5: Impact of Commits and Rule Additions
The experiment analyses the GitHub repositories and creates the plot for Figure 2 in the paper.
Besides the plotting, some statistical analyses are also included.
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python github_commmit_plot.py
```
Please note that under 
```python
path = os.path.join(os.getcwd(), '..', '..', '02_Measurement_Data' ,"filterlisten - githubs/")
```
The data for the analysis have to be stored.

#### Experiment 6: Runtime Analysis
To capture the data for the runtime analysis, use the Rust script
in the [runtime](04_Plots_and_Statistics/runtime) folder. After capturing the data,
a further analysis can be done with 
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python runtime_analysis.py
```

#### Experiment 7: Usage of Rules
To analyze the effect of each rule on all filter lists, use the script:
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python rule_usage.py
```

#### Experiment 8: Potential Site Breakage
To do the analysis of potential site breakage, use the script:
```bash
cd 04_Plots_and_Statistics/Code/Analysis/
python site_breakage_analysis.py
```


## Limitations (Only for Functional and Reproduced badges)
We are aware of the following limitations of our artifacts:
- Instructions regarding running the queries on the raw data are not provided due to the high resource requirements.

## Notes on Reusability (Only for Functional and Reproduced badges)
The dataset can be used for further analysis. Further, the results from the analysis can be used to improve the building of filter lists or filter rules.
