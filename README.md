# Protected Designation of Origin - An Analysis of Efficacy in Regional Filterlists
This repository contains supplementary material to the paper "Protected Designation of Origin - An Analysis of Efficacy in Regional Filterlists" submitted to PETS 2025.


This repository contains additional material to the paper "A Large-Scale Study of Cookie Banner Interaction Tools and Their Impact on Users’ Privacy," accepted at the *23rd Privacy Enhancing Technologies Symposium 2024 (PETS)*. 

In this repository, we provide __everything__ we have used in our study: tools, code, raw and processed measurement data, data processing pipeline, and the code for generating plots and statistics to reproduce our study's results.



## Repository Structure

- **01_MultiCrawl_(Framework)**  contains the framework necessary to conduct the large-scale measurement presented in the paper. It can be utilized for extensive Web measurements, including crawling multiple websites simultaneously with various browser configurations (e.g., different user agents, extensions, etc.).
  
- **02_Measurement_Data** hosts the collected measurement data, both raw and processed, available as a xxx GB ZIP archive.
  
- **03_Data_Processing_SQLs** contains the entire data processing and evaluation pipeline, including all the SQL statements. 
  
- **04_Plots_and_Statistics** provides two notebooks for generating plots and calculating statistics, as presented in our paper.


If you want to use our artifact, you can cite our [paper]():

```
@article{,
  title = {},
  author = {},
  journal = {Proceedings on Privacy Enhancing Technologies},
  volume = {2025}, 
  year ={2025},
  series = {PoPETS~'25}, 
  doi = {}
  url= {}
}
```




## Table of Contents
- [Introduction](#introduction)
- [Installation](#installation)
- [Using BigQuery](#using-bigquery)
- [Usage](#usage)
| - [Collectable Data](#collectable-data)
- [Dependencies](#dependencies)
- [MultiCrawl (v0.1.2)](#multicrawl-v012)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
  - [Installation \& Configuration](#installation--configuration)
  - [Running the Framework](#running-the-framework)
  - [Acknowledgements](#acknowledgements)

## Introduction
Main purpose of this project is:
1. Collecting data with MultiCrawl (v0.1.2)
2. pushing the traffic to the database server (e.g., BigQuery)
3. Analyse requests


## Installation 
```
Git clone
cd filterlists-2024-pets
pip install -r requirements.txt
```

## Using BigQuery
For using BigQuery as database, store the key as JSON under

```
filterlists-2024-pets/resources/google_bkp.json
```

## Usage

### Collectable Data
* HTTP Traffic
* Cookies


## Dependencies
* MultiCrawl
* OpenWPM
* Python3
* Rust
* adblock-rust
* BigQuery
* AWS EC2 Instances on different global locations


# MultiCrawl (v0.1.2) 

MultiCrawl is a framework designed for running web measurements with different crawling setups across various machines, enabling near real-time website crawling with browsers like Firefox and Chrome. MultiCrawl also automates interactions with consent banners on websites and recognizes tracking requests. All measurement data is pushed to BigQuery for analysis.

**Supported Browsers**: Chrome, Firefox

**Collectable Data Types**:
- Cookies
- LocalStorage
- Requests
- Responses
- DNS Responses
- Callstacks
- JavaScript calls


## Getting Started

Before diving into the installation process, ensure you have the prerequisites ready:
- PostgreSQL database
- Authentication JSON for Google Cloud API
- Sites to visit (e.g., Tranco list)
- A VM (e.g., Ubuntu 20.04) setup

## Installation & Configuration

1. Initialize your PostgreSQL database using the `/resources/posgres.sql` script.
2. Update the PostgreSQL connection string in the `/DBOps.py` file.
3. Save your Google Cloud API's `authentication JSON` as `google.json` in `/resources` ([Guide](https://cloud.google.com/docs/authentication/getting-started)).
4. Import your list into the `sites` table of PostgreSQL.
5. Use `/Commander_extract_Subpages.py` to extract subpages from your imported list.
6. Prepare your BigQuery dataset with the tables `requests`, `responses`, `cookies`, and `localstorage`. For column definitions, refer to `resources/bigquery.md`.

## Running the Framework

1. Set up an Ubuntu 20.04 VM.
2. Install the required packages from `/req-pip.txt` and `/req-conda.txt`.
3. Execute `install.sh` for OpenWPM installation.
4. Configure a VPN connection on your VM (if needed).
5. Name your VMs according to the `getMode()` function in `/setup.py`.
6. Adjust the crawling preferences in the `getConfig()` function  in `/setup.py`
7. Execute `restart.sh` on every VM to initiate the measurement.

## Acknowledgements

This repository incorporates files from [OpenWPM](https://github.com/openwpm/OpenWPM), utilizing OpenWPM (v0.28) for Firefox operations.
