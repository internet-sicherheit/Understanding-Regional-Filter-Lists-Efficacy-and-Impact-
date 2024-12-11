# Analysis
In this folder all scripts are provided for the analysis. Please note, that some
analysis can take longer time. In the [code folder](Code) are all analysis scripts in Python
and plots. Also, some data required for the analysis is also contained (e.g., CSV files).

## Queries
In the folder [Queries](Queries) are the sql queries to use in BigQuery.

## Just Domains
The data for the [Just Domains](justdomains_analysis) contains all required input data 
without the git repository.

## AdBlock
The data we used to classify which rule were used to block a request from a 
filter list is in the [AdBlock](adblock_test) folder. Also, there is a Rust script
to perform the classification. The classification needs about 7 to 10 days
based on the machine that is used.

## Filterlists
In the folder [Filterlists](Filterlist) are the 23 lists of our scope. Also, there are some
Python scripts to crawl, download and sanitize these lists.
