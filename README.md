# Swile - Head of data - Case study

## Introduction and background

Swile sales team is composed of 60 salesperson distributed into teams covering different region of France:
* Paris - 7 persons, created early 2018
* Montpellier - 39 persons, created end of 2017
* Region - 14 persons, created early 2018

Those salesperson have the objective to sign deals with companies of different size for them to use digital Titre-Restaurants.

The aim of this GitHub repository, is to create data pipeline that will ingest csv source files into a SQLite database, and then perform queries which be used a basis for a dashboard.

### Datasets

In order to perform an analysis an sales performance, salespeson profiling and cohort analysis, 3 datasets have been shared:

* 2 extracts from Salesforce:
  * `users.csv` which contains all the salesperson with their start date, birth date and the team they belong to.
  * `opportunities` which contains all the opportunities, the outcome of the deal, the client and the salesperson linked to the deal
 * 1 extract from the back-office system:
  * `accounts_with_bookings.csv`, which contains all the orders perform by the client to to-up their accounts.
  
### Opportunities status

Each opportunity can have a different outcome/status:
* `never touched`, if a deal allocated to a salesperson was never looked at
* `under negotiation`, if the salesperson is currently working on the deal
* `signed`, if the deal ended up being signed
* `lost`, if the deal ended up being lost

## Installation

First create a virtual environment:
```shell
virtualenv -v env
```

Then activate the virtual environment:
```
source env/bin/activate
```

And finally, install all python libraries which are required to run the pipeline:
```
pip install -r requirements.txt
```

## Architecture

Our architecture is composed of 3 main components:
* a luigi pipeline;
* a SQLite database; and
* a GoogleSheet dashboard

### Luigi pipeline

The luigi pipeline, loads each of the csv files provided into a SQLite database.

### SQLite database

It was decided to use SQLite as it is open source and easily portable through a `.db` file.

### GoogleSheet

Thank to the GoogleSheet API, we can execuute SQL queries and push the results to a define sheet which will then be used for a dashboard.

## Execute

The below command line runs the luigi pipeline:
```shell
PYTHONPATH="." luigi --module pipeline LoadDataGsheet --local-scheduler
```
