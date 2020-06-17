# Capstone

## Scope of Work

The purpose of this project is to demonstrate various skills associated with
data engineering projects. In particular, developing ETL pipelines using
Airflow, data storage as well as defining efficient data models e.g. star
schema, etc. 
For this project I will perform a pipeline using the data of the Madrid Public
Libraries provided by the [Madrid OpenData
Portal](https://datos.madrid.es/portal/site/egob),focusing only on the _book_ 
loans. Just for test purposes the data used will be only for 2018, 
but any other data could be easily added.

## Data Description & Sources

The two main sources used in this project are:

- Library Catalog [(Bibliotecas Públicas de Madrid.
  Catálogo)](https://datos.madrid.es/sites/v/index.jsp?vgnextoid=67065cde99be2410VgnVCM1000000b205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD):
wich provides the whole catalog of items stored in all Public Libraries of
Madrid.
- Historical active Loans [(Bibliotecas Públicas de Madrid. Préstamos
  Activos)](https://datos.madrid.es/sites/v/index.jsp?vgnextoid=b98bde41aceeb410VgnVCM2000000c205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD):
which provides the historical of all loans from 2014 until the current year. 

## ETL Process

### Download datasets

The first step for the pipeline is to download all the datasets from the
repository, for this I'm using an Airflow PythonOperator and the library
[wget](https://pypi.org/project/wget/). The data is stored in the _datasets_
folder.

### Process staging data

The loans data is stored in CSV format, so this could be used directly with no
processing.
The catalog dataset is provided in the [MARCXML
format](https://en.wikipedia.org/wiki/MARC_standards#MARCXML), to make use of
the data  this needs to be converted from this format. For this task I'm
using the library [pymarc](https://pypi.org/project/pymarc/), which allow
convert MARC into JSON. As part of the scope of the project only the data for
books will be extracted. To determine how to extract only those items the script
needs to filter the data using some specific MARC fields (as stated in the
documentation provided from the dataset repository), finally only those records
will be stored in a JSON file which will be used for the next step.

## Instructions

- Install required libraries:

First you need to execute the following commands:

```
pip install -r requirements.txt
```

- Create tables:
```
psql -U $DB_USER -h $DB_HOST -W $DB  < create_tables.sql
```

