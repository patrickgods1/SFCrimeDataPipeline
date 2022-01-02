# SFCrimeDataPipeline
The City of San Francisco provides a daily updated report with incidents filed with SFPD's reporting system. This data pipeline automates the ELT process into a dimensional model ready for data analysis querying.

## Table of Contents
  * [Extract Load Tansform](#extract-load-transform)
    * [Data Pipeline Diagram](#data-pipeline-diagram)
    * [Dimensional Model](#dimensional-model)
  * [Dashboard](#dashboard)
  * [Development](#development)
    * [Built With](#built-with)
    * [Setup](#setup)
      * [.env File](#.env-file)
      * [Docker](#docker)
  * [Authors](#authors)

## Extract Load Transform
* Extract - Download the data from [Police Department Incident Reports: 2018 to Present](https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-2018-to-Present/wg3w-h783) dataset as CSV format.
* Load - Copy data from CSV file into a staging table in Postgres database.
* Transform - The raw data is transformed into a [dimensional model](#dimensional-model) with a star schema design.

### Data Pipeline Diagram
![SFCrimePipeline](https://user-images.githubusercontent.com/60832092/147300250-72944bbc-7843-4ec5-81b4-dfc6d5d6c9f0.png)

### Dimensional Model
![SFCrimeDimensionalModel](https://user-images.githubusercontent.com/60832092/147300248-c05daa58-a3d6-4273-83e4-650113c01ca2.png)

## Dashboard
Tableau Public [dashboard](https://public.tableau.com/app/profile/pat3330/viz/SFCrimeData_16407224575150/Story?publish=yes).

## Development
These instructions will get you a copy of the project up and running on your local machine for development.

### Built With
* [Python 3.6](https://docs.python.org/3/) - The scripting language used.
* [Pandas](https://pandas.pydata.org/) - Data manipulation tool used.
* [Requests](https://docs.python-requests.org/en/latest/) - HTTP library used to make fetch the data.
* [pygsheets](https://pygsheets.readthedocs.io/en/stable/) - Library used to interactive Google Sheets via Google Sheets API v4.
* [Docker](https://www.docker.com/) - Container platform used to containerize PostgreSQL and PGAdmin.
* [PostgreSQL](https://www.postgresql.org/) - Relational database used to store extracted data.
* [pgAdmin](https://www.pgadmin.org/) - GUI tool used to interact and manage Postgres database.
* [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration tool used to manage and schedule the data pipeline.

### Setup
#### .env File
Create a `.env` file under the parent directory of the project (in the same directory as the `docker-compose.yaml` file):
```
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
_PIP_ADDITIONAL_REQUIREMENTS=pandas pygsheets
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
PGADMIN_DEFAULT_EMAIL=example_email@gmail.com
PGADMIN_DEFAULT_PASSWORD=example_password
```
* Replace usernames and passwords to one of your own. Configurations can be changed to suit your needs.

#### Docker
`docker-compose.yaml` file configurations can be changed to suit your needs.

Run the following command to start the container:
```
docker-compose --env-file .env up
```
Run the following command to stop the container:
```
docker-compose down
```

## Authors
* **Patrick Yu** - *Initial work* - [patrickgods1](https://github.com/patrickgods1)