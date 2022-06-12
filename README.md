# My portfolio with test tasks from different companies for Data Engineer

All the code has been formatted by [Black: The Uncompromising Code Formatter](https://github.com/psf/black)

## Configured GitHub actions

1. Dependabot checks on weekly basis
1. After each commit GitHub workflows run the following checks:

    - [flake8](https://flake8.pycqa.org/en/latest/)
    - [mypy](https://mypy.readthedocs.io/en/stable/)
    - [markdown linter](https://github.com/markdownlint/markdownlint)

## [Task 1](task1)

Description: calculate pyspark aggregations from the given csv.

Tech:

- python
- spark
- csv

## [Task 2](task2)

Description: calculate pyspark aggregations from the given parquet and csv.

Tech:

- python
- spark
- csv

## [Task 3](task3)

Description: calculate pyspark aggregations from the given csv.

Tech:

- python
- spark
- csv

## [Task 4](task4)

Description:

- calculate pyspark aggregations from the given parquet
- ingest the data to postgres
- read the data from postgres
- calculate pyspark aggregations and save as cvs

Tech:

- python
- spark
- parquet
- postgres in docker with persistent storage

## [Task 5](task5)

Description:

- calculate pyspark metrics and dimensions aggregations from given json
- test the app

Tech:

- python
- spark
- pytest: 91% test coverage according to [Coverage](https://coverage.readthedocs.io/en/6.4.1/)
- json/parquet

## Kafka pet project

The project itself is another [GitHub repo](https://github.com/iamtodor/kafka-twitter-project). The purpose of the project is to prove Java, Kafka, Prometheus and Grafana knowledge.