# Prerequisites

docker and gnu-make need to be already installed on your system. (Installing them on Linux or MacOS is fairly straightforward, but if you’re running Windows, then we recommend using WSL2.)

The first step is to get a `TimescaleDB` database running locally on your system.

Next, extract the zip archive containing the parquet file. This file contains the following columns:

- timestamp
- user_id
- content_id
- content_type_id
- task_container_id
- user_answer
- answered_correctly
- prior_question_elapsed_time
- prior_question_had_explanation

Among these, you need to only use the columns `timestamp`, `user_id`, `user_answer` and `answered_correctly` for your task.

## The task itself is divided into 4 steps

1. Read the file efficiently using a Python script (feel free to use any API for this) and convert the timestamp column into a DateTime object (row 0 of the file can be ignored).
2. Create a table in the local database to store the contents of the file. (It is not necessary to create the table programmatically. You can simply connect to the database, and run SQL commands to create the table.)
3. Adapt your Python script from step 1 to store the file data in the table.
4. Write a new Python script to fetch the data from the table, calculate total `user_answer` and `answered_correctly` per user per month, and save the results in a CSV file. Any timestamps in the final CSV file should be in the local timezone.

## Solution

### 1. Run postgres in docker container

```shell
docker run \
--rm \
-P -p 127.0.0.1:5432:5432 \
-e POSTGRES_PASSWORD="1234" \
--name pg \
-v $(pwd)/postgres:/var/lib/postgresql/data \
timescale/timescaledb:latest-pg14
```

### 2. Connect to postgres

```shell
pgcli postgresql://postgres:1234@localhost:5432/postgres
```

### 3. Create postgres table

```shell
CREATE TABLE user_answers (
timestamp TIMESTAMP NOT NULL, 
user_id BIGINT NOT NULL, 
user_answer BIGINT NOT NULL, 
answered_correctly BIGINT NOT NULL
);
```

### 4. Run pyspark scripts
