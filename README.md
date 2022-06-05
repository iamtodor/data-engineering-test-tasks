# My portfolio with test tasks from different companies for Data Engineer

All the code has been formatted by [Black: The Uncompromising Code Formatter](https://github.com/psf/black)

## Configured github actions

After each commit github workflows run the following checks:

- flake8
- mypy
- markdown linter

## [Task 1](task1)

Tech:

- python
- spark
- csv

## [Task 2](task2)

Tech:

- python
- spark
- csv

## [Task 3](task3)

Tech:

- python
- spark
- csv

## [Task 4](task4)

Tech:

- python
- spark
- parquet
- postgres in docker with persistent storage

```shell
mypy . \
--ignore-missing-imports \
--disallow-untyped-defs \
--disallow-untyped-calls \
--no-implicit-optional

flake8 --max-line-length 120 task1 
black --line-length 120 task4

docker run -v $PWD:/workdir ghcr.io/igorshubovych/markdownlint-cli:latest "**/*.md" \
--ignore env \
--disable MD013

mypy . --ignore-missing-imports --disallow-untyped-defs --disallow-untyped-calls --no-implicit-optional --exclude env
```
