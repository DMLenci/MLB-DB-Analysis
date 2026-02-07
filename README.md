# MLB-DB-Analysis
Project for practicing setup of local Postgres data-base, running queries, and generating analysis in a professional and reproducible way.

## ENV Setup
Provided in the github repo is a [pyproject.toml](./pyproject.toml) which can be used to create a poetry environment for running all python scripts. With poetry installed simply run `poetry run python python_script.py` to run a python script using the provided env file. To update the poetry env to add new modules run `poetry add module`.

## Working Notes:
- Setup docker container by pulling the postgres image and using docker.yml to create container. Was able to connect to container through DBeaver.
- Setup local env using poetry with pyproject.toml file.
- Created python scripts to pull raw data from MLB API and from retrosheet. The script will pull two files for each season from retrosheet: a gamelog file which tracks game by game scores/outcomes and a eventlog files which tracks play by play for each season. Note there does not exist eventlog files for seasons prior to 1910.
- To-do: Create tables in postgres database and ingest raw data into DB. Once that is complete we can start thinking about how we want to transform data so we can more easily pull info and perform analyses.