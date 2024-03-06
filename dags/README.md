# Aircraft Accidents Project

This project runs through Apache Airflow in a Docker environment and focuses on collecting data on aircraft accidents. The scraper retrieves data from the [baaa-acro.com](https://www.baaa-acro.com/) website and populates a PostgreSQL database. The collected data is then used for analytics in Metabase.

**Note: This project is currently under development and not yet completed.**

## Features

- **scraper_links**: Retrieves accident URL links from [baaa-acro.com](https://www.baaa-acro.com/) and stores them in a JSON file.
- **db_config.sql**: Creates a PostgreSQL database and necessary data tables.
- **scraper_data.py**: Parses data from the URLs in the JSON file and inserts it into the PostgreSQL database.
- **cleansing.py**: Cleans the collected data and stores it in a new table and schema.
- **get_daily.py**: Checks for new accidents hourly and inserts them into the database.

## Prerequisites

Before running the project, make sure you have the following:

- Docker and Docker Compose installed on your system

## Installation

1. Clone this repository to your local machine.
2. Run the project using Docker Compose:

    ```shell
    docker-compose up -d
    ```

- To stop and remove the containers, make sure you're in the `dockerized` directory and run
    ```
    docker compose down
    ```
- To remove the volumes as well, add the flag `-v` to the command above.

- At any time you can check the container/s currently running by using `docker ps`
