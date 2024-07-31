# Breweries API Ingestion Experiments

This is a mini Data Engineering project, exemplifying how an API ingestion could function, with data passing through usually implemented Data Lake layers: Bronze, Silver and Gold, with Airflow being used to orchestrate the jobs.

## How to run

**You need to have Docker installed in your system.**

Clone the repo to your local machine and do the following:

- Build the Docker image using the following command:
    > docker build . -t airflow-local

- Run a container passing the dag and source code:
    > docker run -p 8080:8080 -v ./dags:/opt/airflow/dags -v ./app:/opt/airflow/app -d airflow-local

- Once the container is running, check go to localhost:8080 to visualize the Airflow pipeline
    - You should find the password within your container logs. If you can't find it, check the content in the file `/opt/standalone_admin_password.txt` in your running container
    - Username: admin

- Go to "DAGS" -> "breweries-ingestion" and click "Trigger DAG"

- To check the Gold layer outputs, open the logs page for "gold_pipeline" job

- GG WP

## Improvements and room to grow
If you want here is still a lot of room to improve the whole process. A few examples are:
- Better managing requirements (poetry, pdm, etc.)
- Saving the state of the pages already ingested and execute extractions from where the previous stopped (in a real world scenario, schedule would be defined by business requirements)
- Enabling data quality tools, such as Great Expectations or Collibra DQ
- Defining tables specifications in a file, alongside data quality specifications and cataloging in some tool
- Ensuring Silver and Gold layers schemas and quality rules
- Create test scenarios
- Running the pipelines in the cloud
- Storing tables with names defined in your warehouse (saving not only the path)
- Really, there's a lot of stuff to build up on this mini-project