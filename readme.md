1) have docker desktop installed
2) docker compose up airflow-init         :This sets up folders and airflow database /username and password 
3) docker compose up                      : Starts the containers required, and spins on airflow based on celery executor



If we need more workers, we can simply clone this code and place a new block with name of container changed. The command
celery worker will convert it to a airflow worker.


  airflow-worker:
    <<: *airflow-common
    command: celery worker
    healthcheck:
      # yamllint disable rule:line-length
      test: ["CMD-SHELL", 'celery --app airflow.providers.celery.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}" || celery --app airflow.executors.celery_executor.app inspect ping -d "celery@$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    environment:
      <<: *airflow-common-env
      # Required to handle warm shutdown of the celery workers properly
      # See https://airflow.apache.org/docs/docker-stack/entrypoint.html#signal-propagation
      DUMB_INIT_SETSID: "0"
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-apiserver:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully




4) in python venv also install pip install apache-airflow==3.0.0  -- reqd to get IDE support
--------------------------------------------------------------------------------------------------------------------

If additional python packages are to be installed or u want to put more things in airflow worker like install spark providers, install python packages put them in DockerFile and comment this line:

  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.2.0}

  also uncomment this line:

  build: .


This will make sure packages and other requirements are installed in workers. Remeber to do docker-compose build after changing DockerFIle.
 


--------------------------------------------------------------------------------------------------------------------

Post setup airflow webserver should be available in localhost:8080

In postgres container to access the airflow metadata db run:

psql -U airflow

This opens the postgres shell, on which we can run sql queries.

select * from dag;
