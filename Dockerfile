FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && \
    apt-get install -y wget && \
    apt-get install -y default-jre

RUN chown -R airflow:root /usr/local
COPY start.sh /start.sh
RUN chmod +x /start.sh

# Installing Python packages from requirements.txt
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/bin/bash","/start.sh"]
