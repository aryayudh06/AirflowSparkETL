FROM apache/airflow:3.0.1-python3.10

USER root

# Pisahkan layers untuk better caching
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk wget procps
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Download Spark dengan CDN yang lebih cepat
RUN wget https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.6-bin-hadoop3.tgz -C /opt/ && \
    ln -s /opt/spark-3.5.6-bin-hadoop3 /opt/spark && \
    rm spark-3.5.6-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:/opt/spark/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt