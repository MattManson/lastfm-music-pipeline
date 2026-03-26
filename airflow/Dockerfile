FROM apache/airflow:2.8.1

USER root

# Install Java - PySpark needs Java to run
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set Java environment variable so PySpark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

USER airflow

# Install PySpark and delta-spark
RUN pip install pyspark==3.5.8 delta-spark==3.3.0 requests