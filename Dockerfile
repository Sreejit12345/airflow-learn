FROM apache/airflow:3.2.0

# we can add dependencies on airflow worker by extending the image as done below:

RUN pip install requests pandas 
RUN pip install apache-airflow-providers-apache-spark
RUN pip install apache-airflow-providers-docker
RUN pip install boto3
RUN pip install yfinance

# make sure jar in present in build context (same directory as Dockerfile) . In real life make sure the jar is present in same directory as docker build
COPY scala1234learn-assembly-0.1.0-SNAPSHOT.jar /tmp/

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
 
# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.13



RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION} /opt/spark

# Set Spark env vars
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

