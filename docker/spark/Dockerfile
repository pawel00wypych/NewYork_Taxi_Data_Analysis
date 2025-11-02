FROM openjdk:11-jre-slim

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3.3.6
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update && apt-get install -y curl python3 python3-pip procps && \
    pip3 install pyspark fastparquet && \
    curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm -rf /var/lib/apt/lists/*

COPY spark-defaults.conf $SPARK_HOME/conf/
WORKDIR /opt/spark
CMD ["bash"]
