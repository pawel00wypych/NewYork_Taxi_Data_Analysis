FROM eclipse-temurin:11-jdk

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3.3.6
ENV SPARK_HOME=/opt/spark
# Dynamically set JAVA_HOME based on where 'java' is installed
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile && \
    echo "export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin:\$PATH" >> /etc/profile

ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

RUN apt-get update && apt-get install -y curl python3 python3-pip procps && \
    pip3 install --no-cache-dir --break-system-packages fastparquet==2024.2.0 matplotlib==3.9.2 seaborn && \
    curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
         | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm -rf /var/lib/apt/lists/*

COPY spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
WORKDIR /opt/spark
CMD ["bash"]
