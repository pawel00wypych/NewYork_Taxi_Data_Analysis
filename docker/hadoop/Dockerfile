FROM eclipse-temurin:11-jdk

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV HADOOP_LOG_DIR=$HADOOP_HOME/logs
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update && apt-get install -y curl ssh rsync && \
    curl -L https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm -rf /var/lib/apt/lists/*

COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml
COPY mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data
RUN mkdir -p $HADOOP_LOG_DIR && chmod -R 777 $HADOOP_LOG_DIR

COPY init-hdfs.sh /opt/hadoop/init-hdfs.sh
RUN chmod +x /opt/hadoop/init-hdfs.sh

ENTRYPOINT ["/opt/hadoop/init-hdfs.sh"]

