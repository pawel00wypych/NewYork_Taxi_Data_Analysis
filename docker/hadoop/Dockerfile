FROM openjdk:11-jre-slim

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN apt-get update && apt-get install -y curl ssh rsync && \
    curl -L https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm -rf /var/lib/apt/lists/*

COPY core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
COPY hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data

COPY init-hdfs.sh /opt/hadoop/init-hdfs.sh
RUN chmod +x /opt/hadoop/init-hdfs.sh
ENTRYPOINT ["bash", "/opt/hadoop/init-hdfs.sh"]

CMD ["bash"]
