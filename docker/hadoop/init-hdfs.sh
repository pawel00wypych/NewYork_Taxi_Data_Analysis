#!/bin/bash
# Format NameNode if necessary
if [ ! -d /hadoop/dfs/name/current ]; then
    hdfs namenode -format
fi

# Start HDFS daemons
hdfs namenode &
hdfs datanode &

# Wait a few seconds to ensure NameNode is up
sleep 5

# Create Spark log directory
hdfs dfs -mkdir -p /spark-logs
hdfs dfs -chmod 777 /spark-logs

# Keep the container running
tail -f /dev/null
