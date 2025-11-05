#!/bin/bash
set -e

ROLE=${HADOOP_ROLE:-namenode}

if [ "$ROLE" = "namenode" ]; then
  if [ ! -d /hadoop/dfs/name/current ]; then
    echo "Formatting NameNode directory..."
    hdfs namenode -format -force -nonInteractive
  fi

  echo "Starting NameNode..."
  hdfs --daemon start namenode
  sleep 5
  hdfs dfsadmin -safemode leave || true
  echo "NameNode is running."

  # --- Automatically upload Spark JARs ---
#  SPARK_HDFS_DIR=/spark/jars
#  LOCAL_SPARK_JARS=/opt/spark/jars
#
#  until hdfs dfs -ls / >/dev/null 2>&1; do
#    echo "Waiting for HDFS to be ready..."
#    sleep 2
#  done
#
#  # Wait for at least 1 live DataNode
#  LIVE_DNS=$(hdfs dfsadmin -report | grep 'Live datanodes' | awk '{print $3}' | tr -d '():')
#  while [ "$LIVE_DNS" -lt 1 ]; do
#    echo "Waiting for at least 1 live DataNode..."
#    sleep 2
#    LIVE_DNS=$(hdfs dfsadmin -report | grep 'Live datanodes' | awk '{print $3}' | tr -d '():')
#  done
#
#  echo "Creating HDFS directory $SPARK_HDFS_DIR (if not exists)..."
#  hdfs dfs -mkdir -p $SPARK_HDFS_DIR || true
#
#  # Clean any incomplete uploads
#  echo "Cleaning up any incomplete Spark JARs in HDFS..."
#  hdfs dfs -rm -r $SPARK_HDFS_DIR/* || true
#
#  echo "Uploading Spark JARs to HDFS..."
#  for jar in /opt/spark/jars/*; do
#    echo "Uploading $jar"
#    hdfs dfs -put "$jar" /spark/jars
#  done
#
#  hdfs dfs -ls /spark/jars | wc -l
  tail -f /opt/hadoop/logs/hadoop-*-namenode-*.log

elif [ "$ROLE" = "datanode" ]; then
  echo "Starting DataNode..."
  hdfs --daemon start datanode

  echo "Waiting for DataNode log..."
  for i in {1..10}; do
    LOG_FILE=$(ls /opt/hadoop/logs/hadoop-*-datanode-*.log 2>/dev/null || true)
    if [ -n "$LOG_FILE" ]; then
      echo "Log file found: $LOG_FILE"
      break
    fi
    echo "No log yet, sleeping..."
    sleep 2
  done

  if [ -z "$LOG_FILE" ]; then
    echo "DataNode log not found after waiting — possible startup failure."
    exec bash
  else
    tail -f "$LOG_FILE"
  fi

elif [ "$ROLE" = "resourcemanager" ]; then
  echo "Starting ResourceManager..."
  yarn --daemon start resourcemanager

  echo "Waiting for ResourceManager log to appear..."
  for i in {1..10}; do
    LOG_FILE=$(ls /opt/hadoop/logs/hadoop-*-resourcemanager-*.log 2>/dev/null || true)
    if [ -n "$LOG_FILE" ]; then
      echo "Log file found: $LOG_FILE"
      break
    fi
    echo "No log yet, sleeping..."
    sleep 2
  done

  if [ -z "$LOG_FILE" ]; then
    echo "ResourceManager log not found after waiting — possible startup issue."
    exec bash
  else
    tail -f "$LOG_FILE"
  fi

elif [ "$ROLE" = "nodemanager" ]; then
  echo "Cleaning NodeManager local cache..."
  NM_LOCAL_DIR=/opt/hadoop/tmp/nm-local-dir
  mkdir -p "$NM_LOCAL_DIR"
  rm -rf "$NM_LOCAL_DIR"/*

  echo "Starting NodeManager..."
  yarn --daemon start nodemanager

  echo "Waiting for NodeManager log..."
  for i in {1..10}; do
    LOG_FILE=$(ls /opt/hadoop/logs/hadoop-*-nodemanager-*.log 2>/dev/null | head -n 1 || true)
    if [ -n "$LOG_FILE" ]; then
      echo "Log file found: $LOG_FILE"
      break
    fi
    echo "No log yet, sleeping..."
    sleep 2
  done

  if [ -z "$LOG_FILE" ]; then
    echo "NodeManager log not found — startup failed. Opening shell for debugging..."
    exec bash
  else
    tail -f "$LOG_FILE"
  fi

else
  echo "Unknown role: $ROLE"
  exec "$@"
fi
