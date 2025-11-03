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
  tail -f /opt/hadoop/logs/hadoop-*-namenode-*.log

elif [ "$ROLE" = "datanode" ]; then
  echo "Starting DataNode..."
  hdfs --daemon start datanode

  echo "Waiting for datanode log to appear..."
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
    echo "DataNode log not found after waiting, possible startup failure."
    exec bash
  else
    tail -f "$LOG_FILE"
  fi
fi

else
  echo "Unknown role: $ROLE"
  exec "$@"
fi
