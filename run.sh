#!/bin/bash

# 进入项目目录
cd /home/yinfangyuan/Cquirrel-TPC-H-Q10

# 1. 编译
echo "===================================="
echo " 1. 编译 Maven 项目..."
echo "===================================="
mvn clean package -DskipTests

# 2. 启动 Flink 集群
echo "===================================="
echo " 2. 启动 Flink 集群..."
echo "===================================="
/mnt/c/flink-1.14.6/bin/start-cluster.sh

sleep 3

# 3. 运行 Flink 任务
echo "===================================="
echo " 3. 运行 Cquirrel Q10 任务..."
echo "===================================="
/mnt/c/flink-1.14.6/bin/flink run ./target/cquirrel-tpch-q10-1.0.jar

# 4. 运行结果分析
echo "===================================="
echo " 4. 运行 Python 结果分析..."
echo "===================================="
python3 analyze.py

echo "===================================="
echo " 全部完成！"
echo " 结果文件：result.txt"
echo " 图片：top10_customers.png + nation_revenue.png"
echo "===================================="