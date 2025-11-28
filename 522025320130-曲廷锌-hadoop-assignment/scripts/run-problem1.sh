#!/bin/bash

# 题目一运行脚本
echo "Running Problem 1: Basic WordCount"

# 编译
javac -cp $(hadoop classpath) -d target/classes src/main/java/com/bigdata/assignment/problem1/*.java

# 打包
jar cf hadoop-assignment-problem1.jar -C target/classes .

# 运行
hadoop jar hadoop-assignment-problem1.jar com.bigdata.assignment.problem1.WordCountDriver \
    /public/data/wordcount /users/$USER/homework1/problem1

# 查看结果
echo "=== Results ==="
hdfs dfs -ls /users/$USER/homework1/problem1
hdfs dfs -cat /users/$USER/homework1/problem1/part-r-00000 | head -20