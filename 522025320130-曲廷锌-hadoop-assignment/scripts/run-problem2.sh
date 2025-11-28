#!/bin/bash

# 题目二运行脚本
echo "Running Problem 2: WordCount with Combiner and Partitioner"

# 编译
javac -cp $(hadoop classpath) -d target/classes src/main/java/com/bigdata/assignment/problem2/*.java

# 打包
jar cf hadoop-assignment-problem2.jar -C target/classes .

# 运行
hadoop jar hadoop-assignment-problem2.jar com.bigdata.assignment.problem2.WordCountWithCombinerDriver \
    /public/data/wordcount /users/$USER/homework1/problem2

# 查看结果
echo "=== Results ==="
hdfs dfs -ls /users/$USER/homework1/problem2
hdfs dfs -cat /users/$USER/homework1/problem2/part-r-00000 | head -20