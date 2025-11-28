#!/bin/bash

# 题目三运行脚本
echo "Running Problem 3: Optimized WordCount"

# 编译
javac -cp $(hadoop classpath) -d target/classes src/main/java/com/bigdata/assignment/problem3/*.java

# 打包
jar cf hadoop-assignment-problem3.jar -C target/classes .

# 运行
hadoop jar hadoop-assignment-problem3.jar com.bigdata.assignment.problem3.WordCountOptimizedDriver \
    /public/data/wordcount /users/$USER/homework1/problem3

# 查看结果
echo "=== Results ==="
hdfs dfs -ls /users/$USER/homework1/problem3
hdfs dfs -cat /users/$USER/homework1/problem3/part-r-00000 | head -20