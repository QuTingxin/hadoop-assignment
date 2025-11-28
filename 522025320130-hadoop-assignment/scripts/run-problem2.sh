#!/bin/bash

# 题目二运行脚本
INPUT_PATH="/public/data/wordcount"
OUTPUT_PATH="/users/s522025320130/homework1/problem2"

echo "Running Problem 2: WordCount with Combiner and Partitioner"
echo "Input: $INPUT_PATH"
echo "Output: $OUTPUT_PATH"

hadoop jar hadoop-assignment.jar com.bigdata.assignment.problem2.WordCountDriver \
    $INPUT_PATH $OUTPUT_PATH

echo "Problem 2 completed. Results saved to: $OUTPUT_PATH"