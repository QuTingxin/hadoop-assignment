#!/bin/bash

# 题目一运行脚本
INPUT_PATH="/public/data/wordcount"
OUTPUT_PATH="/users/s522025320130/homework1/problem1"

echo "Running Problem 1: Basic WordCount"
echo "Input: $INPUT_PATH"
echo "Output: $OUTPUT_PATH"

hadoop jar hadoop-assignment.jar com.bigdata.assignment.problem1.WordCountDriver \
    $INPUT_PATH $OUTPUT_PATH

echo "Problem 1 completed. Results saved to: $OUTPUT_PATH"