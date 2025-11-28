package com.bigdata.assignment.problem2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
    
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        String word = key.toString().toUpperCase();
        
        if (word.isEmpty()) {
            return 0;
        }
        
        char firstChar = word.charAt(0);
        
        // 分区规则：
        // A-F → 分区0, G-N → 分区1, O-S → 分区2, T-Z → 分区3
        if (firstChar >= 'A' && firstChar <= 'F') {
            return 0;
        } else if (firstChar >= 'G' && firstChar <= 'N') {
            return 1;
        } else if (firstChar >= 'O' && firstChar <= 'S') {
            return 2;
        } else if (firstChar >= 'T' && firstChar <= 'Z') {
            return 3;
        } else {
            // 数字和特殊字符分配到分区0
            return 0;
        }
    }
}