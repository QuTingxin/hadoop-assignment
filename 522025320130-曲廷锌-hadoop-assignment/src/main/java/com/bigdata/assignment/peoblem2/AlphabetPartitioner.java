package com.bigdata.assignment.problem2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {
    
    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {
        if (key == null || key.toString().isEmpty()) {
            return 0;
        }
        
        String word = key.toString();
        if (word.isEmpty()) {
            return 0;
        }
        
        char firstChar = Character.toUpperCase(word.charAt(0));
        
        // 分区规则
        if (firstChar >= 'A' && firstChar <= 'F') {
            context.getCounter("PARTITION_STATS", "PARTITION_0").increment(1);
            return 0;
        } else if (firstChar >= 'G' && firstChar <= 'N') {
            context.getCounter("PARTITION_STATS", "PARTITION_1").increment(1);
            return 1;
        } else if (firstChar >= 'O' && firstChar <= 'S') {
            context.getCounter("PARTITION_STATS", "PARTITION_2").increment(1);
            return 2;
        } else if (firstChar >= 'T' && firstChar <= 'Z') {
            context.getCounter("PARTITION_STATS", "PARTITION_3").increment(1);
            return 3;
        } else {
            // 数字、特殊字符等分配到分区0
            context.getCounter("PARTITION_STATS", "PARTITION_0").increment(1);
            return 0;
        }
    }
}