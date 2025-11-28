package com.bigdata.assignment.problem3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountOptimizedMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        
        String line = value.toString().toLowerCase();
        String[] words = line.split("[^a-zA-Z0-9]+");
        
        for (String w : words) {
            if (w.length() > 0) {
                word.set(w);
                context.write(word, one);
                
                // 统计输入记录数
                context.getCounter("STATS", "INPUT_RECORDS").increment(1);
            }
        }
        
        // 统计处理的行数
        context.getCounter("STATS", "TOTAL_LINES").increment(1);
    }
}