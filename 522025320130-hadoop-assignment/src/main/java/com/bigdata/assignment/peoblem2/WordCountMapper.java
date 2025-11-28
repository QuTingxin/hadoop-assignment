package com.bigdata.assignment.problem2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString().toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);
        
        while (tokenizer.hasMoreTokens()) {
            String rawWord = tokenizer.nextToken();
            String cleanWord = rawWord.replaceAll("[^a-zA-Z0-9]", "");
            
            if (!cleanWord.isEmpty()) {
                word.set(cleanWord);
                context.write(word, one);
                
                // Combiner输入记录统计
                context.getCounter("COMBINER_STATS", "INPUT_RECORDS").increment(1);
            }
        }
    }
}