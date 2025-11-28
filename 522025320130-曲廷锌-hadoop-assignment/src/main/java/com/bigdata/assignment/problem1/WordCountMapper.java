package com.bigdata.assignment.problem1;

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
        
        // 将输入行转换为小写并清理特殊字符
        String line = value.toString().toLowerCase();
        
        // 使用正则表达式分割单词，保留字母和数字
        String[] words = line.split("[^a-zA-Z0-9]+");
        
        for (String w : words) {
            // 过滤空字符串
            if (w.length() > 0) {
                word.set(w);
                context.write(word, one);
                
                // 统计处理的单词数
                context.getCounter("WORD_COUNT", "TOTAL_WORDS").increment(1);
            }
        }
        
        // 统计处理的行数
        context.getCounter("LINE_COUNT", "TOTAL_LINES").increment(1);
    }
}