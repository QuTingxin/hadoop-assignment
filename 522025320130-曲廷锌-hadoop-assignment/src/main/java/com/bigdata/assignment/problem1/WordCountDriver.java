package com.bigdata.assignment.problem1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        
        // 创建Job对象
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // HDFS操作：检查输入目录
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(otherArgs[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input directory does not exist: " + otherArgs[0]);
            System.exit(1);
        }
        
        // HDFS操作：删除已存在的输出目录
        Path outputPath = new Path(otherArgs[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + otherArgs[1]);
        }
        
        // 设置输入输出路径
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        if (success) {
            // 生成统计信息
            generateStatistics(job, fs, outputPath, processingTime, inputPath);
            System.out.println("WordCount completed successfully!");
        } else {
            System.out.println("WordCount failed!");
            System.exit(1);
        }
    }
    
    private static void generateStatistics(Job job, FileSystem fs, Path outputPath, 
                                         long processingTime, Path inputPath) throws Exception {
        
        // 获取计数器值
        long totalWords = job.getCounters().findCounter("WORD_COUNT", "TOTAL_WORDS").getValue();
        long uniqueWords = job.getCounters().findCounter("WORD_COUNT", "UNIQUE_WORDS").getValue();
        long totalLines = job.getCounters().findCounter("LINE_COUNT", "TOTAL_LINES").getValue();
        
        // 获取输入文件数量
        int inputFiles = fs.listStatus(inputPath).length;
        
        // 创建统计信息文件
        Path statsPath = new Path(outputPath, "statistics.txt");
        try (OutputStream os = fs.create(statsPath);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
            
            writer.write("input_files\t" + inputFiles + "\n");
            writer.write("processing_time\t" + processingTime + "\n");
            writer.write("total_words\t" + totalWords + "\n");
            writer.write("unique_words\t" + uniqueWords + "\n");
            writer.write("total_lines\t" + totalLines + "\n");
        }
        
        // 显示统计信息
        System.out.println("=== WordCount Statistics ===");
        System.out.println("Input files: " + inputFiles);
        System.out.println("Processing time: " + processingTime + " ms");
        System.out.println("Total words: " + totalWords);
        System.out.println("Unique words: " + uniqueWords);
        System.out.println("Total lines: " + totalLines);
        
        // 显示输出文件列表
        System.out.println("\n=== Output Files ===");
        for (org.apache.hadoop.fs.FileStatus file : fs.listStatus(outputPath)) {
            System.out.println(file.getPath().getName() + " (" + file.getLen() + " bytes)");
        }
    }
}