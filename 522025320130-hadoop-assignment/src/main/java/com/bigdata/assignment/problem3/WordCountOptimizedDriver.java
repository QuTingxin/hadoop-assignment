package com.bigdata.assignment.problem3;

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

public class WordCountOptimizedDriver {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        
        // 任务调优配置
        // 设置Map任务数量（通过输入分片大小控制）
        conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728"); // 128MB
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "268435456"); // 256MB
        
        // 设置压缩
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", 
                "org.apache.hadoop.io.compress.SnappyCodec");
        
        // 创建Job对象
        Job job = Job.getInstance(conf, "word count optimized");
        job.setJarByClass(WordCountOptimizedDriver.class);
        job.setMapperClass(WordCountOptimizedMapper.class);
        job.setCombinerClass(WordCountOptimizedCombiner.class);
        job.setReducerClass(WordCountOptimizedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 设置Reduce任务数量
        job.setNumReduceTasks(4);
        
        // HDFS操作
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(otherArgs[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input directory does not exist: " + otherArgs[0]);
            System.exit(1);
        }
        
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
            // 生成性能报告
            generatePerformanceReport(job, fs, outputPath, processingTime);
            System.out.println("Optimized WordCount completed successfully!");
        } else {
            System.out.println("Optimized WordCount failed!");
            System.exit(1);
        }
    }
    
    private static void generatePerformanceReport(Job job, FileSystem fs, Path outputPath, 
                                                long processingTime) throws Exception {
        
        // 获取计数器值
        long inputRecords = job.getCounters().findCounter("STATS", "INPUT_RECORDS").getValue();
        long outputRecords = job.getCounters().findCounter("STATS", "OUTPUT_RECORDS").getValue();
        long totalLines = job.getCounters().findCounter("STATS", "TOTAL_LINES").getValue();
        long combinerInput = job.getCounters().findCounter("COMBINER", "INPUT_RECORDS").getValue();
        long combinerOutput = job.getCounters().findCounter("COMBINER", "OUTPUT_RECORDS").getValue();
        
        int mapTasks = job.getConfiguration().getInt("mapreduce.job.maps", 0);
        int reduceTasks = job.getConfiguration().getInt("mapreduce.job.reduces", 0);
        
        // 计算处理速度
        double processingSpeed = (double) inputRecords / processingTime * 1000; // records per second
        
        // 创建性能报告文件
        Path reportPath = new Path(outputPath, "performance-report.txt");
        try (OutputStream os = fs.create(reportPath);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
            
            writer.write("total_processing_time\t" + processingTime + "\n");
            writer.write("map_tasks_count\t" + mapTasks + "\n");
            writer.write("reduce_tasks_count\t" + reduceTasks + "\n");
            writer.write("input_records\t" + inputRecords + "\n");
            writer.write("output_records\t" + outputRecords + "\n");
            writer.write("total_words\t" + inputRecords + "\n");
            writer.write("combiner_enabled\ttrue\n");
            writer.write("combiner_input_records\t" + combinerInput + "\n");
            writer.write("combiner_output_records\t" + combinerOutput + "\n");
            writer.write("processing_speed_records_per_sec\t" + String.format("%.2f", processingSpeed) + "\n");
            writer.write("total_lines_processed\t" + totalLines + "\n");
            writer.write("combiner_reduction_ratio\t" + 
                        String.format("%.2f", (1 - (double)combinerOutput/combinerInput) * 100) + "%\n");
        }
        
        // 显示性能报告
        System.out.println("=== Performance Report ===");
        System.out.println("Total processing time: " + processingTime + " ms");
        System.out.println("Map tasks count: " + mapTasks);
        System.out.println("Reduce tasks count: " + reduceTasks);
        System.out.println("Input records: " + inputRecords);
        System.out.println("Output records: " + outputRecords);
        System.out.println("Total words: " + inputRecords);
        System.out.println("Combiner enabled: true");
        System.out.println("Combiner input records: " + combinerInput);
        System.out.println("Combiner output records: " + combinerOutput);
        System.out.println("Combiner reduction ratio: " + 
                          String.format("%.2f%%", (1 - (double)combinerOutput/combinerInput) * 100));
        System.out.println("Processing speed: " + String.format("%.2f", processingSpeed) + " records/sec");
        System.out.println("Total lines processed: " + totalLines);
    }
}