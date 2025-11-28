package com.bigdata.assignment.problem2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountDriver <input-path> <output-path>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count with Combiner and Partitioner");
        
        // 设置Job参数
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountCombiner.class); // 使用相同的Reducer
        job.setPartitionerClass(AlphabetPartitioner.class);
        job.setNumReduceTasks(4); // 对应4个分区
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // HDFS操作
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input path does not exist: " + args[0]);
            System.exit(1);
        }
        
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        
        // 设置输入输出路径
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        if (success) {
            generateStatistics(job, processingTime, outputPath, fs);
            System.out.println("WordCount with Combiner and Partitioner completed successfully!");
        } else {
            System.out.println("WordCount failed!");
            System.exit(1);
        }
    }
    
    private static void generateStatistics(Job job, long processingTime, Path outputPath, FileSystem fs) 
            throws Exception {
        
        // 获取计数器值
        long combinerInput = job.getCounters().findCounter("COMBINER_STATS", "INPUT_RECORDS").getValue();
        long combinerOutput = job.getCounters().findCounter("COMBINER_STATS", "OUTPUT_RECORDS").getValue();
        long partition0 = job.getCounters().findCounter("PARTITION_STATS", "PARTITION_0").getValue();
        long partition1 = job.getCounters().findCounter("PARTITION_STATS", "PARTITION_1").getValue();
        long partition2 = job.getCounters().findCounter("PARTITION_STATS", "PARTITION_2").getValue();
        long partition3 = job.getCounters().findCounter("PARTITION_STATS", "PARTITION_3").getValue();
        
        long totalWords = combinerInput; // 近似值
        long uniqueWords = combinerOutput; // 近似值
        
        // 创建统计信息文件
        Path statsPath = new Path(outputPath, "statistics.txt");
        try (OutputStream os = fs.create(statsPath);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
            
            writer.write("combiner_input_records\t" + combinerInput + "\n");
            writer.write("combiner_output_records\t" + combinerOutput + "\n");
            writer.write("partition_0_records\t" + partition0 + "\n");
            writer.write("partition_1_records\t" + partition1 + "\n");
            writer.write("partition_2_records\t" + partition2 + "\n");
            writer.write("partition_3_records\t" + partition3 + "\n");
            writer.write("total_words\t" + totalWords + "\n");
            writer.write("unique_words\t" + uniqueWords + "\n");
            writer.write("processing_time\t" + processingTime + "\n");
        }
        
        // 打印统计信息
        System.out.println("=== Combiner and Partitioner Statistics ===");
        System.out.println("Combiner Input Records: " + combinerInput);
        System.out.println("Combiner Output Records: " + combinerOutput);
        System.out.println("Combiner Reduction: " + (combinerInput - combinerOutput) + " records");
        System.out.println("Partition 0 Records: " + partition0);
        System.out.println("Partition 1 Records: " + partition1);
        System.out.println("Partition 2 Records: " + partition2);
        System.out.println("Partition 3 Records: " + partition3);
        System.out.println("Processing Time: " + processingTime + " ms");
    }
}