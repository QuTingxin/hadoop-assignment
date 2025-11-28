package com.bigdata.assignment.problem2;

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

public class WordCountWithCombinerDriver {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        
        // 创建Job对象
        Job job = Job.getInstance(conf, "word count with combiner and partitioner");
        job.setJarByClass(WordCountWithCombinerDriver.class);
        job.setMapperClass(WordCountWithCombinerMapper.class);
        job.setCombinerClass(WordCountCombiner.class);
        job.setReducerClass(WordCountCombiner.class); // 使用相同的Reducer
        job.setPartitionerClass(AlphabetPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 设置Reduce任务数量为4（对应4个分区）
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
            // 生成统计信息
            generateStatistics(job, fs, outputPath, processingTime, inputPath);
            System.out.println("WordCount with Combiner and Partitioner completed successfully!");
        } else {
            System.out.println("WordCount with Combiner and Partitioner failed!");
            System.exit(1);
        }
    }
    
    private static void generateStatistics(Job job, FileSystem fs, Path outputPath, 
                                         long processingTime, Path inputPath) throws Exception {
        
        // 获取计数器值
        long combinerInput = job.getCounters().findCounter("COMBINER_STATS", "INPUT_RECORDS").getValue();
        long combinerOutput = job.getCounters().findCounter("COMBINER_STATS", "OUTPUT_RECORDS").getValue();
        
        // 统计各分区的记录数
        long partition0 = 0, partition1 = 0, partition2 = 0, partition3 = 0;
        
        // 通过读取输出文件来统计各分区记录数
        for (int i = 0; i < 4; i++) {
            Path partFile = new Path(outputPath, "part-r-0000" + i);
            if (fs.exists(partFile)) {
                long recordCount = countRecordsInFile(fs, partFile);
                switch (i) {
                    case 0: partition0 = recordCount; break;
                    case 1: partition1 = recordCount; break;
                    case 2: partition2 = recordCount; break;
                    case 3: partition3 = recordCount; break;
                }
            }
        }
        
        long totalWords = partition0 + partition1 + partition2 + partition3;
        long uniqueWords = totalWords; // 在WordCount中，输出记录数就是唯一单词数
        
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
        
        // 显示统计信息
        System.out.println("=== WordCount with Combiner and Partitioner Statistics ===");
        System.out.println("Combiner input records: " + combinerInput);
        System.out.println("Combiner output records: " + combinerOutput);
        System.out.println("Combiner reduction ratio: " + 
                          String.format("%.2f%%", (1 - (double)combinerOutput/combinerInput) * 100));
        System.out.println("Partition 0 records: " + partition0);
        System.out.println("Partition 1 records: " + partition1);
        System.out.println("Partition 2 records: " + partition2);
        System.out.println("Partition 3 records: " + partition3);
        System.out.println("Total words: " + totalWords);
        System.out.println("Unique words: " + uniqueWords);
        System.out.println("Processing time: " + processingTime + " ms");
    }
    
    private static long countRecordsInFile(FileSystem fs, Path filePath) throws Exception {
        long count = 0;
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(fs.open(filePath)))) {
            while (reader.readLine() != null) {
                count++;
            }
        }
        return count;
    }
}