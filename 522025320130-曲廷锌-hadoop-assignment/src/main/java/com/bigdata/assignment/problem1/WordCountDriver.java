package com.bigdata.assignment.problem1;

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
import java.net.URI;

public class WordCountDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountDriver <input-path> <output-path>");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count Problem 1");
        
        // 
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        if (!fs.exists(inputPath)) {
            System.err.println("Input path does not exist: " + args[0]);
            System.exit(1);
        }
        
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + args[1]);
        }
        
        // 
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // 
        boolean success = job.waitForCompletion(true);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        
        if (success) {
            // 
            generateStatistics(job, processingTime, outputPath, fs);
            System.out.println("WordCount completed successfully!");
        } else {
            System.out.println("WordCount failed!");
            System.exit(1);
        }
    }
    
    private static void generateStatistics(Job job, long processingTime, Path outputPath, FileSystem fs) 
            throws Exception {
        
        // 
        long totalWords = job.getCounters().findCounter("WORD_COUNT", "TOTAL_WORDS").getValue();
        long uniqueWords = job.getCounters().findCounter("WORD_COUNT", "UNIQUE_WORDS").getValue();
        long totalLines = job.getCounters().findCounter("WORD_COUNT", "TOTAL_LINES").getValue();
        
        // 
        Path statsPath = new Path(outputPath, "statistics.txt");
        try (OutputStream os = fs.create(statsPath);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
            
            writer.write("input_files\t1\n");
            writer.write("processing_time\t" + processingTime + "\n");
            writer.write("total_words\t" + totalWords + "\n");
            writer.write("unique_words\t" + uniqueWords + "\n");
            writer.write("total_lines\t" + totalLines + "\n");
        }
        
        System.out.println("=== WordCount Statistics ===");
        System.out.println("Processing Time: " + processingTime + " ms");
        System.out.println("Total Words: " + totalWords);
        System.out.println("Unique Words: " + uniqueWords);
        System.out.println("Total Lines: " + totalLines);
        
        System.out.println("=== Output Files ===");
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        for (FileStatus status : fileStatuses) {
            System.out.println(status.getPath().getName());
        }
    }
}