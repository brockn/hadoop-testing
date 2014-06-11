package com.cloudera.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DefensiveMapReduceWordCount {
  public static class TokenizerMapper 
       extends Mapper<LongWritable, Text, Text, IntWritable>{
    private static final Log LOG = LogFactory.getLog(DefensiveMapReduceWordCount.class);
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private long recordCount = 0;
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 10KB is much larger than any expected value
      if (value.getLength() > 10 * 1024) {
        throw new IllegalArgumentException("Value of size " + value.getLength() + " found");
      }
      String s = value.toString();
      // log 1 out of 10,000 records
      if (++recordCount % 10000 == 0) {
        LOG.info("Record " + recordCount + ": " + s);
      }
      StringTokenizer itr = new StringTokenizer(s);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context)
      throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  public static void configureJob(Job job, String[] args) 
      throws IllegalArgumentException, IOException {
    job.setJarByClass(CrunchWordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = new Job(conf, "word count");
    configureJob(job, args);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}