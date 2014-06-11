package com.cloudera.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

import com.cloudera.examples.MapReduceWordCount.TokenizerMapper;
import com.cloudera.examples.MapReduceWordCount.IntSumReducer;

public class TestMapReduceWordCount {  
  @Test
  public void testExample() throws IOException {
    TokenizerMapper mapper = new TokenizerMapper();
    IntSumReducer reducer = new IntSumReducer();
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver = 
        MapReduceDriver.newMapReduceDriver(mapper, reducer);
    driver.withInput(new LongWritable(0), new Text("the cat"))
      .withInput(new LongWritable(1), new Text("in the hat"))
      .withInput(new LongWritable(2), new Text("is black"))
      .withOutput(new Text("black"), new IntWritable(1))
      .withOutput(new Text("cat"), new IntWritable(1))
      .withOutput(new Text("hat"), new IntWritable(1))
      .withOutput(new Text("in"), new IntWritable(1))
      .withOutput(new Text("is"), new IntWritable(1))
      .withOutput(new Text("the"), new IntWritable(2))
      .runTest();
  }
}
