package com.cloudera.examples;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CrunchWordCount extends Configured implements Tool, Serializable {
  public int run(String[] args) throws Exception {
    Pipeline pipeline = new MRPipeline(CrunchWordCount.class, getConf());
    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PTable<String, Long> counts = countWords(lines);
    pipeline.writeTextFile(counts, args[1]);
    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }
  public static PTable<String, Long> countWords(PCollection<String> lines) {
    return lines.parallelDo(new WordSplit(), Writables.strings()).count();
  }
  public static class WordSplit extends DoFn<String, String> {
    public void process(String line, Emitter<String> emitter) {
      for (String word : line.split("\\s+")) {
        emitter.emit(word);
      }
    }
  }
  public static void main(String[] args) throws Exception {
    int result = ToolRunner.run(new Configuration(), new CrunchWordCount(), args);
    System.exit(result);
  }
}