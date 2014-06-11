package com.cloudera.examples;

import java.io.IOException;
import java.util.Map;

import junit.framework.Assert;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestCrunchWordCount {
  
  @Test
  public void testWordSplit() throws IOException {
    InMemoryEmitter<String> emitter = new InMemoryEmitter<String>();
    (new CrunchWordCount.WordSplit()).process("the cat in the hat", emitter);
    Assert.assertEquals(ImmutableList.of("the", "cat", "in", "the", "hat"), 
        emitter.getOutput());
  }

  @Test
  public void testPipeline() throws IOException {
    PCollection<String> lines = MemPipeline.collectionOf("the cat in the hat");
    Map<String, Long> counts = CrunchWordCount.countWords(lines).materializeToMap();
    Assert.assertEquals(4, counts.size());
    Assert.assertEquals(new Long(1), counts.get("cat"));
    Assert.assertEquals(new Long(1), counts.get("hat"));
    Assert.assertEquals(new Long(1), counts.get("in"));
    Assert.assertEquals(new Long(2), counts.get("the"));
  }
}
