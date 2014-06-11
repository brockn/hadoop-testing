package com.cloudera.examples;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestMiniCluster {
  private static final Log LOG = LogFactory.getLog(TestMiniCluster.class);
  private MiniDFSCluster dfsCluster;
  private MiniMRClientCluster miniMrCluster;
  private FileSystem fileSystem;
  private File baseDir;
  
  @Before
  public void setup() throws IOException {
    baseDir = Files.createTempDir();
    Configuration conf = new Configuration();
    File dfsDir = new File(baseDir, "dfs");
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dfsDir.getPath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(2).build();
    fileSystem = dfsCluster.getFileSystem();
    miniMrCluster = MiniMRClientClusterFactory.create(TestMiniCluster.class, 1, conf);
  }
  
  @After
  public void teardown() {
    if (miniMrCluster != null) {
      try {
        miniMrCluster.stop();
      } catch (IOException e) {
        LOG.error("Error shutting down minimr cluster", e);
      }
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
    if (baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }
  
  @Test
  public void testExample() throws Exception {
    Job job = Job.getInstance(miniMrCluster.getConfig());
    Path input = new Path("/input");
    Path output = new Path("/output");
    fileSystem.mkdirs(input);
    fileSystem.copyFromLocalFile(new Path("/etc/passwd"), input);
    String[] args = {
        input.toString(),
        output.toString(),
    };
    MapReduceWordCount.configureJob(job, args);
    Assert.assertTrue(job.waitForCompletion(true));
    List<String> result = new ArrayList<String>();
    for(FileStatus outputFile : fileSystem.listStatus(output)) {
      InputStream is = fileSystem.open(outputFile.getPath());
      result.addAll(IOUtils.readLines(is));
      is.close();
    }
    Assert.assertEquals(181, result.size());
  }
}
