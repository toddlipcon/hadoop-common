package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNNWithQJM {
  Configuration conf = new HdfsConfiguration();
  private MiniJournalCluster mjc;
  private Path TEST_PATH = new Path("/test-dir");
  private Path TEST_PATH_2 = new Path("/test-dir");

  @Before
  public void startJNs() throws Exception {
    mjc = new MiniJournalCluster.Builder(conf).build();
    mjc.start();
  }
  
  @After
  public void stopJNs() throws Exception {
    if (mjc != null) {
      mjc.shutdown();
    }
  }
  
  @Test
  public void testLogAndRestart() throws IOException {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        "qjournal://localhost/myjournal");
    mjc.setupClientConfigs(conf);
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    try {
      cluster.getFileSystem().mkdirs(TEST_PATH);
      
      // Restart the NN and make sure the edit was persisted
      // and loaded again
      cluster.restartNameNode();
      
      assertTrue(cluster.getFileSystem().exists(TEST_PATH));
      cluster.getFileSystem().mkdirs(TEST_PATH_2);
      
      // Restart the NN again and make sure both edits are persisted.
      cluster.restartNameNode();
      assertTrue(cluster.getFileSystem().exists(TEST_PATH));
      assertTrue(cluster.getFileSystem().exists(TEST_PATH_2));
    } finally {
      cluster.shutdown();
    }
    
    
  }
}
