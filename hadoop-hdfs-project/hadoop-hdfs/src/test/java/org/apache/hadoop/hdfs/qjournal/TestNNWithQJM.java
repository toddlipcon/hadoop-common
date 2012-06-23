/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.qjournal;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doNothing;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

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
        mjc.getQuorumJournalURI("myjournal").toString());
    
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
  
  @Test
  public void testNewNamenodeTakesOverWriter() throws Exception {
    File nn1Dir = new File(
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image-nn1");
    File nn2Dir = new File(
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image-nn2");
    
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        nn1Dir.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    Runtime spyRuntime = NameNodeAdapter.spyOnEditLogRuntime(
        cluster.getNameNode());
    doNothing().when(spyRuntime).exit(anyInt());

    try {
      cluster.getFileSystem().mkdirs(TEST_PATH);
      
      // Start a second NN pointed to the same quorum.
      // We need to copy the image dir from the first NN -- or else
      // the new NN will just be rejected because of Namespace mismatch.
      FileUtil.fullyDelete(nn2Dir);
      FileUtil.copy(nn1Dir, FileSystem.getLocal(conf).getRaw(),
          new Path(nn2Dir.getAbsolutePath()), false, conf);
      
      Configuration conf2 = new Configuration();
      conf2.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
          nn2Dir.getAbsolutePath());
      conf2.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
          mjc.getQuorumJournalURI("myjournal").toString());
      MiniDFSCluster cluster2 = new MiniDFSCluster.Builder(conf2)
        .numDataNodes(0)
        .format(false)
        .manageNameDfsDirs(false)
        .build();
      
      // Check that the new cluster sees the edits made on the old cluster
      try {
        assertTrue(cluster2.getFileSystem().exists(TEST_PATH));
      } finally {
        cluster2.shutdown();
      }
      
      // Check that, if we try to write to the old NN
      // that it aborts.
      Mockito.verify(spyRuntime, Mockito.times(0)).exit(Mockito.anyInt());
      cluster.getFileSystem().mkdirs(new Path("/x"));
      Mockito.verify(spyRuntime, Mockito.times(1)).exit(Mockito.anyInt());
      
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testMismatchedNNIsRejected() throws Exception {
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
        MiniDFSCluster.getBaseDirectory() + "/TestNNWithQJM/image");
    conf.set(DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_KEY,
        mjc.getQuorumJournalURI("myjournal").toString());
    
    // Start a NN, so the storage is formatted with its namespace info. 
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
      .numDataNodes(0)
      .manageNameDfsDirs(false)
      .build();
    cluster.shutdown();
    
    // Create a new (freshly-formatted) NN, which should not be able to
    // reuse the same journal, since its journal ID would not match.
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(0)
        .manageNameDfsDirs(false)
        .build();
      fail("New NN with different namespace should have been rejected");
    } catch (IOException ioe) {
      
    }
  }
}
