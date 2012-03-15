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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class MiniJournalCluster {
  public static class Builder {
    private String baseDir;
    private int numJournalNodes = 3;
    private Configuration conf;
    
    public Builder(Configuration conf) {
      this.conf = conf;
    }
    
    public Builder baseDir(String d) {
      this.baseDir = d;
      return this;
    }
    
    public Builder numJournalNodes(int n) {
      this.numJournalNodes = n;
      return this;
    }
    
    public MiniJournalCluster build() throws IOException {
      return new MiniJournalCluster(this);
    }
  }

  private static final Log LOG = LogFactory.getLog(MiniJournalCluster.class);
  private File baseDir;
  private JournalNode nodes[];
  
  private MiniJournalCluster(Builder b) throws IOException {
    LOG.info("Starting MiniJournalCluster with " +
        b.numJournalNodes + " journal nodes");
    
    if (b.baseDir != null) {
      this.baseDir = new File(b.baseDir);
    } else {
      this.baseDir = new File(MiniDFSCluster.getBaseDirectory());
    }
    
    nodes = new JournalNode[b.numJournalNodes];
    for (int i = 0; i < b.numJournalNodes; i++) {
      nodes[i] = new JournalNode();
      nodes[i].setConf(createConfForNode(b, i));
      nodes[i].init();
    }
  }

  /**
   * Set up the given Configuration object to point to the set of JournalNodes 
   * in this cluster.
   */
  public void setupClientConfigs(Configuration conf) {
    List<String> addrs = Lists.newArrayList();
    for (JournalNode jn : nodes) {
      Preconditions.checkState(jn.isStarted(), "Cluster not yet started");
      InetSocketAddress addr = jn.getBoundAddress();
      assert addr.getPort() != 0;
      addrs.add("127.0.0.1:" + addr.getPort());
    }
    String addrsVal = Joiner.on(",").join(addrs);
    LOG.debug("Setting logger addresses to: " + addrsVal);
    conf.set(QuorumJournalManager.LOGGER_ADDRESSES_KEY, addrsVal);
  }

  /**
   * Start the JournalNodes in the cluster.
   */
  public void start() throws IOException {
    for (JournalNode jn : nodes) {
      jn.start();
    }
  }

  /**
   * Shutdown all of the JournalNodes in the cluster.
   * @throws IOException if one or more nodes failed to stop
   */
  public void shutdown() throws IOException {
    boolean failed = false;
    for (JournalNode jn : nodes) {
      try {
        jn.stop(0);
        jn.join();
      } catch (Exception e) {
        failed = true;
        LOG.warn("Unable to stop journal node " + jn, e);
      }
    }
    if (failed) {
      throw new IOException("Unable to shut down. Check log for details");
    }
  }

  private Configuration createConfForNode(Builder b, int idx) {
    Configuration conf = new Configuration(b.conf);
    File logDir = getStorageDir(idx);
    conf.set(JournalNode.DFS_JOURNALNODE_DIR_KEY, logDir.toString());
    conf.set(JournalNodeRpcServer.DFS_JOURNALNODE_RPC_ADDRESS_KEY, "0.0.0.0:0");
    return conf;
  }

  public File getStorageDir(int idx) {
    return new File(baseDir, "journalnode-" + idx);
  }
  
  public File getCurrentDir(int idx) {
    return new File(getStorageDir(idx), "current");
  }


  public JournalNode getJournalNode(int i) {
    return nodes[i];
  }
}
