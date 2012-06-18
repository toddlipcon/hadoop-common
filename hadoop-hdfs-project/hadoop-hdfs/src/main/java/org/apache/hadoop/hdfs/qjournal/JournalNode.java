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
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * The JournalNode is a daemon which allows namenodes using
 * the QuorumJournalManager to log and retrieve edits stored
 * remotely. It is a thin wrapper around a local edit log
 * directory with the addition of facilities to participate
 * in the quorum protocol.
 */
@InterfaceAudience.Private
public class JournalNode implements Tool, Configurable {
  public static final Log LOG = LogFactory.getLog(JournalNode.class);
  static final String DFS_JOURNALNODE_DIR_KEY =
    "dfs.journalnode.journal.dir";
  static final String DFS_JOURNALNODE_DIR_DEFAULT =
    "/tmp/hadoop/dfs/journalnode/";
  
  private Configuration conf;
  private JournalNodeRpcServer rpcServer;
  private Map<String, Journal> journalsById = Maps.newHashMap();
  
  
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;

  synchronized Journal getOrCreateJournal(String jid) {
    QuorumJournalManager.checkJournalId(jid);
    
    Journal journal = journalsById.get(jid);
    if (journal == null) {
      File logDir = getLogDir(jid);
      LOG.info("logDir: " + logDir);
      journal = new Journal(logDir, new ErrorReporter());
      journalsById.put(jid, journal);
    }
    
    return journal;
  }


  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    start();
    return join();
  }

  /**
   * Start listening for edits via RPC.
   */
  void start() throws IOException {
    rpcServer = new JournalNodeRpcServer(conf, this);
    rpcServer.start();
  }

  public boolean isStarted() {
    return rpcServer != null;
  }

  /**
   * @return the address the IPC server is bound to
   */
  public InetSocketAddress getBoundAddress() {
    return rpcServer.getAddress();
  }

  /**
   * Stop the daemon with the given status code
   * @param rc the status code with which to exit (non-zero
   * should indicate an error)
   */
  public void stop(int rc) {
    this.resultCode = rc;
    if (rpcServer != null) { 
      rpcServer.stop();
    }
    
    for (Journal j : journalsById.values()) {
      IOUtils.cleanup(LOG, j);
    }
  }

  /**
   * Wait for the daemon to exit.
   * @return the result code (non-zero if error)
   */
  int join() throws InterruptedException {
    if (rpcServer != null) {
      rpcServer.join();
    }
    return resultCode;
  }

  private File getLogDir(String jid) {
    String dir = conf.get(DFS_JOURNALNODE_DIR_KEY,
        DFS_JOURNALNODE_DIR_DEFAULT).trim();
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty(),
        "bad journal identifier: %s", jid);
    return new File(new File(dir), jid);
  }

  
  private class ErrorReporter implements StorageErrorReporter {
    @Override
    public void reportErrorsOnDirectory(StorageDirectory sd) {
      LOG.fatal("Error reported on storage directory " + sd + "... exiting",
          new Exception());
      stop(1);
    }
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new JournalNode(), args));
  }

}
