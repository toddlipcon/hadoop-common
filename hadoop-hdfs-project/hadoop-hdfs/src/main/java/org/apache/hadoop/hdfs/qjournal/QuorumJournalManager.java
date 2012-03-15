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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A JournalManager that writes to a set of remote JournalNodes,
 * requiring a quorum of nodes to ack each write.
 */
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager, Configurable {
  static final Log LOG = LogFactory.getLog(QuorumJournalManager.class);

  static final String LOGGER_ADDRESSES_KEY =
    "dfs.namenode.quorum-logger.logger-addresess";


  private static final int START_SEGMENT_TIMEOUT_MS = 20000;

  private Configuration conf;
  private boolean initted;
  
  private AsyncLoggerSet loggers;
  private static final long INVALID_EPOCH = -1;
  private long myEpoch = INVALID_EPOCH;
  
  private synchronized void init() throws IOException {
    if (initted) return;
    Preconditions.checkState(conf != null);
    
    this.loggers = new AsyncLoggerSet(createLoggers());
    
    myEpoch = loggers.createNewUniqueEpoch();
    loggers.setEpoch(myEpoch);
  }
  
  long getWriterEpoch() {
    Preconditions.checkState(myEpoch != INVALID_EPOCH,
        "No epoch created yet");
    return myEpoch;
  }

  protected List<AsyncLogger> createLoggers() throws IOException {
    return createLoggers(conf);
  }
  
  static List<AsyncLogger> createLoggers(Configuration conf)
      throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = getLoggerAddresses(conf);
    for (InetSocketAddress addr : addrs) {
      ret.add(new IPCLoggerChannel(conf, addr));
    }
    return ret;
  }
 
  private static List<InetSocketAddress> getLoggerAddresses(Configuration conf)
      throws IOException {
    String[] addrStrings = conf.getTrimmedStrings(LOGGER_ADDRESSES_KEY);
    if (addrStrings == null) {
      throw new IOException("No loggers configured in " + LOGGER_ADDRESSES_KEY);
    }
    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : addrStrings) {
      addrs.add(NetUtils.createSocketAddr(
          addr, JournalNodeRpcServer.DEFAULT_PORT, LOGGER_ADDRESSES_KEY));
    }
    return addrs;
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId) throws IOException {
    init();
    QuorumCall<AsyncLogger,Void> q = loggers.startLogSegment(txId);
    loggers.waitForWriteQuorum(q, START_SEGMENT_TIMEOUT_MS);
    return new QuorumOutputStream(loggers);
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    QuorumCall<AsyncLogger,Void> q = loggers.finalizeLogSegment(
        firstTxId, lastTxId);
    loggers.waitForWriteQuorum(q, START_SEGMENT_TIMEOUT_MS);
  }

  @Override
  public EditLogInputStream getInputStream(long fromTxnId, boolean inProgressOk)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getNumberOfTransactions(long fromTxnId, boolean inProgressOk)
      throws IOException, CorruptionException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }


  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

}
