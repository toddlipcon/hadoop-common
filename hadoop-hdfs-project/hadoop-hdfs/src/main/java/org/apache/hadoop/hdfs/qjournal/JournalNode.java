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

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.LogSegmentProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.protocol.JournalInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

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
  private FileJournalManager fjm;
  
  /**
   * When stopped, the daemon will exit with this code. 
   */
  private int resultCode = 0;
  
  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  // TODO: set me at startup
  private boolean curSegmentFinalized = false;
  
  /** f_p in ZAB terminology */
  private PersistentLong lastPromisedEpoch;
  
  /** f_a in ZAB terminology */
  private PersistentLong lastAcceptedEpoch;

  
  public GetEpochInfoResponseProto getEpochInfo() throws IOException {
    return GetEpochInfoResponseProto.newBuilder()
      .setLastPromisedEpoch(getLastPromisedEpoch())
      .build();
  }
  
  synchronized long getLastPromisedEpoch() throws IOException {
    return lastPromisedEpoch.get();
  }

  public synchronized NewEpochResponseProto newEpoch(long epoch)
      throws IOException {
    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getEpochInfo());
    }
    lastPromisedEpoch.set(epoch);
    if (curSegment != null) {
      curSegment.close();
      curSegment = null;
    }
    return NewEpochResponseProto.newBuilder()
      .setLastSegment(LogSegmentProto.newBuilder()
          .setStartTxId(curSegmentTxId)
          .setEndTxId(nextTxId - 1)
          .setIsInProgress(!curSegmentFinalized))
          .setCurrentEpoch(lastAcceptedEpoch.get())
      .build();
  }


  public synchronized void journal(RequestInfo reqInfo, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    checkRequest(reqInfo);
    Preconditions.checkState(curSegment != null,
        "Can't write, no segment open");
    Preconditions.checkState(nextTxId == firstTxnId,
        "Can't write txid " + firstTxnId + " expecting nextTxId=" + nextTxId);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + (firstTxnId + numTxns - 1));
    }
    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    curSegment.flush();
    nextTxId += numTxns;
  }

  private void checkRequest(RequestInfo reqInfo) throws IOException {
    // Invariant 25 from ZAB paper
    if (reqInfo.getEpoch() < lastPromisedEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is less than the last promised epoch " + lastPromisedEpoch);
    }
    // TODO: some check on serial number that they only increase from a given
    // client
  }

  public synchronized void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    checkRequest(reqInfo);
    Preconditions.checkState(curSegment == null,
        "Can't start a log segment, already writing " + curSegment);
    Preconditions.checkState(nextTxId == txid || nextTxId == HdfsConstants.INVALID_TXID,
        "Can't start log segment " + txid + " expecting nextTxId=" + nextTxId);
    curSegment = fjm.startLogSegment(txid);
    curSegmentTxId = txid;
    nextTxId = txid;
    curSegmentFinalized = false;
  }
  
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    checkRequest(reqInfo);
    
    if (startTxId == curSegmentTxId) {
      Preconditions.checkState(nextTxId == endTxId + 1,
          "Trying to finalize current log segment (startTxId=%s) " +
          "with ending txid=%s, but cur txid is %s",
          curSegmentTxId, endTxId, nextTxId - 1);
      Preconditions.checkState(!curSegmentFinalized,
          "Trying to finalize already-finalized segment %s-%s",
          curSegmentTxId, endTxId);
      
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
      }
      fjm.finalizeLogSegment(startTxId, endTxId);
      // TODO: add some sanity check here for non-overlapping edits
      // in debug case?
      curSegmentFinalized = true;
    } else {
      fjm.finalizeLogSegment(startTxId, endTxId);
    }
  }
  

  public void syncLogs(SyncLogsRequestProto req) {
    
    
  }


  public RemoteEditLogManifest getEditLogManifest(JournalInfo info,
      long sinceTxId) throws IOException {
    // TODO: check fencing info?
    return new RemoteEditLogManifest(
        fjm.getRemoteEditLogs(sinceTxId));
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
    init();
    start();
    return join();
  }

  /**
   * Initialize the FileJournalManager, etc.
   * 
   * TODO: maybe this is just part of start()?
   */
  void init() throws IOException {
    File logDir = getLogDir();
    StorageDirectory sd = new StorageDirectory(logDir);
    // TODO: this is wrong:
    sd.clearDirectory();
    
    this.fjm = new FileJournalManager(sd, new ErrorReporter());
    this.lastPromisedEpoch = new PersistentLong(
        new File(fjm.getStorageDirectory().getCurrentDir(),
            "last-promised-epoch"), 0);
    this.lastAcceptedEpoch = new PersistentLong(
        new File(fjm.getStorageDirectory().getCurrentDir(),
            "last-accepted-epoch"), 0);
  }

  /**
   * Start listening for edits via RPC.
   */
  void start() throws IOException {
    Preconditions.checkState(fjm != null,
        "must init() first");
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
    this.resultCode  = rc;
    if (rpcServer != null) { 
      rpcServer.stop();
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

  private File getLogDir() {
    String dir = conf.get(DFS_JOURNALNODE_DIR_KEY,
        DFS_JOURNALNODE_DIR_DEFAULT).trim();
    return new File(dir);
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
