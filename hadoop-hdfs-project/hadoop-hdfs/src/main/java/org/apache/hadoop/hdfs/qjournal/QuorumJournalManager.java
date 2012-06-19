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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.namenode.LocalOrRemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.net.NetUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

/**
 * A JournalManager that writes to a set of remote JournalNodes,
 * requiring a quorum of nodes to ack each write.
 */
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager {
  static final Log LOG = LogFactory.getLog(QuorumJournalManager.class);

  static final String LOGGER_ADDRESSES_KEY =
    "dfs.namenode.quorum-logger.logger-addresess";


  private static final int START_SEGMENT_TIMEOUT_MS = 20000;

  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private String journalId;
  private boolean isActiveWriter;
  
  private final AsyncLoggerSet loggers;

  

  
  public QuorumJournalManager(Configuration conf,
      URI uri) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    String path = uri.getPath();
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "Bad URI '%s': must identify journal in path component",
        uri);
    this.journalId = path.substring(1);
    checkJournalId(journalId);
    
    // TODO: need to plumb namespace info in here from NN/Storage
    this.nsInfo = new NamespaceInfo(12345, "fake-cluster", "fake-bp", 1L, 1);
    
    this.loggers = new AsyncLoggerSet(createLoggers());
  }
  
  static void checkJournalId(String jid) {
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty() &&
        !jid.contains("/") &&
        !jid.startsWith("."),
        "bad journal id: " + jid);
  }

  private synchronized void becomeActiveWriter() throws IOException {
    Preconditions.checkState(!isActiveWriter, "already active writer");

    Map<AsyncLogger, NewEpochResponseProto> resps =
        loggers.createNewUniqueEpoch(nsInfo);
    LOG.info("newEpoch(" + getWriterEpoch() + ") responses:\n" +
        Joiner.on("\n").withKeyValueSeparator(": ").join(resps));

    Entry<AsyncLogger, NewEpochResponseProto> newestLogger = Collections.max(
        resps.entrySet(), RecoveryComparator.INSTANCE);
    
    LOG.info("Newest logger: " + newestLogger);
    for (Entry<AsyncLogger, NewEpochResponseProto> resp : resps.entrySet()) {
      if (RecoveryComparator.INSTANCE.compare(resp, newestLogger) < 0) {
        LOG.info("Older logger needs sync: " + resp);
        throw new UnsupportedOperationException("TODO");
      }
    }
    
    isActiveWriter = true;
  }
  
  private static class RecoveryComparator
      implements Comparator<Map.Entry<AsyncLogger, NewEpochResponseProto>> {
    private static final RecoveryComparator INSTANCE =
        new RecoveryComparator();
    
    @Override
    public int compare(
        Entry<AsyncLogger, NewEpochResponseProto> a,
        Entry<AsyncLogger, NewEpochResponseProto> b) {
      
      NewEpochResponseProto r1 = a.getValue();
      NewEpochResponseProto r2 = b.getValue();
      
      return ComparisonChain.start()
          .compare(r1.getCurrentEpoch(), r2.getCurrentEpoch())
          .compare(r1.getLastSegment().getEndTxId(),
                   r2.getLastSegment().getEndTxId())
          .result();
    }
  }
  

  long getWriterEpoch() {
    return loggers.getEpoch();
  }

  protected List<AsyncLogger> createLoggers() throws IOException {
    return createLoggers(conf, journalId);
  }
  
  static List<AsyncLogger> createLoggers(Configuration conf,
      String journalId) throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = getLoggerAddresses(conf);
    for (InetSocketAddress addr : addrs) {
      ret.add(new IPCLoggerChannel(conf, journalId, addr));
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
    Preconditions.checkState(isActiveWriter,
        "must recover segments before starting a new one");
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
  public void setOutputBufferCapacity(int size) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    becomeActiveWriter();
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxnId, boolean inProgressOk) {

    QuorumCall<AsyncLogger,GetEditLogManifestResponseProto> q =
        loggers.getEditLogManifest(fromTxnId);
    Map<AsyncLogger, GetEditLogManifestResponseProto> resps;
    try {
      resps = loggers.waitForWriteQuorum(q, START_SEGMENT_TIMEOUT_MS);
    } catch (IOException ioe) {
      // TODO: can we do better here?
      throw new RuntimeException(ioe);
    }
    
    LOG.info("selectInputStream manifests:\n" +
        Joiner.on("\n").withKeyValueSeparator(": ").join(resps));
    
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            JournalSet.EDIT_LOG_INPUT_STREAM_COMPARATOR);
    for (Map.Entry<AsyncLogger, GetEditLogManifestResponseProto> e : resps.entrySet()) {
      AsyncLogger logger = e.getKey();
      GetEditLogManifestResponseProto response = e.getValue();
      RemoteEditLogManifest manifest = PBHelper.convert(response.getManifest());
      
      for (RemoteEditLog remoteLog : manifest.getLogs()) {
        URL url;
        try {
          url = new URL("http",
              logger.getHostNameForHttpFetch(),
              response.getHttpPort(),
              String.format("/getimage?startTxId=%d&endTxId=%d&jid=%s",
                  remoteLog.getStartTxId(),
                  remoteLog.getEndTxId(),
                  journalId));
        } catch (MalformedURLException e1) {
          // should never get here
          throw new RuntimeException(e1);
        }
        // TODO: refactor above mess out
        LOG.info("URL: " + url);
                        
        EditLogInputStream elis = new EditLogFileInputStream(
            new LocalOrRemoteEditLog.URLLog(url),
            remoteLog.getStartTxId(), remoteLog.getEndTxId(),
            false); // TODO inprogress
        allStreams.add(elis);
      }
    }
    JournalSet.chainAndMakeRedundantStreams(
        streams, allStreams, fromTxnId, inProgressOk);
  }
  
  @Override
  public String toString() {
    return "Quorum journal manager " + uri;
  }

}
