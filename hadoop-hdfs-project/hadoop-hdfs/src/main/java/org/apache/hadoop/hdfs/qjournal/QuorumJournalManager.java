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
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.namenode.LocalOrRemoteEditLog;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
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

    long mostRecentSegmentTxId = Long.MIN_VALUE;
    for (NewEpochResponseProto r : resps.values()) {
      if (r.hasCurSegmentTxId()) {
        mostRecentSegmentTxId = Math.max(mostRecentSegmentTxId,
            r.getCurSegmentTxId());
      }
    }

    // On a completely fresh system, none of the journals have any
    // segments, so there's nothing to recover.
    if (mostRecentSegmentTxId != Long.MIN_VALUE) {
      recoverUnclosedSegment(mostRecentSegmentTxId);
    }
    isActiveWriter = true;
  }
  
  private void recoverUnclosedSegment(long segmentTxId) throws IOException {
    Preconditions.checkArgument(segmentTxId > 0);

    LOG.info("Beginning recovery of unclosed segment starting at txid " +
        segmentTxId);
    
    Map<AsyncLogger, PaxosPrepareResponseProto> prepareResponses =
        loggers.prepareRecovery(segmentTxId);
    
    Entry<AsyncLogger, PaxosPrepareResponseProto> bestEntry = Collections.max(
        prepareResponses.entrySet(), RECOVERY_COMPARATOR); 

    AsyncLogger bestLogger = bestEntry.getKey();
    PaxosPrepareResponseProto bestResponse = bestEntry.getValue();
    
    if (bestResponse.hasAcceptedRecovery()) {
      LOG.info("Using already-accepted recovery for segment " +
          "starting at txid " + segmentTxId + ": " + bestEntry);
    } else if (bestResponse.hasSegmentInfo()) {
      LOG.info("Using longest log: " + bestEntry);
    } else {
      throw new AssertionError("Invariant violated! None of the responses " +
          "had a log to recover!\n" +
          Joiner.on("\n").withKeyValueSeparator(": ").join(prepareResponses));
    }
    
    RemoteEditLogProto logToSync = bestResponse.getSegmentInfo();
    URL syncFromUrl = buildURLToFetchLogs(
        bestLogger.getHostNameForHttpFetch(),
        bestResponse.getHttpPort(),
        PBHelper.convert(bestResponse.getSegmentInfo()));
    
    assert segmentTxId == logToSync.getStartTxId();
    loggers.acceptRecovery(logToSync, syncFromUrl);
    
    // Write a test case for this condition, which I think should work now:
    // edit lengths [3,4,5]
    // first recovery:
    // - sees [3,4,x]
    // - picks length 4 for recoveryEndTxId
    // - syncLog() up to 4
    // - runs paxos, commits length 4
    // - crashes before finalizing
    // second recovery:
    // - sees [x, 4, 5]
    // - picks length 5 for recoveryEndTxId
    // - syncLog() up to 5
    // - runs paxos, sees already-committed value 4
    //
    

    // TODO:
    // we should only try to finalize loggers who successfully synced above
    // eg if a logger was down, we don't want to send the finalize request.
    // write a test for this!
    
    QuorumCall<AsyncLogger, Void> finalize =
        loggers.finalizeLogSegment(logToSync.getStartTxId(), logToSync.getEndTxId()); 
    loggers.waitForWriteQuorum(finalize, 20000); // TODO: timeout configurable
  }
  
  private static final Comparator<Entry<AsyncLogger, PaxosPrepareResponseProto>> RECOVERY_COMPARATOR =
  new Comparator<Entry<AsyncLogger, PaxosPrepareResponseProto>>() {
      @Override
      public int compare(
          Entry<AsyncLogger, PaxosPrepareResponseProto> a,
          Entry<AsyncLogger, PaxosPrepareResponseProto> b) {
        
        PaxosPrepareResponseProto r1 = a.getValue();
        PaxosPrepareResponseProto r2 = b.getValue();
        
        if (r1.hasSegmentInfo() && r2.hasSegmentInfo()) {
          assert r1.getSegmentInfo().getStartTxId() ==
              r2.getSegmentInfo().getStartTxId() : "bad args: " + r1 + ", " + r2;
        }
        
        return ComparisonChain.start()
            // If one of them has accepted something and the other hasn't,
            // use the one with an accepted recovery
            .compare(r1.hasAcceptedRecovery(), r2.hasAcceptedRecovery())
            // If they both accepted, use the one that's more recent
            .compare(r1.getAcceptedRecovery().getAcceptedInEpoch(),
                     r2.getAcceptedRecovery().getAcceptedInEpoch())
            // Otherwise, choose based on which log is longer
            .compare(r1.hasSegmentInfo(), r2.hasSegmentInfo())
            .compare(r1.getSegmentInfo().getEndTxId(), r2.getSegmentInfo().getEndTxId())
            .result();
      }
  };

  long getWriterEpoch() {
    return loggers.getEpoch();
  }

  protected List<AsyncLogger> createLoggers() throws IOException {
    return createLoggers(conf, uri, journalId);
  }
  
  static List<AsyncLogger> createLoggers(Configuration conf,
      URI uri, String journalId) throws IOException {
    List<AsyncLogger> ret = Lists.newArrayList();
    List<InetSocketAddress> addrs = getLoggerAddresses(uri);
    for (InetSocketAddress addr : addrs) {
      ret.add(new IPCLoggerChannel(conf, journalId, addr));
    }
    return ret;
  }
 
  private static List<InetSocketAddress> getLoggerAddresses(URI uri)
      throws IOException {
    String authority = uri.getAuthority();
    Preconditions.checkArgument(authority != null && !authority.isEmpty(),
        "URI has no authority: " + uri);
    
    String[] parts = StringUtils.split(authority, ';');
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }

    if (parts.length % 2 == 0) {
      LOG.warn("Quorum journal URI '" + uri + "' has an even number " +
          "of Journal Nodes specified. This is not recommended!");
    }
    
    List<InetSocketAddress> addrs = Lists.newArrayList();
    for (String addr : parts) {
      addrs.add(NetUtils.createSocketAddr(
          addr, JournalNodeRpcServer.DEFAULT_PORT));
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
        URL url = buildURLToFetchLogs(logger.getHostNameForHttpFetch(),
            response.getHttpPort(), remoteLog);
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
  
  private URL buildURLToFetchLogs(String hostname, int httpPort,
      RemoteEditLog segment) {
    Preconditions.checkArgument(segment.getStartTxId() > 0 &&
        (segment.isInProgress() ||
            segment.getEndTxId() > 0),
        "Invalid segment: %s", segment);
        
    try {
      StringBuilder path = new StringBuilder("/getimage?");
      path.append("jid=").append(URLEncoder.encode(journalId, "UTF-8"));
      path.append("&filename=")
          .append(URLEncoder.encode(getLogFilename(segment), "UTF-8"));
      path.append("&storageinfo=")
          .append(URLEncoder.encode(nsInfo.toColonSeparatedString(), "UTF-8"));
      return new URL("http", hostname, httpPort, path.toString());
    } catch (MalformedURLException e) {
      // should never get here.
      throw new RuntimeException(e);
    } catch (UnsupportedEncodingException e) {
      // should never get here -- everyone supports UTF-8.
      throw new RuntimeException(e);
    }
  }

  private String getLogFilename(RemoteEditLog segment) {
    if (segment.isInProgress()) {
      return NNStorage.getInProgressEditsFileName(
          segment.getStartTxId());
    } else {
      return NNStorage.getFinalizedEditsFileName(
          segment.getStartTxId(), segment.getEndTxId());
    }
  }

  @Override
  public String toString() {
    return "Quorum journal manager " + uri;
  }

  @VisibleForTesting
  AsyncLoggerSet getLoggerSetForTests() {
    return loggers;
  }

}
