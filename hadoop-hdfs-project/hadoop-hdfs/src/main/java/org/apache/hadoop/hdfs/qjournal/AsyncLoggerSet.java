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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Wrapper around a set of Loggers, taking care of fanning out
 * calls to the underlying loggers and constructing corresponding
 * {@link QuorumCall} instances.
 */
class AsyncLoggerSet {
  static final Log LOG = LogFactory.getLog(AsyncLoggerSet.class);

  private static final int NEWEPOCH_TIMEOUT_MS = 10000;
  
  private final List<AsyncLogger> loggers;
  
  private static final long INVALID_EPOCH = -1;
  private long myEpoch = INVALID_EPOCH;
  
  public AsyncLoggerSet(List<AsyncLogger> loggers) {
    this.loggers = ImmutableList.copyOf(loggers);
  }
  
  /**
   * Fence any previous writers, and obtain a unique epoch number
   * for write-access to the journal nodes.
   * @return the epoch number
   * @throws IOException
   */
  Map<AsyncLogger, NewEpochResponseProto> createNewUniqueEpoch(
      NamespaceInfo nsInfo) throws IOException {
    Preconditions.checkState(myEpoch == -1,
        "epoch already created: epoch=" + myEpoch);
    
    Map<AsyncLogger, GetEpochInfoResponseProto> lastPromises =
      waitForWriteQuorum(getEpochInfo(), NEWEPOCH_TIMEOUT_MS);
    
    long maxPromised = Long.MIN_VALUE;
    for (GetEpochInfoResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    
    long myEpoch = maxPromised + 1;
    Map<AsyncLogger, NewEpochResponseProto> resps =
        waitForWriteQuorum(newEpoch(nsInfo, myEpoch), NEWEPOCH_TIMEOUT_MS);
    this.myEpoch = myEpoch;
    setEpoch(myEpoch);
    return resps;
  }
  
  
  Map<AsyncLogger, PaxosPrepareResponseProto> prepareRecovery(
      long segmentTxId) throws IOException {
    QuorumCall<AsyncLogger,PaxosPrepareResponseProto> prepare =
        paxosPrepare(segmentTxId);
    Map<AsyncLogger, PaxosPrepareResponseProto> resps=
        waitForWriteQuorum(prepare, NEWEPOCH_TIMEOUT_MS);
    return resps;
  }
  
  void acceptRecovery(RemoteEditLogProto log, URL fromURL)
      throws IOException {
    // ================================================
    // Step 2. Accept.
    // ================================================

    QuorumCall<AsyncLogger,Void> accept = paxosAccept(log, fromURL);
    waitForWriteQuorum(accept, NEWEPOCH_TIMEOUT_MS);

    // TODO: some sanity-checks, eg if anyone returns an IllegalStateException
    // we should probably bail!
  }
  
  private void setEpoch(long e) {
    for (AsyncLogger l : loggers) {
      l.setEpoch(e);
    }
  }

  public long getEpoch() {
    Preconditions.checkState(myEpoch != INVALID_EPOCH,
        "No epoch created yet");
    return myEpoch;
  }


  /**
   * Wait for a quorum of loggers to respond to the given call. If a quorum
   * can't be achieved, throws a QuorumException.
   * @param q the quorum call
   * @param timeoutMs the number of millis to wait
   * @return a map of successful results
   * @throws QuorumException if a quorum doesn't respond with success
   * @throws IOException if the thread is interrupted or times out
   */
  <V> Map<AsyncLogger, V> waitForWriteQuorum(QuorumCall<AsyncLogger, V> q,
      int timeoutMs) throws IOException {
    int majority = getMajoritySize();
    try {
      q.waitFor(
          loggers.size(), // either all respond 
          majority, // or we get a majority successes
          majority, // or we get a majority failures,
          timeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for quorum results");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting " + timeoutMs + " for write quorum");
    }
    
    if (q.countSuccesses() < majority) {
      q.rethrowException("Got too many exceptions to achieve quorum size " +
          getMajorityString());
    }
    
    return q.getResults();
  }
  
  private QuorumCall<AsyncLogger, GetEpochInfoResponseProto> getEpochInfo() {
    Map<AsyncLogger, ListenableFuture<GetEpochInfoResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.getEpochInfo());
    }
    return QuorumCall.create(calls);    
  }

  private QuorumCall<AsyncLogger,NewEpochResponseProto> newEpoch(
      NamespaceInfo nsInfo,
      long epoch) {
    Map<AsyncLogger, ListenableFuture<NewEpochResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.newEpoch(nsInfo, epoch));
    }
    return QuorumCall.create(calls);    
  }

  public QuorumCall<AsyncLogger, Void> startLogSegment(
      long txid) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.startLogSegment(txid));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> finalizeLogSegment(long firstTxId,
      long lastTxId) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.finalizeLogSegment(firstTxId, lastTxId));
    }
    return QuorumCall.create(calls);
  }
  
  public QuorumCall<AsyncLogger, Void> sendEdits(
      long firstTxnId, int numTxns, byte[] data) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = 
        logger.sendEdits(firstTxnId, numTxns, data);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  public QuorumCall<AsyncLogger,GetEditLogManifestResponseProto>
      getEditLogManifest(long fromTxnId) {
    Map<AsyncLogger,
        ListenableFuture<GetEditLogManifestResponseProto>> calls
        = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<GetEditLogManifestResponseProto> future =
          logger.getEditLogManifest(fromTxnId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  

  private QuorumCall<AsyncLogger, PaxosPrepareResponseProto>
      paxosPrepare(long segmentTxId) {
    Map<AsyncLogger,
      ListenableFuture<PaxosPrepareResponseProto>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<PaxosPrepareResponseProto> future =
          logger.paxosPrepare(segmentTxId);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }

  private QuorumCall<AsyncLogger,Void>
      paxosAccept(RemoteEditLogProto log, URL fromURL) {
    // TODO: sanity check log txids
    
    Map<AsyncLogger, ListenableFuture<Void>> calls
      = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future =
          logger.paxosAccept(log, fromURL);
      calls.put(logger, future);
    }
    return QuorumCall.create(calls);
  }
  

  int getMajoritySize() {
    return loggers.size() / 2 + 1;
  }
  
  String getMajorityString() {
    return getMajoritySize() + "/" + loggers.size();
  }
  
  int size() {
    return loggers.size();
  }

  /**
   * @return the (mutable) list of loggers, for use in tests to
   * set up spies
   */
  @VisibleForTesting
  List<AsyncLogger> getLoggersForTests() {
    return loggers;
  }

}
