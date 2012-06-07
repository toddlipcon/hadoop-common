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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;

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
  
  public AsyncLoggerSet(List<AsyncLogger> loggers) {
    this.loggers = ImmutableList.copyOf(loggers);
  }
  
  long createNewUniqueEpoch() throws IOException {
    Map<AsyncLogger, GetEpochInfoResponseProto> lastPromises =
      waitForWriteQuorum(getEpochInfo(), NEWEPOCH_TIMEOUT_MS);
    
    long maxPromised = Long.MIN_VALUE;
    for (GetEpochInfoResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    
    long myEpoch = maxPromised + 1;
    waitForWriteQuorum(newEpoch(myEpoch), NEWEPOCH_TIMEOUT_MS);
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

  private QuorumCall<AsyncLogger,NewEpochResponseProto> newEpoch(long epoch) {
    Map<AsyncLogger, ListenableFuture<NewEpochResponseProto>> calls =
        Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      calls.put(logger, logger.newEpoch(epoch));
    }
    return QuorumCall.create(calls);    
  }

  QuorumCall<AsyncLogger, Void> startLogSegment(
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
  
  QuorumCall<AsyncLogger, Void> sendEdits(
      long firstTxnId, int numTxns, byte[] data) {
    Map<AsyncLogger, ListenableFuture<Void>> calls = Maps.newHashMap();
    for (AsyncLogger logger : loggers) {
      ListenableFuture<Void> future = 
        logger.sendEdits(firstTxnId, numTxns, data);
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

  public void setEpoch(long e) {
    for (AsyncLogger l : loggers) {
      l.setEpoch(e);
    }
  }
}
