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
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
class IPCLoggerChannel implements AsyncLogger {

  private final Configuration conf;
  private final InetSocketAddress addr;
  private QJournalProtocol proxy;
  //private JournalSyncProtocol syncProxy; TODO
  
  private final ListeningExecutorService executor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private final String journalId;
  
  public IPCLoggerChannel(Configuration conf,
      String journalId,
      InetSocketAddress addr) {
    this.conf = conf;
    this.journalId = journalId;
    this.addr = addr;
    executor = MoreExecutors.listeningDecorator(
        Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Logger channel to " + addr)
            .setUncaughtExceptionHandler(
                UncaughtExceptionHandlers.systemExit())
            .build()));
  }
  
  @Override
  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  private QJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;

    RPC.setProtocolEngine(conf,
        QJournalProtocolPB.class, ProtobufRpcEngine.class);
    QJournalProtocolPB pbproxy = RPC.getProxy(
        QJournalProtocolPB.class,
        RPC.getProtocolVersion(QJournalProtocolPB.class),
        addr, conf);
    return new QJournalProtocolTranslatorPB(pbproxy);
  }
  
  @Override
  public String getHostNameForHttpFetch() {
    return addr.getHostName();
  }

  private RequestInfo createReqInfo() {
    Preconditions.checkState(epoch > 0, "bad epoch: " + epoch);
    return new RequestInfo(journalId, epoch, ipcSerial++);
  }


  @Override
  public ListenableFuture<GetEpochInfoResponseProto> getEpochInfo() {
    return executor.submit(new Callable<GetEpochInfoResponseProto>() {
      @Override
      public GetEpochInfoResponseProto call() throws IOException {
        return getProxy().getEpochInfo(journalId);
      }
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final NamespaceInfo nsInfo,
      final long epoch) {
    return executor.submit(new Callable<NewEpochResponseProto>() {
      @Override
      public NewEpochResponseProto call() throws IOException {
        return getProxy().newEpoch(journalId, nsInfo, epoch);
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long firstTxnId, final int numTxns, final byte[] data) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().journal(createReqInfo(), firstTxnId, numTxns, data);
        return null;
      }
    });
  }

  @Override
  public ListenableFuture<Void> startLogSegment(final long txid) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().startLogSegment(createReqInfo(), txid);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<Void> finalizeLogSegment(
      final long startTxId, final long endTxId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().finalizeLogSegment(createReqInfo(),
            startTxId, endTxId);
        return null;
      }
    });
  }
  
  @Override
  public ListenableFuture<GetEditLogManifestResponseProto> getEditLogManifest(
      final long fromTxnId) {
    return executor.submit(new Callable<GetEditLogManifestResponseProto>() {
      @Override
      public GetEditLogManifestResponseProto call() throws IOException {
        return getProxy().getEditLogManifest(
            journalId,
            fromTxnId);
      }
    });
  }

  @Override
  public ListenableFuture<PaxosPrepareResponseProto> paxosPrepare(
      final String decisionId) {
    return executor.submit(new Callable<PaxosPrepareResponseProto>() {
      @Override
      public PaxosPrepareResponseProto call() throws IOException {
        return getProxy().paxosPrepare(createReqInfo(), decisionId);
      }
    });
  }

  @Override
  public ListenableFuture<Void> paxosAccept(final String decisionId,
      final byte[] value) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws IOException {
        getProxy().paxosAccept(createReqInfo(), decisionId, value);
        return null;
      }
    });
  }

  @Override
  public String toString() {
    return "Channel to journal node " + addr; 
  }
}
