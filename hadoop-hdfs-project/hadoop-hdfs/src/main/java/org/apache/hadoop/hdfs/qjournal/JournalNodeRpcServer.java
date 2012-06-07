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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.QJournalProtocolService;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;

import com.google.protobuf.BlockingService;

class JournalNodeRpcServer implements QJournalProtocol {

  static final String DFS_JOURNALNODE_RPC_ADDRESS_KEY = "dfs.journalnode.rpc-address";
  static final int DEFAULT_PORT = 8485;

  private static final String DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT = "0.0.0.0:" + DEFAULT_PORT;
  private static final int HANDLER_COUNT = 5;
  private JournalNode jn;
  private Server server;

  JournalNodeRpcServer(Configuration conf, JournalNode jn) throws IOException {
    this.jn = jn;
    
    InetSocketAddress addr = getAddress(conf);
    RPC.setProtocolEngine(conf, QJournalProtocolPB.class,
        ProtobufRpcEngine.class);
    QJournalProtocolServerSideTranslatorPB translator =
        new QJournalProtocolServerSideTranslatorPB(this);
    BlockingService service = QJournalProtocolService
        .newReflectiveBlockingService(translator);
    this.server = RPC.getServer(
        QJournalProtocolPB.class,
        service, addr.getHostName(),
            addr.getPort(), HANDLER_COUNT, false, conf,
            null /*secretManager*/);
  }

  void start() {
    this.server.start();
  }

  public InetSocketAddress getAddress() {
    return server.getListenerAddress();
  }
  
  void join() throws InterruptedException {
    this.server.join();
  }
  
  void stop() {
    this.server.stop();
  }
  
  private static InetSocketAddress getAddress(Configuration conf) {
    String addr = conf.get(DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        DFS_JOURNALNODE_RPC_ADDRESS_DEFAULT);
    return NetUtils.createSocketAddr(addr, DEFAULT_PORT,
        DFS_JOURNALNODE_RPC_ADDRESS_KEY);
  }

  @Override
  public GetEpochInfoResponseProto getEpochInfo() throws IOException {
    return jn.getEpochInfo();
  }

  @Override
  public NewEpochResponseProto newEpoch(long epoch) throws IOException {
    return jn.newEpoch(epoch);
  }


  @Override
  public void journal(RequestInfo reqInfo, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    jn.journal(reqInfo, firstTxnId, numTxns, records);
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    jn.startLogSegment(reqInfo, txid);
  }

  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    jn.finalizeLogSegment(reqInfo, startTxId, endTxId);
  }
}
