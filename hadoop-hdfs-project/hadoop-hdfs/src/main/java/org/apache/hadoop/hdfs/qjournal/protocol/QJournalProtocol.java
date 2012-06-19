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
package org.apache.hadoop.hdfs.qjournal.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogsRequestProto;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to journal edits to a remote node. Currently,
 * this is used to publish edits from the NameNode to a BackupNode.
 */
@KerberosInfo(
    // TODO: 
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface QJournalProtocol {
  public static final long versionID = 1L;

  public GetEpochInfoResponseProto getEpochInfo(String journalId)
      throws IOException;
  public NewEpochResponseProto newEpoch(String journalId,
      NamespaceInfo nsInfo,
      long epoch) throws IOException;
  
  /**
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param registration active node registration
   * @param firstTxnId the first transaction of this batch
   * @param numTxns number of transactions
   * @param records byte array containing serialized journal records
   */
  public void journal(RequestInfo reqInfo,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * Notify the BackupNode that the NameNode has rolled its edit logs
   * and is now writing a new log segment.
   * @param registration the registration of the active NameNode
   * @param txid the first txid in the new log
   */
  public void startLogSegment(RequestInfo reqInfo,
      long txid) throws IOException;
  
  public void finalizeLogSegment(RequestInfo reqInfo,
      long startTxId, long endTxId) throws IOException;
  
  public void syncLogs(SyncLogsRequestProto req) throws IOException;
  
  GetEditLogManifestResponseProto getEditLogManifest(
      String jid, long sinceTxId) throws IOException;
  
  public PaxosPrepareResponseProto paxosPrepare(RequestInfo reqInfo,
      String decisionId) throws IOException;
  public void paxosAccept(RequestInfo reqInfo,
      String decisionId, byte[] value) throws IOException;
}
