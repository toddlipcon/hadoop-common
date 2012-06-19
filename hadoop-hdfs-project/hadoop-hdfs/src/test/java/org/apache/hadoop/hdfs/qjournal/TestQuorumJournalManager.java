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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Functional tests for QuorumJournalManager.
 * For true unit tests, see {@link TestQuorumJournalManagerUnit}.
 */
public class TestQuorumJournalManager {
  private static final String JID = "testQuorumJournalManager";
  private MiniJournalCluster cluster;
  private Configuration conf;

  @Before
  public void setup() throws Exception {
    conf = new Configuration();
    cluster = new MiniJournalCluster.Builder(conf)
      .build();
    cluster.start();
    cluster.setupClientConfigs(conf);
  }
  
  @After
  public void shutdown() throws IOException {
    cluster.shutdown();
  }
  
  @Test
  public void testSingleWriter() throws Exception {
    QuorumJournalManager qjm = new QuorumJournalManager(
        conf, new URI("qjournal://x/" + JID));
    qjm.recoverUnfinalizedSegments();
    assertEquals(1, qjm.getWriterEpoch());

    writeSegment(qjm, 1, 3, true);
    
    // Should be finalized
    assertExistsInQuorum(cluster,
        NNStorage.getFinalizedEditsFileName(1, 3));
    
    // Start a new segment
    writeSegment(qjm, 4, 1, true);
    assertExistsInQuorum(cluster,
        NNStorage.getFinalizedEditsFileName(4, 4));
  }
  
  /**
   * Test case where a new writer picks up from an old one with no failures
   * and the previous unfinalized segment entirely consistent -- i.e. all
   * the JournalNodes end at the same transaction ID.
   */
  @Test
  public void testChangeWritersLogsInSync() throws Exception {
    QuorumJournalManager qjm = new QuorumJournalManager(
        conf, new URI("qjournal://x/" + JID));
    qjm.recoverUnfinalizedSegments();
    writeSegment(qjm, 1, 3, false);
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(1));

    // Make a new QJM
    qjm = new QuorumJournalManager(
        conf, new URI("qjournal://x/" + JID));
    qjm.recoverUnfinalizedSegments();
    assertExistsInQuorum(cluster,
        NNStorage.getFinalizedEditsFileName(1, 3));
  }
  
  private void writeSegment(QuorumJournalManager qjm,
      int startTxId, int numTxns, boolean finalize) throws IOException {
    
    EditLogOutputStream stm = qjm.startLogSegment(startTxId);
    // Should create in-progress
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(startTxId));

    for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
      TestQuorumJournalManagerUnit.writeOp(stm, txid);
    }
    stm.setReadyToFlush();
    stm.flush();
    stm.close();
    if (finalize) {
      qjm.finalizeLogSegment(startTxId, startTxId + numTxns - 1);
    }
  }

  private void assertExistsInQuorum(MiniJournalCluster cluster,
      String fname) {
    int count = 0;
    for (int i = 0; i < 3; i++) {
      File dir = cluster.getCurrentDir(i, JID);
      if (new File(dir, fname).exists()) {
        count++;
      }
    }
    assertTrue("File " + fname + " should exist in a quorum of dirs",
        count > 2);
  }
}
