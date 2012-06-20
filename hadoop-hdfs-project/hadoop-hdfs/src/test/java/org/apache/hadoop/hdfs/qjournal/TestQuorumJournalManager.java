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
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

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
  }
  
  @After
  public void shutdown() throws IOException {
    cluster.shutdown();
  }
  
  @Test
  public void testSingleWriter() throws Exception {
    QuorumJournalManager qjm = new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID));
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
        conf, cluster.getQuorumJournalURI(JID));
    qjm.recoverUnfinalizedSegments();
    writeSegment(qjm, 1, 3, false);
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(1));

    // Make a new QJM
    qjm = new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID));
    qjm.recoverUnfinalizedSegments();
    assertExistsInQuorum(cluster,
        NNStorage.getFinalizedEditsFileName(1, 3));
  }
  
  /**
   * Test case where a new writer picks up from an old one which crashed
   * with the three loggers at different txnids
   */
  @Test
  public void testChangeWritersLogsOutOfSync1() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [x, 4, 5]
    // Should recovery to txn 5
    doOutOfSyncTest(0, 5L);
  }

  @Test
  public void testChangeWritersLogsOutOfSync2() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [3, x, 5]
    // Should recovery to txn 5
    doOutOfSyncTest(1, 5L);
  }

  @Test
  public void testChangeWritersLogsOutOfSync3() throws Exception {
    // Journal states:  [3, 4, 5]
    // During recovery: [3, 4, x]
    // Should recovery to txn 4
    doOutOfSyncTest(2, 4L);
  }

  
  private void doOutOfSyncTest(int missingOnRecoveryIdx,
      long expectedRecoveryTxnId) throws Exception {
    QuorumJournalManager qjm = createSpyingQJM();
    List<AsyncLogger> spies = qjm.getLoggerSetForTests().getLoggersForTests();

    qjm.recoverUnfinalizedSegments();
    EditLogOutputStream stm = qjm.startLogSegment(1);
    
    TestQuorumJournalManagerUnit.futureThrows(new IOException("mock failure"))
      .when(spies.get(0)).sendEdits(
          Mockito.eq(4L), Mockito.eq(1), Mockito.<byte[]>any());
    TestQuorumJournalManagerUnit.futureThrows(new IOException("mock failure"))
      .when(spies.get(1)).sendEdits(
          Mockito.eq(5L), Mockito.eq(1), Mockito.<byte[]>any());

    writeTxns(stm, 1, 3);
    
    // This should succeed to 2/3 loggers
    writeTxns(stm, 4, 1);
    
    // This should only succeed to 1 logger (index 2). Hence it should
    // fail
    try {
      writeTxns(stm, 5, 1);
      fail("Did not fail to write when only a minority succeeded");
    } catch (QuorumException qe) {
      GenericTestUtils.assertExceptionContains(
          "too many exceptions to achieve quorum size 2/3",
          qe);
    }
    
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(1));

    // Shut down the specified JN, so it's not present during recovery.
    cluster.getJournalNode(missingOnRecoveryIdx).stop(0);

    // Make a new QJM
    qjm = createSpyingQJM();
    
    qjm.recoverUnfinalizedSegments();
    assertExistsInQuorum(cluster,
        NNStorage.getFinalizedEditsFileName(1, expectedRecoveryTxnId));
  }
  
  
  private QuorumJournalManager createSpyingQJM()
      throws IOException, URISyntaxException {
    return new QuorumJournalManager(
        conf, cluster.getQuorumJournalURI(JID)) {
          @Override
          protected List<AsyncLogger> createLoggers() throws IOException {
            LOG.info("===> make spies");
            List<AsyncLogger> realLoggers = super.createLoggers();
            List<AsyncLogger> spies = Lists.newArrayList();
            for (AsyncLogger logger : realLoggers) {
              spies.add(Mockito.spy(logger));
            }
            return spies;
          }
    };
  }

  private void writeSegment(QuorumJournalManager qjm,
      int startTxId, int numTxns, boolean finalize) throws IOException {
    EditLogOutputStream stm = qjm.startLogSegment(startTxId);
    // Should create in-progress
    assertExistsInQuorum(cluster,
        NNStorage.getInProgressEditsFileName(startTxId));
    
    writeTxns(stm, startTxId, numTxns);
    if (finalize) {
      stm.close();
      qjm.finalizeLogSegment(startTxId, startTxId + numTxns - 1);
    }
  }

  private void writeTxns(EditLogOutputStream stm, int startTxId, int numTxns)
      throws IOException {
    for (long txid = startTxId; txid < startTxId + numTxns; txid++) {
      TestQuorumJournalManagerUnit.writeOp(stm, txid);
    }
    stm.setReadyToFlush();
    stm.flush();
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
        count >= cluster.getQuorumSize());
  }
}
