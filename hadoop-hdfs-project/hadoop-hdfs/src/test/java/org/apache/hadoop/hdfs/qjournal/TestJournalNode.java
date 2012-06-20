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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;


public class TestJournalNode {
  private static final NamespaceInfo FAKE_NSINFO = new NamespaceInfo(
      12345, "mycluster", "my-bp", 0L, 0);
  private static final String JID = "test-journalid";

  private static final String TEST_DECISION = "decision-1";
  private static final byte[] TEST_PAXOS_VALUE_1 = "val-1".getBytes();
  
  private JournalNode jn;
  private Journal journal; 
  private Configuration conf = new Configuration();
  private IPCLoggerChannel ch;

  static {
    // Avoid an error when we double-initialize JvmMetrics
    DefaultMetricsSystem.setMiniClusterMode(true);
  }
  
  @Before
  public void setup() throws Exception {
    conf.set(JournalNodeRpcServer.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        "0.0.0.0:0");
    jn = new JournalNode();
    jn.setConf(conf);
    jn.start();
    journal = jn.getOrCreateJournal(JID);
    // TODO: this should not have to be done explicitly in the unit
    // test, really, I dont think..
    journal.format();
    
    ch = new IPCLoggerChannel(conf, JID, jn.getBoundIpcAddress());
  }
  
  @After
  public void teardown() throws Exception {
    jn.stop(0);
  }
  
  @Test
  public void testJournal() throws Exception {
    IPCLoggerChannel ch = new IPCLoggerChannel(
        conf, JID, jn.getBoundIpcAddress());
    ch.newEpoch(FAKE_NSINFO, 1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1).get();
    ch.sendEdits(1, 1, "hello".getBytes(Charsets.UTF_8)).get();
  }
  
  @Test
  public void testEpochHandling() throws Exception {
    assertEquals(0, journal.getLastPromisedEpoch());
    NewEpochResponseProto.Builder newEpoch =
        journal.newEpoch(FAKE_NSINFO, 1);
    assertFalse(newEpoch.hasLastSegment());
    assertEquals(1, journal.getLastPromisedEpoch());
    journal.newEpoch(FAKE_NSINFO, 3);
    assertFalse(newEpoch.hasLastSegment());
    assertEquals(3, journal.getLastPromisedEpoch());
    try {
      journal.newEpoch(
          FAKE_NSINFO, 3);
      fail("Should have failed to promise same epoch twice");
    } catch (IOException ioe) {
      // expected
    }
    try {
      journal.startLogSegment(new RequestInfo(JID, 1L, 1L),
          12345L);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
    try {
      journal.journal(new RequestInfo(JID, 1L, 1L),
          100L, 0, new byte[0]);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
  }
  
  @Test
  public void testReturnsSegmentInfoAtEpochTransition() throws Exception {
    ch.newEpoch(FAKE_NSINFO, 1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1).get();
    ch.sendEdits(1, 2, "hello".getBytes(Charsets.UTF_8)).get();
    
    // Switch to a new epoch without closing earlier segment
    NewEpochResponseProto response = ch.newEpoch(
        FAKE_NSINFO, 2).get();
    ch.setEpoch(2);
    assertEquals(1, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertTrue(response.getLastSegment().getIsInProgress());
    
    ch.finalizeLogSegment(1, 2).get();
    
    // Switch to a new epoch after just closing the earlier segment.
    // The response should now indicate the segment is closed.
    response = ch.newEpoch(FAKE_NSINFO, 3).get();
    ch.setEpoch(3);
    assertEquals(1, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertFalse(response.getLastSegment().getIsInProgress());
    
    // Start a segment but don't write anything, check newEpoch segment info
    ch.startLogSegment(3).get();
    response = ch.newEpoch(FAKE_NSINFO, 4).get();
    ch.setEpoch(4);
    assertEquals(3, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertTrue(response.getLastSegment().getIsInProgress());
  }
  
  @Test
  public void testHttpServer() throws Exception {
    InetSocketAddress addr = jn.getBoundHttpAddress();
    assertTrue(addr.getPort() > 0);
    
    String urlRoot = "http://localhost:" + addr.getPort();
    
    String pageContents = DFSTestUtil.urlGet(new URL(urlRoot + "/jmx"));
    assertTrue("Bad contents: " + pageContents,
        pageContents.contains(
            "Hadoop:service=JournalNode,name=JvmMetrics"));
    
    // Create some edits on server side
    IPCLoggerChannel ch = new IPCLoggerChannel(
        conf, JID, jn.getBoundIpcAddress());
    ch.newEpoch(FAKE_NSINFO, 1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1).get();
    byte[] EDITS_DATA = "hello".getBytes(Charsets.UTF_8);
    ch.sendEdits(1, 3, EDITS_DATA).get();
    ch.finalizeLogSegment(1, 3).get();

    // Attempt to retrieve via HTTP, ensure we get the data back
    // including the header we expected
    byte[] retrievedViaHttp = DFSTestUtil.urlGetBytes(new URL(urlRoot +
        "/getimage?filename=" + NNStorage.getFinalizedEditsFileName(1, 3) +
        "&jid=" + JID));
    byte[] expected = Bytes.concat(
            Ints.toByteArray(HdfsConstants.LAYOUT_VERSION),
            EDITS_DATA);

    assertArrayEquals(expected, retrievedViaHttp);
  }

  /**
   * Test that the JournalNode performs correctly as a Paxos
   * <em>Acceptor</em> process.
   * 
   * @throws Exception
   */
  @Test
  public void testPaxosAcceptorBehavior() throws Exception {
    // We need to run newEpoch() first, or else we have no way to distinguish
    // different proposals for the same decision.
    try {
      ch.paxosPrepare("decision-1").get();
      fail("Did not throw IllegalState when trying to run paxos without an epoch");
    } catch (ExecutionException ise) {
      GenericTestUtils.assertExceptionContains("bad epoch", ise);
    }
    
    ch.newEpoch(FAKE_NSINFO, 1).get();
    ch.setEpoch(1);
    
    // prepare() with no previously accepted value returns an empty protobuf
    PaxosPrepareResponseProto prep = ch.paxosPrepare("decision-1").get();
    System.err.println("Prep: " + prep);
    assertFalse(prep.hasAcceptedEpoch());
    assertFalse(prep.hasAcceptedValue());
    
    // accept() should save the accepted value in persistent storage
    ch.paxosAccept(TEST_DECISION, TEST_PAXOS_VALUE_1).get();

    // So another prepare() call from a new epoch would return this value
    ch.newEpoch(FAKE_NSINFO, 2);
    ch.setEpoch(2);
    prep = ch.paxosPrepare("decision-1").get();
    assertEquals(1, prep.getAcceptedEpoch());
    assertArrayEquals(TEST_PAXOS_VALUE_1, prep.getAcceptedValue().toByteArray());
    
    // A prepare() or accept() call from an earlier epoch should now be rejected
    ch.setEpoch(1);
    try {
      ch.paxosPrepare(TEST_DECISION).get();
      fail("prepare from earlier epoch not rejected");
    } catch (ExecutionException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 2",
          ioe);
    }
    try {
      ch.paxosAccept(TEST_DECISION, new byte[0]).get();
      fail("accept from earlier epoch not rejected");
    } catch (ExecutionException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 2",
          ioe);
    }
  }
  
  @Test
  public void testSyncLog() throws Exception {
    // TODO ch.syncLog(1, new URL("http://google.com/")).get();
  }
  
  // TODO:
  // - add test that checks formatting behavior
  // - add test that checks rejects newEpoch if nsinfo doesn't match
  
}
