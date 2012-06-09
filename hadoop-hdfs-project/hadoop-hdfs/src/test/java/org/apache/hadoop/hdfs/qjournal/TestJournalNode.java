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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;


public class TestJournalNode {
  private JournalNode jn = new JournalNode();
  private Configuration conf = new Configuration();

  @Before
  public void setup() throws Exception {
    conf.set(JournalNodeRpcServer.DFS_JOURNALNODE_RPC_ADDRESS_KEY,
        "0.0.0.0:0");
    jn.setConf(conf);
    jn.init();
    jn.start();
  }
  
  @After
  public void teardown() throws Exception {
    jn.stop(0);
  }
  
  @Test
  public void testJournal() throws Exception {
    IPCLoggerChannel ch = new IPCLoggerChannel(conf, jn.getBoundAddress());
    ch.setEpoch(1);
    ch.startLogSegment(1).get();
    ch.sendEdits(1, 1, "hello".getBytes(Charsets.UTF_8)).get();
  }
  
  @Test
  public void testEpochHandling() throws Exception {
    assertEquals(0, jn.getLastPromisedEpoch());
    jn.newEpoch(1);
    assertEquals(1, jn.getLastPromisedEpoch());
    jn.newEpoch(3);
    assertEquals(3, jn.getLastPromisedEpoch());
    try {
      NewEpochResponseProto newEpoch = jn.newEpoch(3);
      // TODO: maybe this should be returning a different value
      // to indicate no segment?
      assertEquals(HdfsConstants.INVALID_TXID,
          newEpoch.getLastSegment().getStartTxId());
      fail("Should have failed to promise same epoch twice");
    } catch (IOException ioe) {
      // expected
    }
    try {
      jn.startLogSegment(new RequestInfo(1L, 1L), 12345L);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
    try {
      jn.journal(new RequestInfo(1L, 1L), 100L, 0, new byte[0]);
      fail("Should have rejected call from prior epoch");
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "epoch 1 is less than the last promised epoch 3", ioe);
    }
  }
  
  @Test
  public void testReturnsSegmentInfoAtEpochTransition() throws Exception {
    IPCLoggerChannel ch = new IPCLoggerChannel(conf, jn.getBoundAddress());
    ch.newEpoch(1).get();
    ch.setEpoch(1);
    ch.startLogSegment(1).get();
    ch.sendEdits(1, 2, "hello".getBytes(Charsets.UTF_8)).get();
    
    // Switch to a new epoch without closing earlier segment
    NewEpochResponseProto response = ch.newEpoch(2).get();
    ch.setEpoch(2);
    assertEquals(1, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertTrue(response.getLastSegment().getIsInProgress());
    
    ch.finalizeLogSegment(1, 2).get();
    
    // Switch to a new epoch after just closing the earlier segment.
    // The response should now indicate the segment is closed.
    response = ch.newEpoch(3).get();
    ch.setEpoch(3);
    assertEquals(1, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertFalse(response.getLastSegment().getIsInProgress());
    
    // Start a segment but don't write anything, check newEpoch segment info
    ch.startLogSegment(3).get();
    response = ch.newEpoch(4).get();
    ch.setEpoch(4);
    assertEquals(3, response.getLastSegment().getStartTxId());
    assertEquals(2, response.getLastSegment().getEndTxId());
    assertTrue(response.getLastSegment().getIsInProgress());
  }
}
