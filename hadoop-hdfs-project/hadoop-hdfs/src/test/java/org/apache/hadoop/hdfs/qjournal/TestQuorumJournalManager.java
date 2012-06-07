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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.junit.Test;

/**
 * Functional tests for QuorumJournalManager.
 * For true unit tests, see {@link TestQuorumJournalManagerUnit}.
 */
public class TestQuorumJournalManager {
  @Test
  public void testSingleWriter() throws IOException {
    Configuration conf = new Configuration();
    MiniJournalCluster cluster = new MiniJournalCluster.Builder(conf).build();
    cluster.start();
    try {
      cluster.setupClientConfigs(conf);
      QuorumJournalManager qjm = new QuorumJournalManager();
      qjm.setConf(conf);
      EditLogOutputStream stm = qjm.startLogSegment(1);
      // Should create in-progress
      assertExistsInQuorum(cluster,
          NNStorage.getInProgressEditsFileName(1));

      assertEquals(1, qjm.getWriterEpoch());
      TestQuorumJournalManagerUnit.writeOp(stm, 1);
      TestQuorumJournalManagerUnit.writeOp(stm, 2);
      TestQuorumJournalManagerUnit.writeOp(stm, 3);
      stm.setReadyToFlush();
      stm.flush();

      stm.close();
      qjm.finalizeLogSegment(1, 3);
      
      // Should be finalized
      assertExistsInQuorum(cluster,
          NNStorage.getFinalizedEditsFileName(1, 3));
      
      // Start a new segment
      stm = qjm.startLogSegment(4);
      assertExistsInQuorum(cluster,
          NNStorage.getInProgressEditsFileName(4));
      TestQuorumJournalManagerUnit.writeOp(stm, 4);
      stm.setReadyToFlush();
      stm.flush();

      stm.close();
      qjm.finalizeLogSegment(4, 4);
    } finally {
      cluster.shutdown();
    }
  }

  private void assertExistsInQuorum(MiniJournalCluster cluster,
      String fname) {
    int count = 0;
    for (int i = 0; i < 3; i++) {
      if (new File(cluster.getCurrentDir(i), fname).exists()) {
        count++;
      }
    }
    assertTrue("File " + fname + " should exist in a quorum of dirs",
        count > 2);
  }
}
