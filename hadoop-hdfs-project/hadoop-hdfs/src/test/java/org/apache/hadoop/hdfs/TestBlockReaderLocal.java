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
package org.apache.hadoop.hdfs;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestBlockReaderLocal {
  public static void compareArrayRegion(byte []buf1, int off1, byte []buf2,
      int off2, int len) {
    for (int i = 0; i < len; i++) {
      if (buf1[off1 + i] != buf2[off2 + i]) {
        throw new RuntimeException("arrays differ at byte " +  i + ". " + 
          "The first array has " + (int)buf1[off1 + i] + 
          ", but the second array has " + (int)buf2[off2 + i]);
      }
    }
  }

  /**
   * Similar to IOUtils#readFully(). Reads bytes in a loop.
   *
   * @param reader           The BlockReaderLocal to read bytes from
   * @param buf              The ByteBuffer to read into
   * @param off              The offset in the buffer to read into
   * @param len              The number of bytes to read.
   * 
   * @throws IOException     If it could not read the requested number of bytes
   */
  private static void readFully(BlockReaderLocal reader,
      ByteBuffer buf, int off, int len) throws IOException {
    int amt = len;
    while (amt > 0) {
      buf.limit(off + len);
      buf.position(off);
      long ret = reader.read(buf);
      if (ret < 0) {
        throw new EOFException( "Premature EOF from BlockReaderLocal " +
            "after reading " + (len - amt) + " byte(s).");
      }
      amt -= ret;
      off += ret;
    }
  }

  private static interface BlockReaderLocalTest {
    final int TEST_LENGTH = 12345;
    public void setup(File blockFile, File checkFile) throws IOException;
    public void doTest(BlockReaderLocal reader, byte original[])
        throws IOException;
  }
  
  public void runBlockReaderLocalTest(BlockReaderLocalTest test,
      boolean checksum) throws IOException {
    MiniDFSCluster cluster = null;
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(DFSConfigKeys.DFS_CHECKSUM_TYPE_KEY, "CRC32C");
    FileInputStream dataIn = null, checkIn = null;
    final Path TEST_PATH = new Path("/a");
    final long RANDOM_SEED = 4567L;
    BlockReaderLocal blockReaderLocal = null;
    FSDataInputStream fsIn = null;
    byte original[] = new byte[BlockReaderLocalTest.TEST_LENGTH];
    
    try {
      cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, TEST_PATH,
          BlockReaderLocalTest.TEST_LENGTH, (short)1, RANDOM_SEED);
      try {
        DFSTestUtil.waitReplication(fs, TEST_PATH, (short)1);
      } catch (InterruptedException e) {
        Assert.fail("unexpected InterruptedException during " +
            "waitReplication: " + e);
      } catch (TimeoutException e) {
        Assert.fail("unexpected TimeoutException during " +
            "waitReplication: " + e);
      }
      fsIn = fs.open(TEST_PATH);
      IOUtils.readFully(fsIn, original, 0,
          BlockReaderLocalTest.TEST_LENGTH);
      fsIn.close();
      fsIn = null;
      ExtendedBlock block = DFSTestUtil.getFirstBlock(fs, TEST_PATH);
      File dataFile = MiniDFSCluster.getBlockFile(0, block);
      File metaFile = MiniDFSCluster.getBlockMetadataFile(0, block);
      cluster.shutdown();
      cluster = null;
      test.setup(dataFile, checksum ? metaFile : null);
      dataIn = new FileInputStream(dataFile);
      if (checksum) {
        checkIn = new FileInputStream(metaFile);
      }
      blockReaderLocal = 
          new BlockReaderLocal(dataIn, checkIn, TEST_PATH.getName(),
              null, null);
      dataIn = null;
      checkIn = null;
      test.doTest(blockReaderLocal, original);
    } finally {
      if (fsIn != null) fsIn.close();
      if (cluster != null) cluster.shutdown();
      if (dataIn != null) dataIn.close();
      if (checkIn != null) checkIn.close();
      if (blockReaderLocal != null) blockReaderLocal.close();
    }
  }
  
  private static class TestBlockReaderLocalImmediateClose 
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException { }
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException { }
  }
  
  @Test
  public void testBlockReaderLocalImmediateClose() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), true);
    runBlockReaderLocalTest(new TestBlockReaderLocalImmediateClose(), false);
  }
  
  private static class TestBlockReaderSimpleReads 
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException { }
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 512);
      compareArrayRegion(original, 0, buf, 0, 512);
      reader.readFully(buf, 512, 512);
      compareArrayRegion(original, 512, buf, 512, 512);
      reader.readFully(buf, 1024, 513);
      compareArrayRegion(original, 1024, buf, 1024, 513);
      reader.readFully(buf, 1537, 514);
      compareArrayRegion(original, 1537, buf, 1537, 514);
    }
  }
  
  @Test
  public void testBlockReaderSimpleReads() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), true);
  }

  @Test
  public void testBlockReaderSimpleReadsNoChecksum() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderSimpleReads(), false);
  }
  
  private static class TestBlockReaderLocalArrayLongSkip 
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException { }
    @Override
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 1);
      compareArrayRegion(original, 0, buf, 0, 1);
      reader.skipFully(10000); // skip from offset 1 to offset 10001
      reader.readFully(buf, 10001, 500);
      compareArrayRegion(original, 10001, buf, 10001, 500);
      try {
        reader.skipFully(10000);
        Assert.fail("expected to reach EOF by skipping");
      } catch (EOFException e) {
        // expected
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalArrayLongSkip() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayLongSkip(), true);
  }

  @Test
  public void testBlockReaderLocalArrayLongSkipNoChecksum() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayLongSkip(), false);
  }
  
  private static class TestBlockReaderLocalArrayReadsAndSkips 
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException { }
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 10);
      compareArrayRegion(original, 0, buf, 0, 10);
      reader.readFully(buf, 10, 100);
      compareArrayRegion(original, 10, buf, 10, 100);
      reader.readFully(buf, 110, 700);
      compareArrayRegion(original, 110, buf, 110, 700);
      reader.skipFully(1); // skip from offset 810 to offset 811
      reader.readFully(buf, 811, 5);
      compareArrayRegion(original, 811, buf, 811, 5);
      reader.skipFully(900); // skip from offset 816 to offset 1716
      reader.readFully(buf, 1716, 5);
      compareArrayRegion(original, 1716, buf, 1716, 5);
    }
  }
  
  @Test
  public void testBlockReaderLocalArrayReadsAndSkips() throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReadsAndSkips(),
        true);
  }

  @Test
  public void testBlockReaderLocalArrayReadsAndSkipsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalArrayReadsAndSkips(),
        false);
  }

  private static class TestBlockReaderLocalByteBufferReadsAndSkips 
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException { }
    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      ByteBuffer buf = ByteBuffer.wrap(new byte[TEST_LENGTH]);
      readFully(reader, buf, 0, 10);
      compareArrayRegion(original, 0, buf.array(), 0, 10);
      readFully(reader, buf, 10, 100);
      compareArrayRegion(original, 10, buf.array(), 10, 100);
      readFully(reader, buf, 110, 700);
      compareArrayRegion(original, 110, buf.array(), 110, 700);
      reader.skipFully(1); // skip from offset 810 to offset 811
      readFully(reader, buf, 811, 5);
      compareArrayRegion(original, 811, buf.array(), 811, 5);
    }
  }
  
  @Test
  public void testBlockReaderLocalByteBufferReadsAndSkips()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferReadsAndSkips(), true);
  }

  @Test
  public void testBlockReaderLocalByteBufferReadsAndSkipsNoChecksum()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalByteBufferReadsAndSkips(), false);
  }
  
  private static class TestBlockReaderLocalReadCorruptStart
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException {
      RandomAccessFile bf = null;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      try {
        reader.readFully(buf, 0, 10);
        Assert.fail("did not detect corruption");
      } catch (IOException e) {
        // expected
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalReadCorruptStart()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorruptStart(), true);
  }
  
  private static class TestBlockReaderLocalReadCorrupt
      implements BlockReaderLocalTest {
    boolean haveChecksumFile = false;
    @Override
    public void setup(File blockFile, File checkFile) throws IOException {
      RandomAccessFile bf = null;
      if (checkFile != null) {
        haveChecksumFile = true;
      }
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.seek(1539);
        bf.write(new byte[] {0,0,0,0,0,0,0,0,0,0,0,0,0,0});
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      byte buf[] = new byte[TEST_LENGTH];
      reader.readFully(buf, 0, 10);
      compareArrayRegion(original, 0, buf, 0, 10);
      reader.readFully(buf, 10, 100);
      compareArrayRegion(original, 10, buf, 10, 100);
      reader.readFully(buf, 110, 700);
      compareArrayRegion(original, 110, buf, 110, 700);
      reader.skipFully(1); // skip from offset 810 to offset 811
      reader.readFully(buf, 811, 5);
      compareArrayRegion(original, 811, buf, 811, 5);
      if (haveChecksumFile) {
        // With a checksum file, we should detect the corruption.
        try {
          reader.readFully(buf, 816, 900);
          Assert.fail("did not detect corruption");
        } catch (ChecksumException e) {
          // expected
        }
      } else {
        // If we're not using the checksum file, the corruption should go
        // undetected.
        reader.readFully(buf, 816, 900);
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalReadCorrupt()
      throws IOException {
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), true);
    runBlockReaderLocalTest(new TestBlockReaderLocalReadCorrupt(), false);
  }

  /**
   * Test the behavior of BlockReaderLocal when confronted with a block file
   * that is zero-length, even though it's supposed to contain data.
   */
  private static class TestBlockReaderLocalZeroLengthCorruptBlockFile
      implements BlockReaderLocalTest {
    @Override
    public void setup(File blockFile, File checkFile) throws IOException {
      RandomAccessFile bf = null;
      try {
        bf = new RandomAccessFile(blockFile, "rw");
        bf.setLength(0);
      } finally {
        if (bf != null) bf.close();
      }
    }

    public void doTest(BlockReaderLocal reader, byte original[]) 
        throws IOException {
      try {
        reader.skipFully(10);
        Assert.fail("did not detect truncation!");
      } catch (EOFException e) {
        // expected
      }
    }
  }
  
  @Test
  public void testBlockReaderLocalZeroLengthCorruptBlockFileTest()
      throws IOException {
    runBlockReaderLocalTest(
        new TestBlockReaderLocalZeroLengthCorruptBlockFile(), true);
    runBlockReaderLocalTest(
        new TestBlockReaderLocalZeroLengthCorruptBlockFile(), false);
  }
}
