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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.util.DataChecksum;

/**
 * BlockReaderLocal enables local short circuited reads. If the DFS client is on
 * the same machine as the datanode, then the client can read files directly
 * from the local file system rather than going through the datanode for better
 * performance. <br>
 */
class BlockReaderLocal implements BlockReader {
  static final Log LOG = LogFactory.getLog(BlockReaderLocal.class);
  
  /**
   *  File name.  We only use this in error messages.
   */
  private final String fileName; 
  
  private DataChecksum checksum;
  private final FileChannel dataCh;
  private final FileChannel checkCh;
  private final ByteBuffer cache;
  private final ByteBuffer checkCache;
  private final ByteBuffer skipCache;
  private int cacheEnd;
  private IOException pendingException;
  private final IOStreamPair ioStreamPair;
  private Socket dnSock;
  
  public BlockReaderLocal(FileInputStream dataIn, FileInputStream checkIn,
      String fileName, IOStreamPair ioStreamPair, Socket dnSock) 
          throws IOException {
    this.fileName = fileName;
    this.dataCh = dataIn.getChannel();
    this.dataCh.position(0);
    if (checkIn == null) {
      // Either there is no checksum file, or we've decided not to use it.
      this.checksum = null;
      this.checkCh = null;
      this.cache = null;
      this.checkCache = null;
      this.skipCache = null;
    } else {
      // Use the checksum file.
      byte csumHeader[] = new byte[2 + DataChecksum.HEADER_LEN];
      try {
        IOUtils.readFully(checkIn, csumHeader, 0, csumHeader.length);
      } catch (IOException e) {
        throw new IOException("error reading the header of the checksum " +
                              "file", e);
      }
      if ((csumHeader[0] != 0) && (csumHeader[1] != 1)) {
        throw new IOException("unsupported checksum file version.");
      }
      this.checksum = DataChecksum.newDataChecksum(csumHeader, 2);
      this.checkCh = checkIn.getChannel();
      this.checkCh.position(2 + DataChecksum.HEADER_LEN);
      this.cache = ByteBuffer.allocateDirect(this.checksum.getBytesPerChecksum());
      this.checkCache = ByteBuffer.allocateDirect(this.checksum.getChecksumSize());
      this.skipCache = ByteBuffer.allocateDirect(this.checksum.getBytesPerChecksum());
    }
    this.ioStreamPair = ioStreamPair;
    this.dnSock = dnSock;
  }
  
  @Override
  public synchronized int read(byte[] buf, int off, int len) 
                               throws IOException {
    return read(ByteBuffer.wrap(buf, off, len));
  }
  
  @Override
  public synchronized int read(ByteBuffer buf) throws IOException {
    if (checksum == null) {
      // Handle the no-checksum case.
      if (pendingException != null) {
        throw pendingException;
      }
      return dataCh.read(buf);
    }
    int nRead = 0;
    boolean firstFill = true;
    while (true) {
      if (buf.remaining() < (cacheEnd - cache.position())) {
        cache.limit(cache.position() + buf.remaining());
        nRead += buf.remaining();
        buf.put(cache);
        break;
      } else {
        cache.limit(cacheEnd);
        nRead += cache.remaining();
        buf.put(cache);
      }
      fillCache(firstFill);
      firstFill = false;
      if (cacheEnd == 0) {
        // Nothing more can be read from the buffer.
        break;
      }
    }
    return (nRead == 0) ? -1 : nRead;
  }

  /**
   * Read as much as possible from a file, stopping only on EOF.
  * 
   * @param ch             The FileChannel to read
   * @param b              The ByteBuffer to read into.
   * @return               the amount read
   * @throws               IOException
   */
  private static int readAsMuchAsPossible(FileChannel ch, ByteBuffer b)
      throws IOException {
    int nRet = 0;
    while (true) {
      int ret = ch.read(b);
      if (ret < 0) {
        return nRet; // EOF
      }
      nRet += ret;
      if (b.remaining() <= 0) {
        return nRet; // filled buffer
      }
    }
  }

  void fillCache(boolean firstFill) throws IOException {
    if (pendingException != null) {
      throw pendingException;
    }
    try {
      long dataPos = dataCh.position();
      cache.clear();
      cacheEnd = readAsMuchAsPossible(dataCh, cache);
      checkCache.clear();
      readAsMuchAsPossible(checkCh, checkCache);
      // Currently the cache only contains one checksum; it could be
      // modified to have several by checking against the length of 
      // the data cache here rather than checkCache.remaining().
      if (checkCache.remaining() > 0) {
        if (cacheEnd > 0) {
          throw new IOException("checksum file ended prematurely");
        }
      }
      cache.limit(cacheEnd);
      cache.position(0);
      if (cacheEnd <= 0) {
        checkCache.limit(0);
      } else {
        checkCache.clear();
      }
      checksum.verifyChunkedSums(cache, checkCache, fileName, dataPos);
    } catch (IOException e) {
      if (firstFill) {
        throw e;
      } else {
        // The basic idea here is that if an exception occurs at position X,
        // we should let the client read all the bytes up to X before throwing
        // the exception at him.
        setPendingException(e);
      }
    }
  }

  /**
   * Set an exception that will occur on the next IO operation.
   *
   * @param e        The IOException to set.
   */
  public synchronized void setPendingException(IOException e) {
    pendingException = e;
    cache.clear();
    cacheEnd = 0;
  }
  
  @Override
  public synchronized long skip(long n) throws IOException {
    if (pendingException != null) {
      throw pendingException;
    }
    long skipped = 0;
    long slop = 0;
    if (checksum != null) {
      // First, discard as many cached bytes as we can.
      int cachePos = cache.position();
      int cacheRem = cacheEnd - cachePos;
      if (cacheRem >= n) {
        // If the cache is big enough to absorb the entire skip
        // request, then we're done.
        cache.limit(cacheEnd);
        cache.position(cachePos + (int)n);
        return n;
      }
      cache.clear();
      cacheEnd = 0;
      n -= cacheRem;
      skipped += cacheRem;

      if ((skipped == 0) && (n < checksum.getBytesPerChecksum())) {
        // If we were asked to skip a small amount of bytes, and there
        // was nothing in the cache to skip, just call read() to ensure
        // we make some progress.
        skipCache.position(0);
        skipCache.limit((int)n);
        long res = read(skipCache);
        return res > 0 ? res : 0; // read returns -1 on EOF; skip returns 0.
      }
      slop = (int)(n % checksum.getBytesPerChecksum());
      n -= slop;
    }
    // At this point, the cache is empty, and the skip amount is a multiple
    // of checks.getBytesPerChecksum (if we are using checksums).
    // We can simply reposition the dataCh and checkCh.
    long curPos = dataCh.position();
    long curSize = dataCh.size();
    long wantPos;
    if (curPos + n > curSize) {
      wantPos = curSize;
    } else {
      wantPos = curPos + n;
    }
    dataCh.position(wantPos);
    if (checkCh != null) {
      checkCh.position(2 + DataChecksum.HEADER_LEN +
          (checksum.getChecksumSize() *
            (wantPos / checksum.getBytesPerChecksum())));
    }
    skipped += (wantPos - curPos);
    return skipped;
  }

  @Override
  public void skipFully(long n) throws IOException {
    BlockReaderUtil.skipFully(this, n);
  } 

  @Override
  public int available() throws IOException {
    // We never do network I/O in BlockReaderLocal.
    return Integer.MAX_VALUE;
  }

  @Override
  public synchronized void close() throws IOException {
    LinkedList<IOException> exceptions = new LinkedList<IOException>();
    try {
      dataCh.close();
    } catch (IOException e) {
      exceptions.add(e);
    }
    try {
      if (checkCh != null) checkCh.close();
    } catch (IOException e) {
      exceptions.add(e);
    }
    if (!exceptions.isEmpty()) {
      throw MultipleIOException.createIOException(exceptions);
    }
  }

  @Override
  public Socket takeSocket() {
    Socket s = dnSock;
    dnSock = null;
    return s;
  }

  @Override
  public boolean hasSentStatusCode() {
    return true;
  }

  @Override
  public int readAll(byte[] buf, int offset, int len) throws IOException {
    return BlockReaderUtil.readAll(this, buf, offset, len);
  }

  @Override
  public void readFully(byte[] buf, int off, int len) throws IOException {
    BlockReaderUtil.readFully(this, buf, off, len);
  }
  
  @Override
  public IOStreamPair getStreams() {
    return ioStreamPair;
  }
}
