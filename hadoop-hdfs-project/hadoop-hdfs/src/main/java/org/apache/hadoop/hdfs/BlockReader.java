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

import java.io.IOException;
import java.net.Socket;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;

/**
 * A BlockReader is responsible for reading a single block
 * from a single datanode.
 */
public interface BlockReader extends ByteBufferReadable {

  /* same interface as inputStream java.io.InputStream#read()
   * used by DFSInputStream#read()
   * This violates one rule when there is a checksum error:
   * "Read should not modify user buffer before successful read"
   * because it first reads the data to user buffer and then checks
   * the checksum.
   */
  int read(byte[] buf, int off, int len) throws IOException;

  /**
   * Skip up to the given number of bytes
   * 
   * @param n     The maximum number of bytes to skip
   * 
   * @return      The number of bytes actually skipped
   */
  long skip(long n) throws IOException;

  /**
   * Skip exactly the given number of bytes
   * 
   * @param n     The exact number of bytes to skip
   * 
   * @throws      EOFException if we can't skip n bytes.
   */
  void skipFully(long n) throws IOException;

  /**
   * Returns an estimate of the number of bytes that can be read
   * (or skipped over) from this input stream without performing
   * network I/O.
   */
  int available() throws IOException;

  void close() throws IOException;

  /**
   * Read exactly the given amount of data, throwing an exception
   * if EOF is reached before that amount
   */
  void readFully(byte[] buf, int readOffset, int amtToRead) throws IOException;

  /**
   * Similar to {@link #readFully(byte[], int, int)} except that it will
   * not throw an exception on EOF. However, it differs from the simple
   * {@link #read(byte[], int, int)} call in that it is guaranteed to
   * read the data if it is available. In other words, if this call
   * does not throw an exception, then either the buffer has been
   * filled or the next call will return EOF.
   */
  int readAll(byte[] buf, int offset, int len) throws IOException;

  /**
   * Take the socket used to talk to the DN.
   */
  Socket takeSocket();

  /**
   * Whether the BlockReader has reached the end of its input stream
   * and successfully sent a status code back to the datanode.
   */
  boolean hasSentStatusCode();

  /**
   * @return a reference to the streams this block reader is using.
   */
  IOStreamPair getStreams();
}
