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

import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.PacketHeader;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.SocketInputStream;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DataChecksum;

import com.google.common.base.Preconditions;

/** This is a wrapper around connection to datanode
 * and understands checksum, offset etc.
 *
 * Terminology:
 * <dl>
 * <dt>block</dt>
 *   <dd>The hdfs block, typically large (~64MB).
 *   </dd>
 * <dt>chunk</dt>
 *   <dd>A block is divided into chunks, each comes with a checksum.
 *       We want transfers to be chunk-aligned, to be able to
 *       verify checksums.
 *   </dd>
 * <dt>packet</dt>
 *   <dd>A grouping of chunks used for transport. It contains a
 *       header, followed by checksum data, followed by real data.
 *   </dd>
 * </dl>
 * Please see DataNode for the RPC specification.
 */
@InterfaceAudience.Private
public class BlockReader extends FSInputStream {

  static final Log LOG = LogFactory.getLog(BlockReader.class);
  
  Socket dnSock; //for now just sending the status code (e.g. checksumOk) after the read.
  private ReadableByteChannel in;
  private DataChecksum checksum;
  
  private PacketHeader curHeader;
  private ByteBuffer curPacketBuf = null;
  private ByteBuffer curDataSlice = null;


  /** offset in block of the last chunk received */
  private long lastSeqNo = -1;

  /** offset in block where reader wants to actually read */
  private long startOffset;
  private final String filename;

  private ByteBuffer headerBuf = ByteBuffer.allocateDirect(PacketHeader.PKT_HEADER_LEN);

  private int bytesPerChecksum;
  private int checksumSize;

  /**
   * The total number of bytes we need to transfer from the DN.
   * This is the amount that the user has requested plus some padding
   * at the beginning so that the read can begin on a chunk boundary.
   */
  private long bytesNeededToFinish;

  private final boolean verifyChecksum;

  private boolean eos = false;
  private boolean sentStatusCode = false;
  
  byte[] skipBuf = null;
  ByteBuffer checksumBytes = null;
  /** Amount of unread data in the current received packet */
  int dataLeft = 0;
  
  @Override
  public synchronized int read(byte[] buf, int off, int len) 
                               throws IOException {
    if (eos) {
      // Already hit EOF
      return -1;
    }

    if (curPacketBuf == null || curDataSlice.remaining() == 0) {
      readNextPacket();
    }
    if (curDataSlice.remaining() == 0) {
      // we're at EOF now
      return -1;
    }
    
    int nRead = Math.min(curDataSlice.remaining(), len);
    curDataSlice.get(buf, off, nRead);
    
    return nRead;
  }

  private void readNextPacket() throws IOException {
    Preconditions.checkState(curHeader == null || !curHeader.isLastPacketInBlock());
    
    //Read packet headers.
    readPacketHeader();

    if (LOG.isDebugEnabled()) {
      LOG.debug("DFSClient readNextPacket got header " + curHeader);
    }

    // Sanity check the lengths
    if (!curHeader.sanityCheck(lastSeqNo)) {
         throw new IOException("BlockReader: error in packet header " +
                               curHeader);
    }
    
    if (curHeader.getDataLen() > 0) {
      int chunks = 1 + (curHeader.getDataLen() - 1) / bytesPerChecksum;
      int checksumsLen = chunks * checksumSize;
      int bufsize = checksumsLen + curHeader.getDataLen();
      
      resetPacketBuffer(checksumsLen, curHeader.getDataLen());
  
      lastSeqNo = curHeader.getSeqno();
      LOG.debug("bufSize: " + bufsize);
      // dataLeft = curHeader.getDataLen();
      if (bufsize > 0) {
        readChannelFully(in, curPacketBuf);
        curPacketBuf.flip();
        if (verifyChecksum) {
          verifyPacketChecksums();
        }
      }
      bytesNeededToFinish -= curHeader.getDataLen();
      LOG.debug("bytesNeededToFinish = " + bytesNeededToFinish);
    }    
    
    // First packet will include some data prior to the first byte
    // the user requested. Skip it.
    if (curHeader.getOffsetInBlock() < startOffset) {
      int newPos = (int) (startOffset - curHeader.getOffsetInBlock());
      curDataSlice.position(newPos);
    }

    // If we've now satisfied the whole client read, read one last packet
    // header, which should be empty
    if (bytesNeededToFinish <= 0) {
      readTrailingEmptyPacket();
      if (verifyChecksum) {
        sendReadResult(dnSock, Status.CHECKSUM_OK);
      } else {
        sendReadResult(dnSock, Status.SUCCESS);
      }
    }
  }

  private void verifyPacketChecksums() throws ChecksumException {
    checksum.verifyChunkedSums(curDataSlice, curPacketBuf,
        filename, curHeader.getOffsetInBlock()); // TODO check offset
  }

  private static void readChannelFully(ReadableByteChannel ch, ByteBuffer buf)
  throws IOException {
    while (buf.remaining() > 0) {
      int n = ch.read(buf);
      if (n < 0) {
        throw new IOException("Premature EOF reading from " + ch);
      }
    }
  }

  private void resetPacketBuffer(int checksumsLen, int dataLen) {
    int packetLen = checksumsLen + dataLen;
    if (curPacketBuf == null ||
        curPacketBuf.capacity() < packetLen) {
      curPacketBuf = ByteBuffer.allocateDirect(packetLen);
    }
    curPacketBuf.position(checksumsLen);
    curDataSlice = curPacketBuf.slice();
    curDataSlice.limit(dataLen);
    curPacketBuf.clear();
    curPacketBuf.limit(checksumsLen + dataLen);
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    /* How can we make sure we don't throw a ChecksumException, at least
     * in majority of the cases?. This one throws. */  
    if ( skipBuf == null ) {
      skipBuf = new byte[bytesPerChecksum]; 
    }

    long nSkipped = 0;
    while ( nSkipped < n ) {
      int toSkip = (int)Math.min(n-nSkipped, skipBuf.length);
      int ret = read(skipBuf, 0, toSkip);
      if ( ret <= 0 ) {
        return nSkipped;
      }
      nSkipped += ret;
    }
    return nSkipped;
  }

  @Override
  public int read() throws IOException {
    throw new IOException("read() is not expected to be invoked. " +
                          "Use read(buf, off, len) instead.");
  }
  
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    /* Checksum errors are handled outside the BlockReader. 
     * DFSInputStream does not always call 'seekToNewSource'. In the 
     * case of pread(), it just tries a different replica without seeking.
     */ 
    return false;
  }
  
  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("Seek() is not supported in BlockInputChecker");
  }
  
  private void readPacketHeader() throws IOException {
    headerBuf.clear();
    readChannelFully(in, headerBuf);
    headerBuf.flip();
    if (curHeader == null) curHeader = new PacketHeader();
    curHeader.readFields(headerBuf);
  }

  private void readTrailingEmptyPacket() throws IOException {
    headerBuf.clear();
    readChannelFully(in, headerBuf);
    headerBuf.flip();
    PacketHeader trailer = new PacketHeader();
    trailer.readFields(headerBuf);
    if (!trailer.isLastPacketInBlock() ||
       trailer.getDataLen() != 0) {
      throw new IOException("Expected empty end-of-read packet! Header: " +
                            trailer);
    }
  }

  private BlockReader(String file, String bpid, long blockId,
      ReadableByteChannel in, DataChecksum checksum, boolean verifyChecksum,
      long startOffset, long firstChunkOffset, long bytesToRead, Socket dnSock) {
    // Path is used only for printing block and file information in debug
    this.dnSock = dnSock;
    this.in = in;
    this.checksum = checksum;
    this.verifyChecksum = verifyChecksum;
    this.startOffset = Math.max( startOffset, 0 );
    this.filename = file;

    // The total number of bytes that we need to transfer from the DN is
    // the amount that the user wants (bytesToRead), plus the padding at
    // the beginning in order to chunk-align. Note that the DN may elect
    // to send more than this amount if the read starts/ends mid-chunk.
    this.bytesNeededToFinish = bytesToRead + (startOffset - firstChunkOffset);
    bytesPerChecksum = this.checksum.getBytesPerChecksum();
    checksumSize = this.checksum.getChecksumSize();
  }

  public static BlockReader newBlockReader(Socket sock, String file,
      ExtendedBlock block, Token<BlockTokenIdentifier> blockToken, 
      long startOffset, long len, int bufferSize) throws IOException {
    return newBlockReader(sock, file, block, blockToken, startOffset, len, bufferSize,
        true);
  }

  /** Java Doc required */
  public static BlockReader newBlockReader( Socket sock, String file, 
                                     ExtendedBlock block, 
                                     Token<BlockTokenIdentifier> blockToken,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum)
                                     throws IOException {
    return newBlockReader(sock, file, block, blockToken, startOffset,
                          len, bufferSize, verifyChecksum, "");
  }

  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_READ_BLOCK request.
   *
   * @param sock  An established Socket to the DN. The BlockReader will not close it normally
   * @param file  File location
   * @param block  The block object
   * @param blockToken  The block token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @param clientName  Client name
   * @return New BlockReader instance, or null on error.
   */
  public static BlockReader newBlockReader( Socket sock, String file,
                                     ExtendedBlock block, 
                                     Token<BlockTokenIdentifier> blockToken,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum,
                                     String clientName)
                                     throws IOException {
    // in and out will be closed when sock is closed (by the caller)
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
          NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT)));
    new Sender(out).readBlock(block, blockToken, clientName, startOffset, len);
    
    //
    // Get bytes in block, set streams
    //

    SocketInputStream sin =
      (SocketInputStream)NetUtils.getInputStream(sock); 
    DataInputStream in = new DataInputStream(sin);
    
    BlockOpResponseProto status = BlockOpResponseProto.parseFrom(
        vintPrefixed(in));
    if (status.getStatus() != Status.SUCCESS) {
      if (status.getStatus() == Status.ERROR_ACCESS_TOKEN) {
        throw new InvalidBlockTokenException(
            "Got access token error for OP_READ_BLOCK, self="
                + sock.getLocalSocketAddress() + ", remote="
                + sock.getRemoteSocketAddress() + ", for file " + file
                + ", for pool " + block.getBlockPoolId() + " block " 
                + block.getBlockId() + "_" + block.getGenerationStamp());
      } else {
        throw new IOException("Got error for OP_READ_BLOCK, self="
            + sock.getLocalSocketAddress() + ", remote="
            + sock.getRemoteSocketAddress() + ", for file " + file
            + ", for pool " + block.getBlockPoolId() + " block " 
            + block.getBlockId() + "_" + block.getGenerationStamp());
      }
    }
    DataChecksum checksum = DataChecksum.newDataChecksum( in );
    //Warning when we get CHECKSUM_NULL?
    
    // Read the first chunk offset.
    long firstChunkOffset = in.readLong();
    
    if ( firstChunkOffset < 0 || firstChunkOffset > startOffset ||
        firstChunkOffset >= (startOffset + checksum.getBytesPerChecksum())) {
      throw new IOException("BlockReader: error in first chunk offset (" +
                            firstChunkOffset + ") startOffset is " + 
                            startOffset + " for file " + file);
    }

    return new BlockReader(file, block.getBlockPoolId(), block.getBlockId(),
        sin, checksum, verifyChecksum, startOffset, firstChunkOffset, len, sock);
  }

  @Override
  public synchronized void close() throws IOException {
    startOffset = -1;
    checksum = null;
    if (dnSock != null) {
      dnSock.close();
    }

    // in will be closed when its Socket is closed.
  }
  
  /**
   * Take the socket used to talk to the DN.
   */
  public Socket takeSocket() {
    assert hasSentStatusCode() :
      "BlockReader shouldn't give back sockets mid-read";
    Socket res = dnSock;
    dnSock = null;
    return res;
  }

  /**
   * Whether the BlockReader has reached the end of its input stream
   * and successfully sent a status code back to the datanode.
   */
  public boolean hasSentStatusCode() {
    return sentStatusCode;
  }

  /**
   * When the reader reaches end of the read, it sends a status response
   * (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
   * closing our connection (which we will re-open), but won't affect
   * data correctness.
   */
  void sendReadResult(Socket sock, Status statusCode) {
    assert !sentStatusCode : "already sent status code to " + sock;
    try {
      OutputStream out = NetUtils.getOutputStream(sock, HdfsConstants.WRITE_TIMEOUT);
      
      ClientReadStatusProto.newBuilder()
        .setStatus(statusCode)
        .build()
        .writeDelimitedTo(out);

      out.flush();
      sentStatusCode = true;
    } catch (IOException e) {
      // It's ok not to be able to send this. But something is probably wrong.
      LOG.info("Could not send read status (" + statusCode + ") to datanode " +
               sock.getInetAddress() + ": " + e.getMessage());
    }
  }
  
  /**
   * File name to print when accessing a block directly (from servlets)
   * @param s Address of the block location
   * @param poolId Block pool ID of the block
   * @param blockId Block ID of the block
   * @return string that has a file name for debug purposes
   */
  public static String getFileName(final InetSocketAddress s,
      final String poolId, final long blockId) {
    return s.toString() + ":" + poolId + ":" + blockId;
  }

  @Override
  public long getPos() throws IOException {
    // DFSInputStream never calls this
    throw new UnsupportedOperationException();
  }
}
