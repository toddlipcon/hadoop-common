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

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferEncryptor;
import org.apache.hadoop.hdfs.protocol.datatransfer.IOStreamPair;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.io.IOUtils;

/** 
 * Utility class to create BlockReader implementations.
 */
@InterfaceAudience.Private
public class BlockReaderFactory {
  private TreeSet<DatanodeID> dataNodesWithoutShortCircuit;
  private final String clientName;
  private final Conf conf;

  public BlockReaderFactory(boolean tryShortCircuit, String clientName,
      Conf conf) {
    if (tryShortCircuit) {
      dataNodesWithoutShortCircuit = new TreeSet<DatanodeID>();
    } else {
      dataNodesWithoutShortCircuit = null;
    }
    this.clientName = clientName;
    this.conf = conf;
  }

  private boolean shortCircuitDisabledForDataNode(DatanodeID dn) {
    if (dataNodesWithoutShortCircuit == null) {
      return true;
    } else {
      return dataNodesWithoutShortCircuit.contains(dn);
    }
  }
  
  private void disableShortCircuitForDataNode(DatanodeID dn) {
    if (dataNodesWithoutShortCircuit != null) {
      dataNodesWithoutShortCircuit.add(dn);
    }
  }

  BlockReaderLocal newShortCircuitBlockReader(DatanodeID chosenNode,
      DomainSocket sock, String file, ExtendedBlock block,
      Token<BlockTokenIdentifier> token, IOStreamPair ioStreams)
        throws IOException {
    final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
          ioStreams.out));
    new Sender(out).requestShortCircuitFds(block, token, 1);
    DataInputStream in = new DataInputStream(ioStreams.in);
    BlockOpResponseProto resp = BlockOpResponseProto.parseFrom(
        HdfsProtoUtil.vintPrefixed(in));
    switch (resp.getStatus()) {
    case SUCCESS:
      BlockReaderLocal reader = null;
      byte buf[] = new byte[1];
      FileInputStream fis[] = new FileInputStream[2];
      sock.recvFileInputStreams(fis, buf, 0, buf.length);
      try {
        reader = new BlockReaderLocal(fis[0], fis[1], file, ioStreams, sock);
      } finally {
        if (reader == null) {
          IOUtils.cleanup(DFSClient.LOG, fis);
        }
      }
      return reader;
    case ERROR_UNSUPPORTED:
      if (!resp.hasShortCircuitAccessVersion()) {
        DFSClient.LOG.warn("short-circuit read access is disabled for " +
            "DataNode " + chosenNode + ".  reason: " + resp.getMessage());
        disableShortCircuitForDataNode(chosenNode);
      } else {
        DFSClient.LOG.warn("short-circuit read access for the file " +
            file + " is disabled for DataNode " + chosenNode +
            ".  reason: " + resp.getMessage());
      }
      return null;
    case ERROR_ACCESS_TOKEN:
      String msg = "access control error while " +
          "attempting to set up short-circuit access to " +
          file + resp.getMessage();
      DFSClient.LOG.error(msg);
      return null; // TODO: throw access control exception?
    default:
      DFSClient.LOG.warn("error while attempting to set up short-circuit " +
          "access to " + file + ": " + resp.getMessage());
      return null;
    }
  }

  /**
   * @see #newBlockReader(Conf, Socket, String, ExtendedBlock, Token, long, long, int, boolean, String)
   */
  public static BlockReader newBlockReader(
      Configuration conf,
      Socket sock, String file,
      ExtendedBlock block, Token<BlockTokenIdentifier> blockToken, 
      long startOffset, long len, DataEncryptionKey encryptionKey)
          throws IOException {
    int bufferSize = conf.getInt(DFSConfigKeys.IO_FILE_BUFFER_SIZE_KEY,
        DFSConfigKeys.IO_FILE_BUFFER_SIZE_DEFAULT);
    return newBlockReader(new Conf(conf),
        sock, file, block, blockToken, startOffset,
        len, bufferSize, true, "", encryptionKey, null);
  }

  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This is a legacy method.  If you are writing new code, it's best to use the
   * non-static methods in order to avoid having to pass the BlockReaderFactory
   * its state all the time.
   * 
   * @see BlockReaderFactory#create(DatanodeInfo, Socket, String, ExtendedBlock, Token, long, long, int, DataEncryptionKey, IOStreamPair)
   */
  public static BlockReader newBlockReader(
                                     Conf conf,
                                     Socket sock, String file,
                                     ExtendedBlock block, 
                                     Token<BlockTokenIdentifier> blockToken,
                                     long startOffset, long len,
                                     int bufferSize, boolean verifyChecksum,
                                     String clientName,
                                     DataEncryptionKey encryptionKey,
                                     IOStreamPair ioStreams)
                                     throws IOException {
     return new BlockReaderFactory(false, clientName, conf).
       create(null, sock, file, block, blockToken, startOffset, len, bufferSize,
           verifyChecksum, encryptionKey, ioStreams);
   }


  /**
   * Create a new BlockReader specifically to satisfy a read.
   * This method also sends the OP_REQUEST_SHORT_CIRCUIT_FD and/or
   * OP_READ_BLOCK requests.
   * 
   * @param chosenNode  If non-null, the Datanode we have chosen.
   * @param sock  An established Socket to the DN. The BlockReader will not close it normally
   * @param file  File location
   * @param block  The block object
   * @param blockToken  The block token for security
   * @param startOffset  The read offset, relative to block head
   * @param len  The number of bytes to read
   * @param bufferSize  The IO buffer size (not the client buffer size)
   * @param verifyChecksum  Whether to verify checksum
   * @return New BlockReader instance, or null on error.
   */
  @SuppressWarnings("deprecation")
  public BlockReader create(DatanodeInfo chosenNode,
                            Socket sock, String file,
                            ExtendedBlock block, 
                            Token<BlockTokenIdentifier> blockToken,
                            long startOffset, long len,
                            int bufferSize, boolean verifyChecksum,
                            DataEncryptionKey encryptionKey,
                            IOStreamPair ioStreams)
                              throws IOException {
    boolean tryShortCircuit = (chosenNode != null) && 
        (sock instanceof DomainSocket) &&
        (!shortCircuitDisabledForDataNode(chosenNode));
    if ((!conf.useLegacyBlockReader) || tryShortCircuit) {
      if (ioStreams == null) {
        ioStreams = new IOStreamPair(NetUtils.getInputStream(sock),
            NetUtils.getOutputStream(sock, HdfsServerConstants.WRITE_TIMEOUT));
      }
    }
    if (encryptionKey != null) {
      if (conf.useLegacyBlockReader) {
        throw new RuntimeException("Encryption is not supported with the legacy block reader.");
      }
      IOStreamPair encryptedStreams =
          DataTransferEncryptor.getEncryptedStreams(
              ioStreams.out, ioStreams.in, encryptionKey);
      ioStreams = encryptedStreams;
    }
    if (tryShortCircuit) {
      BlockReader r = newShortCircuitBlockReader(chosenNode,
          (DomainSocket)sock, file, block, blockToken, ioStreams);
      if (r != null) {
        return r;
      }
    }
    if (conf.useLegacyBlockReader) {
      return RemoteBlockReader.newBlockReader(
          sock, file, block, blockToken, startOffset, len, bufferSize);
    } else {
      return RemoteBlockReader2.newBlockReader(
          sock, file, block, blockToken, startOffset, len, bufferSize,
          verifyChecksum, clientName, encryptionKey, ioStreams);
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
}
