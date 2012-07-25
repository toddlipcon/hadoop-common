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
package org.apache.hadoop.hdfs.protocol.datatransfer;

import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.fromProto;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.fromProtos;
import static org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed;
import static org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil.fromProto;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.HdfsProtoUtil;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpBlockChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpCopyBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReadBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpReplaceBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpTransferBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.SocketInputWrapper;

/** Receiver */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class Receiver implements DataTransferProtocol {
  protected final SocketInputWrapper in;
  private DataInputStream bufferedDataInput;
  private static final int VERY_SMALL_BUFFER = 6;

  /** Create a receiver for DataTransferProtocol with a socket. */
  protected Receiver(final SocketInputWrapper in) {
    this.in = in;
    
    this.bufferedDataInput = new DataInputStream(
        new BufferedInputStream(in, VERY_SMALL_BUFFER));
  }

  /** Read an Op.  It also checks protocol version. */
  protected final Op readOp() throws IOException {
    final short version = bufferedDataInput.readShort();
    if (version != DataTransferProtocol.DATA_TRANSFER_VERSION) {
      throw new IOException( "Version Mismatch (Expected: " +
          DataTransferProtocol.DATA_TRANSFER_VERSION  +
          ", Received: " +  version + " )");
    }
    return Op.read(bufferedDataInput);
  }

  /** Process op by the corresponding method. */
  protected final void processOp(Op op) throws IOException {
    switch(op) {
    case READ_BLOCK:
      opReadBlock();
      break;
    case WRITE_BLOCK:
      opWriteBlock();
      break;
    case REPLACE_BLOCK:
      opReplaceBlock();
      break;
    case COPY_BLOCK:
      opCopyBlock();
      break;
    case BLOCK_CHECKSUM:
      opBlockChecksum();
      break;
    case TRANSFER_BLOCK:
      opTransferBlock();
      break;
    default:
      throw new IOException("Unknown op " + op + " in data stream");
    }
  }

  /** Receive OP_READ_BLOCK */
  private void opReadBlock() throws IOException {
    OpReadBlockProto proto = OpReadBlockProto.parseFrom(
        readVintPrefixedData());
    readBlock(fromProto(proto.getHeader().getBaseHeader().getBlock()),
        fromProto(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        proto.getOffset(),
        proto.getLen());
  }
  
  private byte[] readVintPrefixedData() throws IOException {
    int vint = HdfsProtoUtil.fullyReadVint(bufferedDataInput);
    byte[] data = new byte[vint];
    
    int avail = bufferedDataInput.available(); 
    IOUtils.readFully(bufferedDataInput, data, 0, avail);
    assert bufferedDataInput.available() == 0;
    
    int remaining = vint - avail;
    IOUtils.readFully(in, data, avail, remaining);
    
    return data;
  }

  /** Receive OP_WRITE_BLOCK */
  private void opWriteBlock() throws IOException {
    final OpWriteBlockProto proto = OpWriteBlockProto.parseFrom(readVintPrefixedData());
    writeBlock(fromProto(proto.getHeader().getBaseHeader().getBlock()),
        fromProto(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        fromProtos(proto.getTargetsList()),
        fromProto(proto.getSource()),
        fromProto(proto.getStage()),
        proto.getPipelineSize(),
        proto.getMinBytesRcvd(), proto.getMaxBytesRcvd(),
        proto.getLatestGenerationStamp(),
        fromProto(proto.getRequestedChecksum()));
  }

  /** Receive {@link Op#TRANSFER_BLOCK} */
  private void opTransferBlock() throws IOException {
    final OpTransferBlockProto proto =
      OpTransferBlockProto.parseFrom(readVintPrefixedData());
    transferBlock(fromProto(proto.getHeader().getBaseHeader().getBlock()),
        fromProto(proto.getHeader().getBaseHeader().getToken()),
        proto.getHeader().getClientName(),
        fromProtos(proto.getTargetsList()));
  }

  /** Receive OP_REPLACE_BLOCK */
  private void opReplaceBlock() throws IOException {
    OpReplaceBlockProto proto = OpReplaceBlockProto.parseFrom(readVintPrefixedData());
    replaceBlock(fromProto(proto.getHeader().getBlock()),
        fromProto(proto.getHeader().getToken()),
        proto.getDelHint(),
        fromProto(proto.getSource()));
  }

  /** Receive OP_COPY_BLOCK */
  private void opCopyBlock() throws IOException {
    OpCopyBlockProto proto = OpCopyBlockProto.parseFrom(readVintPrefixedData());
    copyBlock(fromProto(proto.getHeader().getBlock()),
        fromProto(proto.getHeader().getToken()));
  }

  /** Receive OP_BLOCK_CHECKSUM */
  private void opBlockChecksum() throws IOException {
    OpBlockChecksumProto proto = OpBlockChecksumProto.parseFrom(readVintPrefixedData());
    
    blockChecksum(fromProto(proto.getHeader().getBlock()),
        fromProto(proto.getHeader().getToken()));
  }
}
