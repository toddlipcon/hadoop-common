package org.apache.hadoop.hdfs.qjournal.protocolPB;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosAcceptRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.ipc.ProtobufHelper;
import org.apache.hadoop.ipc.ProtocolMetaInterface;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RpcClientUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * This class is the client side translator to translate the requests made on
 * {@link JournalProtocol} interfaces to the RPC server implementing
 * {@link JournalProtocolPB}.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class QJournalProtocolTranslatorPB implements ProtocolMetaInterface,
    QJournalProtocol, Closeable {
  /** RpcController is not used and hence is set to null */
  private final static RpcController NULL_CONTROLLER = null;
  private final QJournalProtocolPB rpcProxy;
  
  public QJournalProtocolTranslatorPB(QJournalProtocolPB rpcProxy) {
    this.rpcProxy = rpcProxy;
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
  }


  @Override
  public GetEpochInfoResponseProto getEpochInfo(String jid)
      throws IOException {
    try {
      GetEpochInfoRequestProto req = GetEpochInfoRequestProto.newBuilder()
          .setJid(convertJournalId(jid))
          .build();
      return rpcProxy.getEpochInfo(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private JournalIdProto convertJournalId(String jid) {
    return JournalIdProto.newBuilder()
        .setIdentifier(jid)
        .build();
  }

  @Override
  public NewEpochResponseProto newEpoch(String jid,
      NamespaceInfo nsInfo, long epoch)
      throws IOException {
    try {
      NewEpochRequestProto req = NewEpochRequestProto.newBuilder()
        .setJid(convertJournalId(jid))
        .setNsInfo(PBHelper.convert(nsInfo))
        .setEpoch(epoch)
        .build();
      return rpcProxy.newEpoch(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void journal(RequestInfo reqInfo, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    JournalRequestProto req = JournalRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setFirstTxnId(firstTxnId)
        .setNumTxns(numTxns)
        .setRecords(PBHelper.getByteString(records))
        .build();
    try {
      rpcProxy.journal(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  private QJournalProtocolProtos.RequestInfoProto convert(RequestInfo reqInfo) {
    return QJournalProtocolProtos.RequestInfoProto.newBuilder()
      .setJournalId(convertJournalId(reqInfo.getJournalId()))
      .setEpoch(reqInfo.getEpoch())
      .setIpcSerialNumber(reqInfo.getIpcSerialNumber())
      .build();
  }

  @Override
  public void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    StartLogSegmentRequestProto req = StartLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setTxid(txid)
        .build();
    try {
      rpcProxy.startLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }
  
  @Override
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    FinalizeLogSegmentRequestProto req = FinalizeLogSegmentRequestProto.newBuilder()
        .setReqInfo(convert(reqInfo))
        .setStartTxId(startTxId)
        .setEndTxId(endTxId)
        .build();
    try {
      rpcProxy.finalizeLogSegment(NULL_CONTROLLER, req);
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void syncLog(RequestInfo reqInfo, RemoteEditLogProto segment,
      URL url) throws IOException {
    try {
      rpcProxy.syncLog(NULL_CONTROLLER,
          SyncLogRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setLogSegment(segment)
            .setFromURL(url.toExternalForm())
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(String jid,
      long sinceTxId) throws IOException {
    try {
      return rpcProxy.getEditLogManifest(NULL_CONTROLLER,
          GetEditLogManifestRequestProto.newBuilder()
            .setJid(convertJournalId(jid))
            .setSinceTxId(sinceTxId)
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public PaxosPrepareResponseProto paxosPrepare(RequestInfo reqInfo,
      String decisionId) throws IOException {
    try {
      return rpcProxy.paxosPrepare(NULL_CONTROLLER,
          PaxosPrepareRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setDecisionId(decisionId)
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  @Override
  public void paxosAccept(RequestInfo reqInfo, String decisionId, byte[] value)
      throws IOException {
    try {
      rpcProxy.paxosAccept(NULL_CONTROLLER,
          PaxosAcceptRequestProto.newBuilder()
            .setReqInfo(convert(reqInfo))
            .setDecisionId(decisionId)
            .setValue(ByteString.copyFrom(value))
            .build());
    } catch (ServiceException e) {
      throw ProtobufHelper.getRemoteException(e);
    }
  }

  public boolean isMethodSupported(String methodName) throws IOException {
    return RpcClientUtil.isMethodSupported(rpcProxy,
        QJournalProtocolPB.class, RPC.RpcKind.RPC_PROTOCOL_BUFFER,
        RPC.getProtocolVersion(QJournalProtocolPB.class), methodName);
  }

}
