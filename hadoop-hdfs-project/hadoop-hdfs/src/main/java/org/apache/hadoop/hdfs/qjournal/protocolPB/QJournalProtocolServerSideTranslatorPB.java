package org.apache.hadoop.hdfs.qjournal.protocolPB;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.FinalizeLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.StartLogSegmentResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogsRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;

/**
 * Implementation for protobuf service that forwards requests
 * received on {@link JournalProtocolPB} to the 
 * {@link JournalProtocol} server implementation.
 */
@InterfaceAudience.Private
public class QJournalProtocolServerSideTranslatorPB implements QJournalProtocolPB {
  /** Server side implementation to delegate the requests to */
  private final QJournalProtocol impl;

  public QJournalProtocolServerSideTranslatorPB(QJournalProtocol impl) {
    this.impl = impl;
  }

  @Override
  public GetEpochInfoResponseProto getEpochInfo(RpcController controller,
      GetEpochInfoRequestProto request) throws ServiceException {
    try {
      return impl.getEpochInfo(
          convert(request.getJid()));
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  private String convert(JournalIdProto jid) {
    return jid.getIdentifier();
  }

  @Override
  public NewEpochResponseProto newEpoch(RpcController controller,
      NewEpochRequestProto request) throws ServiceException {
    try {
      return impl.newEpoch(
          request.getJid().getIdentifier(),
          PBHelper.convert(request.getNsInfo()),
          request.getEpoch());
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }

  /** @see JournalProtocol#journal */
  @Override
  public JournalResponseProto journal(RpcController unused,
      JournalRequestProto req) throws ServiceException {
    try {
      impl.journal(convert(req.getReqInfo()),
          req.getFirstTxnId(), req.getNumTxns(), req.getRecords()
              .toByteArray());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return JournalResponseProto.newBuilder().build();
  }

  /** @see JournalProtocol#startLogSegment */
  @Override
  public StartLogSegmentResponseProto startLogSegment(RpcController controller,
      StartLogSegmentRequestProto req) throws ServiceException {
    try {
      impl.startLogSegment(convert(req.getReqInfo()),
          req.getTxid());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return StartLogSegmentResponseProto.newBuilder().build();
  }
  
  @Override
  public FinalizeLogSegmentResponseProto finalizeLogSegment(
      RpcController controller, FinalizeLogSegmentRequestProto req)
      throws ServiceException {
    try {
      impl.finalizeLogSegment(convert(req.getReqInfo()),
          req.getStartTxId(), req.getEndTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
    return FinalizeLogSegmentResponseProto.newBuilder().build();
  }

  @Override
  public SyncLogsResponseProto syncLogs(RpcController controller,
      SyncLogsRequestProto request) throws ServiceException {
    // TODO Auto-generated method stub
    throw new RuntimeException();
  }

  @Override
  public GetEditLogManifestResponseProto getEditLogManifest(
      RpcController controller, GetEditLogManifestRequestProto request)
      throws ServiceException {
    try {
      return impl.getEditLogManifest(
          request.getJid().getIdentifier(),
          request.getSinceTxId());
    } catch (IOException e) {
      throw new ServiceException(e);
    }
  }


  private RequestInfo convert(
      QJournalProtocolProtos.RequestInfoProto reqInfo) {
    return new RequestInfo(
        reqInfo.getJournalId().getIdentifier(),
        reqInfo.getEpoch(),
        reqInfo.getIpcSerialNumber());
  }
}
