package org.apache.hadoop.hdfs.qjournal;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.LogSegmentProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SyncLogRequestProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;
import org.apache.hadoop.io.IOUtils;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

public class Journal implements Closeable {
  public static final Log LOG = LogFactory.getLog(Journal.class);

  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsConstants.INVALID_TXID;
  private long nextTxId = HdfsConstants.INVALID_TXID;
  // TODO: set me at startup
  private boolean curSegmentFinalized = false;
  

  private final JNStorage storage;

  /** f_p in ZAB terminology */
  private PersistentLong lastPromisedEpoch;
  
  /** f_a in ZAB terminology */
  private PersistentLong lastAcceptedEpoch;
  private final FileJournalManager fjm;


  Journal(File logDir, StorageErrorReporter errorReporter) {
    storage = new JNStorage(logDir, errorReporter);

    File currentDir = storage.getStorageDir(0).getCurrentDir();
    this.lastPromisedEpoch = new PersistentLong(
        new File(currentDir, "last-promised-epoch"), 0);
    this.lastAcceptedEpoch = new PersistentLong(
        new File(currentDir, "last-accepted-epoch"), 0);
    this.fjm = storage.getJournalManager();
  }
  
  synchronized void format() throws IOException {
    storage.format();
  }
  
  public void close() throws IOException {
    storage.close();
  }
  
  JNStorage getStorage() {
    return storage;
  }

  public GetEpochInfoResponseProto getEpochInfo() throws IOException {
    return GetEpochInfoResponseProto.newBuilder()
      .setLastPromisedEpoch(getLastPromisedEpoch())
      .build();
  }
  
  synchronized long getLastPromisedEpoch() throws IOException {
    return lastPromisedEpoch.get();
  }

  public synchronized NewEpochResponseProto newEpoch(
      NamespaceInfo nsInfo, long epoch)
      throws IOException {

    // TODO: we probably don't want to auto-format, but rather
    // take some kind of NN startup flag or tool to do this.
    storage.formatIfEmpty(nsInfo);

    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getEpochInfo());
    }
    
    lastPromisedEpoch.set(epoch);
    if (curSegment != null) {
      curSegment.close();
      curSegment = null;
    }
    
    NewEpochResponseProto.Builder builder =
        NewEpochResponseProto.newBuilder()
        .setCurrentEpoch(lastAcceptedEpoch.get());
        
    if (curSegmentTxId != HdfsConstants.INVALID_TXID) {
      builder.setLastSegment(LogSegmentProto.newBuilder()
          .setStartTxId(curSegmentTxId)
          .setEndTxId(nextTxId - 1)
          .setIsInProgress(!curSegmentFinalized));
    }
    
    return builder.build();
  }


  public synchronized void journal(RequestInfo reqInfo, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    checkRequest(reqInfo);
    Preconditions.checkState(curSegment != null,
        "Can't write, no segment open");
    Preconditions.checkState(nextTxId == firstTxnId,
        "Can't write txid " + firstTxnId + " expecting nextTxId=" + nextTxId);
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writing txid " + firstTxnId + "-" + (firstTxnId + numTxns - 1));
    }
    curSegment.writeRaw(records, 0, records.length);
    curSegment.setReadyToFlush();
    curSegment.flush();
    nextTxId += numTxns;
  }

  private void checkRequest(RequestInfo reqInfo) throws IOException {
    // Invariant 25 from ZAB paper
    if (reqInfo.getEpoch() < lastPromisedEpoch.get()) {
      throw new IOException("IPC's epoch " + reqInfo.getEpoch() +
          " is less than the last promised epoch " +
          lastPromisedEpoch.get());
    }
    
    // TODO: should other requests check the _exact_ epoch instead of
    // the <= check? <= should probably only be necessary for the
    // first calls
    
    // TODO: some check on serial number that they only increase from a given
    // client
  }

  public synchronized void startLogSegment(RequestInfo reqInfo, long txid)
      throws IOException {
    assert fjm != null;
    checkRequest(reqInfo);
    
    Preconditions.checkState(curSegment == null,
        "Can't start a log segment, already writing " + curSegment);
    Preconditions.checkState(nextTxId == txid || nextTxId == HdfsConstants.INVALID_TXID,
        "Can't start log segment " + txid + " expecting nextTxId=" + nextTxId);
    curSegment = fjm.startLogSegment(txid);
    curSegmentTxId = txid;
    nextTxId = txid;
    curSegmentFinalized = false;
  }
  
  public void finalizeLogSegment(RequestInfo reqInfo, long startTxId,
      long endTxId) throws IOException {
    checkRequest(reqInfo);
    
    if (startTxId == curSegmentTxId) {
      Preconditions.checkState(nextTxId == endTxId + 1,
          "Trying to finalize current log segment (startTxId=%s) " +
          "with ending txid=%s, but cur txid is %s",
          curSegmentTxId, endTxId, nextTxId - 1);
      Preconditions.checkState(!curSegmentFinalized,
          "Trying to finalize already-finalized segment %s-%s",
          curSegmentTxId, endTxId);
      
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
      }
      fjm.finalizeLogSegment(startTxId, endTxId);
      // TODO: add some sanity check here for non-overlapping edits
      // in debug case?
      curSegmentFinalized = true;
    } else {
      fjm.finalizeLogSegment(startTxId, endTxId);
    }
  }
  

  public void syncLog(SyncLogRequestProto req) {
    
    
  }


  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    // TODO: check fencing info?
    RemoteEditLogManifest manifest = new RemoteEditLogManifest(
        fjm.getRemoteEditLogs(sinceTxId));
    return manifest;
  }

  public synchronized PaxosPrepareResponseProto paxosPrepare(
      RequestInfo reqInfo, String decisionId)
      throws IOException {
    checkRequest(reqInfo);
    PaxosPrepareResponseProto ret = getPersistedPaxosData(decisionId);
    LOG.info("Prepared paxos for decision '" + decisionId + "': " +
        (ret.hasAcceptedEpoch() ? ret : "no previous value accepted"));
    return ret;
  }

  private PaxosPrepareResponseProto getPersistedPaxosData(String decisionId)
      throws IOException {
    File f = storage.getPaxosFile(decisionId);
    if (!f.exists()) {
      // Default instance has no fields filled in (they're optional)
      return PaxosPrepareResponseProto.getDefaultInstance();
    }
    
    InputStream in = new FileInputStream(f);
    try {
      return PaxosPrepareResponseProto.parseDelimitedFrom(in);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  private void persistPaxosData(String decisionId,
      PaxosPrepareResponseProto newData) throws IOException {
    File f = storage.getPaxosFile(decisionId);
    boolean success = false;
    AtomicFileOutputStream fos = new AtomicFileOutputStream(f);
    try {
      newData.writeDelimitedTo(fos);
      fos.write('\n');
      // Write human-readable data after the protobuf. This is only
      // to assist in debugging -- it's not parsed at all.
      OutputStreamWriter writer = new OutputStreamWriter(fos);
      
      writer.write(String.valueOf(newData));
      writer.write('\n');
      writer.flush();
      
      fos.flush();
      success = true;
    } finally {
      if (success) {
        IOUtils.closeStream(fos);
      } else {
        fos.abort();
      }
    }
  }

  public synchronized void paxosAccept(RequestInfo reqInfo, String decisionId, byte[] value)
      throws IOException {
    checkRequest(reqInfo);
    
    PaxosPrepareResponseProto oldData = getPersistedPaxosData(decisionId);
    PaxosPrepareResponseProto newData = PaxosPrepareResponseProto.newBuilder()
        .setAcceptedEpoch(reqInfo.getEpoch())
        .setAcceptedValue(ByteString.copyFrom(value))
        .build();
    if (oldData != null) {
      Preconditions.checkState(oldData.getAcceptedEpoch() < reqInfo.getEpoch(),
          "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n",
          oldData, newData);
    }
    
    persistPaxosData(decisionId, newData);
    LOG.info("Accepted value for paxos decision '" + decisionId + "': " + newData);
  }

}
