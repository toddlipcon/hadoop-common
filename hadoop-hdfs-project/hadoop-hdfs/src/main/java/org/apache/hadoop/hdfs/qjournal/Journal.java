package org.apache.hadoop.hdfs.qjournal;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.RemoteEditLogProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEpochInfoResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.LogRecoveryProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PaxosPrepareResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
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

  public synchronized NewEpochResponseProto.Builder newEpoch(
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
      builder.setLastSegment(getSegmentInfo(curSegmentTxId));
    }
    
    
    // Return the partial builder instead of the proto, since
    // we have to fill in the http port here, too, and that's
    // only known to the caller.
    // TODO: would be nice to see if we can make this less intertwined, 
    // but don't want to do so at the cost of an extra round trip.
    return builder;
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

  private synchronized void checkRequest(RequestInfo reqInfo) throws IOException {
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
      if (curSegment != null) {
        curSegment.close();
        curSegment = null;
      }
      if (curSegmentFinalized) {
        LOG.info("Received no-op request to finalize " +
            "already-finalized segment " +
            curSegmentTxId + "-" + endTxId);
      } else {
        fjm.finalizeLogSegment(startTxId, endTxId);
        // TODO: add some sanity check here for non-overlapping edits
        // in debug case?
        curSegmentFinalized = true;
      }
    } else {
      fjm.finalizeLogSegment(startTxId, endTxId);
    }
  }
  

  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    // TODO: check fencing info?
    RemoteEditLogManifest manifest = new RemoteEditLogManifest(
        fjm.getRemoteEditLogs(sinceTxId));
    return manifest;
  }
    
  private RemoteEditLogProto getSegmentInfo(long segmentTxId) {
    Preconditions.checkArgument(segmentTxId == curSegmentTxId,
        " TODO: need to handle this for other segments, " +
        "cur: %s  asked for: %s", curSegmentTxId, segmentTxId);
    return RemoteEditLogProto.newBuilder()
        .setStartTxId(segmentTxId)
        .setEndTxId(nextTxId - 1)
        .setIsInProgress(!curSegmentFinalized)
        .build();
  }


  public synchronized PaxosPrepareResponseProto.Builder paxosPrepare(
      RequestInfo reqInfo, long segmentTxId) throws IOException {
    checkRequest(reqInfo);
    
    PaxosPrepareResponseProto.Builder resp = PaxosPrepareResponseProto.newBuilder();
    
    LogRecoveryProto previouslyAccepted = getPersistedPaxosData(segmentTxId);
    if (previouslyAccepted != null) {
      resp.setAcceptedRecovery(previouslyAccepted);
    }
    
    if (curSegmentTxId != HdfsConstants.INVALID_TXID) {
      resp.setSegmentInfo(getSegmentInfo(segmentTxId));
    }
    
    LOG.info("Prepared paxos for decision '" + segmentTxId + "': " + resp);
    return resp;
  }

  private LogRecoveryProto getPersistedPaxosData(long segmentTxId)
      throws IOException {
    File f = storage.getPaxosFile(segmentTxId);
    if (!f.exists()) {
      // Default instance has no fields filled in (they're optional)
      return null;
    }
    
    InputStream in = new FileInputStream(f);
    try {
      return LogRecoveryProto.parseDelimitedFrom(in);
    } finally {
      IOUtils.closeStream(in);
    }
  }

  private void persistPaxosData(long segmentTxId,
      LogRecoveryProto newData) throws IOException {
    File f = storage.getPaxosFile(segmentTxId);
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

  public synchronized void paxosAccept(RequestInfo reqInfo,
      RemoteEditLogProto segment, URL fromUrl)
      throws IOException {
    checkRequest(reqInfo);

    // TODO: right now, a recovery of a segment when the log is
    // completely emtpy (ie startLogSegment() but no txns)
    // will fail this assertion here, since endTxId < startTxId
    Preconditions.checkArgument(segment.getEndTxId() > 0 &&
        segment.getEndTxId() >= segment.getStartTxId(),
        "bad segment for accept: " + segment);
    
    long segmentTxId = segment.getStartTxId();
    LogRecoveryProto oldData = getPersistedPaxosData(segmentTxId);

    LogRecoveryProto newData = LogRecoveryProto.newBuilder()
        .setAcceptedInEpoch(reqInfo.getEpoch())
        .setEndTxId(segment.getEndTxId())
        .build();
    if (oldData != null) {
      Preconditions.checkState(oldData.getAcceptedInEpoch() <= reqInfo.getEpoch(),
          "Bad paxos transition, out-of-order epochs.\nOld: %s\nNew: %s\n",
          oldData, newData);
    }

    RemoteEditLogProto currentSegment = getSegmentInfo(segment.getStartTxId());
    // TODO: what if they have the same length but one is finalized and the
    // other isn't! cover that case.
    if (currentSegment.getEndTxId() != segment.getEndTxId()) {
      syncLog(reqInfo, segment, fromUrl);
    } else {
      LOG.info("Skipping download of log " + segment + " -- already have up-to-date logs");
    }
    
    // TODO: is it OK that this is non-atomic?
    // we might be left with an older epoch recorded, but a newer log
    
    persistPaxosData(segmentTxId, newData);
    LOG.info("Accepted paxos value for segment " + segmentTxId + "': " + newData);
  }

  private void syncLog(RequestInfo reqInfo, RemoteEditLogProto segment, URL url)
      throws IOException {
    checkRequest(reqInfo);
    
    // While not synchronized, transfer the file
    String tmpFileName =
        "synclog_" + segment.getStartTxId() + "_" +
        reqInfo.getEpoch() + "." + reqInfo.getIpcSerialNumber();
    
    List<File> localPaths = storage.getFiles(null, tmpFileName);
    assert localPaths.size() == 1;
    File tmpFile = localPaths.get(0);
 
    boolean success = false;
    
    TransferFsImage.doGetUrl(url, localPaths, storage, true);
    assert tmpFile.exists();
    try {
      synchronized (this) {
        // Re-check that the writer who asked us to synchronize is still
        // current, while holding the lock
        checkRequest(reqInfo);
        
        success = tmpFile.renameTo(storage.getInProgressEditLog(
            segment.getStartTxId()));
        if (success) {
          // If we're synchronizing the latest segment, update our cached
          // info.
          // TODO: can this be done more generally?
          if (curSegmentTxId == segment.getStartTxId()) {
            nextTxId = segment.getEndTxId() + 1;
          }
        }
      }
    } finally {
      if (!success) {
        tmpFile.delete();
      }
    }
  }
}
