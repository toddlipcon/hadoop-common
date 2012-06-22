package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

class JNStorage extends Storage {

  private final FileJournalManager fjm;
  private final StorageDirectory sd;
  private boolean lazyInitted = false;

  protected JNStorage(File logDir, StorageErrorReporter errorReporter) {
    super(NodeType.JOURNAL_NODE);
    
    sd = new StorageDirectory(logDir);
    this.addStorageDir(sd);
    this.fjm = new FileJournalManager(sd, errorReporter);
  }
  
  FileJournalManager getJournalManager() {
    return fjm;
  }

  @Override
  public boolean isPreUpgradableLayout(StorageDirectory sd) throws IOException {
    return false;
  }

  File findFinalizedEditsFile(long startTxId, long endTxId) throws IOException {
    File ret = new File(sd.getCurrentDir(),
        NNStorage.getFinalizedEditsFileName(startTxId, endTxId));
    if (!ret.exists()) {
      throw new IOException(
          "No edits file for range " + startTxId + "-" + endTxId);
    }
    return ret;
  }

  public File getInProgressEditLog(long startTxId) {
    return new File(sd.getCurrentDir(),
        NNStorage.getInProgressEditsFileName(startTxId));
  }

  File getPaxosFile(long segmentTxId) {
    return new File(getPaxosDir(), String.valueOf(segmentTxId));
  }
  
  File getPaxosDir() {
    return new File(sd.getCurrentDir(), "paxos");
  }


  void format() throws IOException {
    LOG.info("Formatting journal storage directory " + 
        sd);
    sd.clearDirectory();
    writeProperties(sd);
    if (!getPaxosDir().mkdirs()) {
      throw new IOException("Could not create paxos dir: " + getPaxosDir());
    }
  }

  void formatIfEmpty(NamespaceInfo nsInfo) throws IOException {
    // TODO: should verify equality, not blindly set!
    // Also need to rename this func
    if (lazyInitted) {
      return;
    }
    
    setStorageInfo(nsInfo);
    StorageState state = sd.analyzeStorage(StartupOption.REGULAR, this);
    switch (state) {
    case NON_EXISTENT:
    case NOT_FORMATTED:
      format();
      break;
    default:
      LOG.warn("TODO: unhandled state for storage dir " + sd + ": " + state);
    }
    lazyInitted  = true;
  }

  public void close() throws IOException {
    LOG.info("Closing journal storage for " + sd);
    unlockAll();
  }
}
