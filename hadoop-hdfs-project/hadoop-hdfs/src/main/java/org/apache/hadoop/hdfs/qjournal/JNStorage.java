package org.apache.hadoop.hdfs.qjournal;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
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

  File findFinalizedEditsFile(long startTxId, long endTxId) {
    throw new AssertionError("TODO");
  }

  void format() throws IOException {
    LOG.info("Formatting journal storage directory " + 
        sd);
    sd.clearDirectory();
    writeProperties(sd);
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
