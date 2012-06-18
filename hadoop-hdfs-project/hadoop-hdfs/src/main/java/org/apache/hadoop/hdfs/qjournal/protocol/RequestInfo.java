package org.apache.hadoop.hdfs.qjournal.protocol;

// TODO annotate
public class RequestInfo {
  private String jid;
  private long epoch;
  private long ipcSerialNumber;
  
  public RequestInfo(String jid,
      long epoch, long ipcSerialNumber) {
    this.jid = jid;
    this.epoch = epoch;
    this.ipcSerialNumber = ipcSerialNumber;
  }

  public long getEpoch() {
    return epoch;
  }

  public void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  public String getJournalId() {
    return jid;
  }

  public long getIpcSerialNumber() {
    return ipcSerialNumber;
  }

  public void setIpcSerialNumber(long ipcSerialNumber) {
    this.ipcSerialNumber = ipcSerialNumber;
  }

}
